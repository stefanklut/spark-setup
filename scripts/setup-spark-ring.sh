#!/usr/bin/env bash
# setup-spark-ring.sh
#
# Automatically configures a three-DGX-Spark ring topology from a single node.
# Clones the NVIDIA dgx-spark-playbooks repo and runs their cluster setup
# script (Option 1: automatic IP assignment, SSH configuration, NCCL test).
#
# Prerequisites: git, python3, ssh, avahi-browse, ibdev2netdev
# Usage:         bash setup-spark-ring.sh

set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────
# Hostname prefix used to identify DGX Sparks via mDNS discovery.
# Change this to match your naming convention (e.g. "dgx-spark-").
SPARK_HOSTNAME_PREFIX="gx10-"

# Where to clone the NVIDIA playbooks repository
PLAYBOOKS_DIR="${HOME}/dgx-spark-playbooks"
SPARK_SETUP_DIR="${PLAYBOOKS_DIR}/nvidia/multi-sparks-through-switch/assets/spark_cluster_setup"
CONFIG_FILE="${SPARK_SETUP_DIR}/config/spark_config_ring.json"
# ──────────────────────────────────────────────────────────────────────────────

# ── Helpers ───────────────────────────────────────────────────────────────────
log()  { echo "[INFO]  $*"; }
warn() { echo "[WARN]  $*" >&2; }
die()  { echo "[ERROR] $*" >&2; exit 1; }

check_command() {
    command -v "$1" &>/dev/null || die "Required tool not found: '$1'. Please install it and re-run."
}

# ── Cleanup ───────────────────────────────────────────────────────────────────
cleanup() {
    if [[ -f "${CONFIG_FILE}" ]]; then
        log "Removing config file containing credentials..."
        if command -v shred &>/dev/null; then
            shred -u "${CONFIG_FILE}"
        else
            rm -f "${CONFIG_FILE}"
        fi
    fi
}
trap cleanup EXIT


# ═══════════════════════════════════════════════════════════════════════════════
# Phase 1: Prerequisites & Inputs
# ═══════════════════════════════════════════════════════════════════════════════
log "=== Phase 1: Prerequisites & Inputs ==="

# Must not run as root
[[ "${EUID}" -ne 0 ]] || die "This script must not be run as root."

# Must be a DGX Spark
[[ -f /etc/dgx-release ]] \
    || die "/etc/dgx-release not found. This script must be run on a DGX Spark."
grep -q 'DGX_NAME="DGX Spark"' /etc/dgx-release 2>/dev/null \
    || die "DGX_NAME is not 'DGX Spark'. This script must be run on a DGX Spark."

# Required tools
for tool in git python3 ssh avahi-browse ibdev2netdev; do
    check_command "${tool}"
done

# Check that CX7 QSFP interfaces are UP before going further
log "Checking CX7 (QSFP) interfaces via ibdev2netdev..."
mapfile -t CX7_UP_IFACES < <(ibdev2netdev | awk '/Up\)/ {print $5}')
if [[ ${#CX7_UP_IFACES[@]} -eq 0 ]]; then
    die "No CX7 interfaces are UP. Check the QSFP cable connections and try again."
fi
log "UP CX7 interfaces: ${CX7_UP_IFACES[*]}"

# ── Credentials ───────────────────────────────────────────────────────────────
echo ""
echo "Enter SSH credentials (same for all three Sparks):"
read -r -p "  Username: " SPARK_USER
read -r -s -p "  Password: " SPARK_PASSWORD
echo ""
[[ -n "${SPARK_USER}" ]]     || die "Username cannot be empty."
[[ -n "${SPARK_PASSWORD}" ]] || die "Password cannot be empty."

# ── Detect local management IP ────────────────────────────────────────────────
log "Detecting local management IP..."
LOCAL_IP=$(ip route get 8.8.8.8 2>/dev/null \
    | awk 'NR==1 { for(i=1;i<=NF;i++) if($i=="src") print $(i+1) }')
if [[ -z "${LOCAL_IP}" ]]; then
    LOCAL_IP=$(hostname -I | tr ' ' '\n' | grep -v '^127\.' | grep -v ':' | head -1)
fi
[[ -n "${LOCAL_IP}" ]] || die "Could not determine local management IP."
log "Local management IP: ${LOCAL_IP}"

# ── Discover other Sparks via mDNS ────────────────────────────────────────────
log "Discovering Sparks with hostname prefix '${SPARK_HOSTNAME_PREFIX}' via mDNS..."
log "(This may take a few seconds...)"

# Build a comma-separated list of all CX7 interface names so we can exclude
# them from avahi results — avahi-browse sees every interface, including the
# CX7 ring interfaces (enp1s0f0np0, enP2p1s0f0np0, etc.) whose IPs are not
# management IPs and would produce spurious or duplicate entries.
CX7_IFACES=$(ibdev2netdev | awk '{print $5}' | tr '\n' ',')

# avahi-browse parseable output format (semicolon-delimited):
#   =;iface;IPv4;service-name;_ssh._tcp;local;fqdn.local;ip;port;"txt"
# Field indices (awk, 1-based with -F';'):
#   $1=type  $2=iface  $3=protocol  $7=fqdn  $8=ip
# We exclude any entry whose interface ($2) is a CX7 interface.
mapfile -t DISCOVERED_IPS < <(
    avahi-browse -p -r -f -t _ssh._tcp 2>/dev/null \
        | awk -F';' -v prefix="${SPARK_HOSTNAME_PREFIX}" -v cx7="${CX7_IFACES}" \
            'BEGIN { n=split(cx7,a,","); for(i=1;i<=n;i++) skip[a[i]]=1 }
             $1=="=" && $3=="IPv4" && !skip[$2] && $7 ~ "^" prefix { print $8 }' \
        | sort -u
)

ALL_NODE_IPS=()

if [[ ${#DISCOVERED_IPS[@]} -eq 3 ]]; then
    log "Discovered 3 Spark nodes: ${DISCOVERED_IPS[*]}"
    ALL_NODE_IPS=("${DISCOVERED_IPS[@]}")
else
    if [[ ${#DISCOVERED_IPS[@]} -eq 0 ]]; then
        warn "mDNS found no nodes with prefix '${SPARK_HOSTNAME_PREFIX}'."
    else
        warn "mDNS found ${#DISCOVERED_IPS[@]} node(s) with prefix '${SPARK_HOSTNAME_PREFIX}' (expected 3): ${DISCOVERED_IPS[*]}"
    fi
    warn "Falling back to manual entry."
    echo ""
    echo "Enter the management IP addresses of all three Sparks:"
    read -r -p "  Node 1 IP [this node: ${LOCAL_IP}]: " NODE1_IP
    read -r -p "  Node 2 IP: " NODE2_IP
    read -r -p "  Node 3 IP: " NODE3_IP
    NODE1_IP="${NODE1_IP:-${LOCAL_IP}}"
    for ip in "${NODE1_IP}" "${NODE2_IP}" "${NODE3_IP}"; do
        [[ -n "${ip}" ]] \
            || die "IP address cannot be empty."
        [[ "${ip}" =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]] \
            || die "Invalid IP address: '${ip}'"
    done
    ALL_NODE_IPS=("${NODE1_IP}" "${NODE2_IP}" "${NODE3_IP}")
fi

log "Nodes: ${ALL_NODE_IPS[*]}"

# ── Verify SSH port reachability ──────────────────────────────────────────────
log "Verifying SSH port (22) reachability on all nodes..."
for ip in "${ALL_NODE_IPS[@]}"; do
    if ! timeout 5 bash -c "> /dev/tcp/${ip}/22" 2>/dev/null; then
        die "SSH port 22 is not reachable on ${ip}. Check network connectivity."
    fi
    log "  ✓ ${ip}:22 reachable"
done


# ═══════════════════════════════════════════════════════════════════════════════
# Phase 2: Clone & Configure
# ═══════════════════════════════════════════════════════════════════════════════
log ""
log "=== Phase 2: Clone & Configure ==="

if [[ -d "${PLAYBOOKS_DIR}/.git" ]]; then
    log "Repository already exists at ${PLAYBOOKS_DIR}, skipping clone."
else
    log "Cloning NVIDIA dgx-spark-playbooks..."
    git clone https://github.com/NVIDIA/dgx-spark-playbooks "${PLAYBOOKS_DIR}"
fi

[[ -d "${SPARK_SETUP_DIR}" ]] \
    || die "Expected setup directory not found: ${SPARK_SETUP_DIR}"

mkdir -p "${SPARK_SETUP_DIR}/config"

# Escape a value for use inside a JSON string
json_escape() {
    printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

ESC_USER=$(json_escape "${SPARK_USER}")
ESC_PASS=$(json_escape "${SPARK_PASSWORD}")

# Clear plaintext password from memory now that it has been escaped
SPARK_PASSWORD=""

log "Writing cluster config to ${CONFIG_FILE}..."
{
    printf '{\n'
    printf '  "nodes_info": [\n'
    first=true
    for ip in "${ALL_NODE_IPS[@]}"; do
        [[ "${first}" == "true" ]] && first=false || printf ',\n'
        printf '    {\n'
        printf '      "ip_address": "%s",\n' "${ip}"
        printf '      "port": 22,\n'
        printf '      "user": "%s",\n' "${ESC_USER}"
        printf '      "password": "%s"\n' "${ESC_PASS}"
        printf '    }'
    done
    printf '\n  ]\n'
    printf '}\n'
} > "${CONFIG_FILE}"
chmod 600 "${CONFIG_FILE}"

# Clear escaped password
ESC_PASS=""

log "Config written (mode: 600). It will be deleted on script exit."


# ═══════════════════════════════════════════════════════════════════════════════
# Phase 3: Run Cluster Setup
# ═══════════════════════════════════════════════════════════════════════════════
log ""
log "=== Phase 3: Running Spark Cluster Setup ==="
log "The NVIDIA script will now:"
log "  1. Detect CX7 interface topology on all nodes"
log "  2. Configure IP addresses via netplan on all nodes"
log "  3. Set up passwordless SSH between all nodes"
log "  4. Run NCCL bandwidth test"
log ""

pushd "${SPARK_SETUP_DIR}" > /dev/null
bash spark_cluster_setup.sh -c "config/spark_config_ring.json" --run-setup
popd > /dev/null

log ""
log "=== Setup Complete ==="
log "The three-Spark ring cluster is configured and ready."
