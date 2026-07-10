#!/usr/bin/env bash
# setup-vllm.sh
#
# Deploys a distributed vLLM inference server across three DGX Sparks using
# vLLM's native multiprocessing backend (--nnodes / --node-rank / --master-addr).
# No Ray required.
#
# Prerequisites:
#   - Ring network already configured (run setup-spark-ring.sh first)
#   - Passwordless SSH between all nodes (set up by setup-spark-ring.sh)
#   - Docker installed; user in the docker group on all nodes
#   - ibdev2netdev, ssh
#
# Usage: bash setup-vllm.sh

set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────
# NVIDIA vLLM container image — check https://catalog.ngc.nvidia.com/orgs/nvidia/containers/vllm
VLLM_IMAGE="nvcr.io/nvidia/vllm:26.02-py3"

# Hostname prefix used to identify DGX Sparks via mDNS discovery.
SPARK_HOSTNAME_PREFIX="gx10-"

# Model to serve
MODEL="Qwen/Qwen3.6-27B"

# HuggingFace cache directory (mounted into the vLLM container on every node)
HF_HOME="${HOME}/.cache/huggingface"

# Port used for vLLM inter-node rendezvous (not the serving port)
MASTER_PORT=54321

# HTTP port for the vLLM API (head node only)
VLLM_PORT=8000

# Total nodes in the cluster
NUM_NODES=3

# Tensor parallel size = total GPUs across all nodes (1 GPU per DGX Spark → 3)
TENSOR_PARALLEL_SIZE=3

# How long to wait (seconds) for the vLLM server to become healthy
VLLM_HEALTH_TIMEOUT=600

# Docker container names
HEAD_CONTAINER="vllm-head"
WORKER_CONTAINER="vllm-worker"
# ──────────────────────────────────────────────────────────────────────────────

# ── Helpers ───────────────────────────────────────────────────────────────────
log()  { echo "[INFO]  $*"; }
warn() { echo "[WARN]  $*" >&2; }
die()  { echo "[ERROR] $*" >&2; exit 1; }

check_command() {
    command -v "$1" &>/dev/null || die "Required tool not found: '$1'. Please install it and re-run."
}

# Run a command on a remote node; exit on failure
remote_run() {
    local user="$1" ip="$2" cmd="$3"
    ssh -o StrictHostKeyChecking=accept-new \
        -o BatchMode=yes \
        -o ConnectTimeout=10 \
        "${user}@${ip}" "${cmd}"
}

# Start a command on a remote node in the background; log to a file there
remote_run_bg() {
    local user="$1" ip="$2" cmd="$3" logfile="$4"
    ssh -o StrictHostKeyChecking=accept-new \
        -o BatchMode=yes \
        -o ConnectTimeout=10 \
        "${user}@${ip}" \
        "nohup bash -c $(printf '%q' "${cmd}") > ${logfile} 2>&1 </dev/null &"
}


# ═══════════════════════════════════════════════════════════════════════════════
# Phase 1: Prerequisites & Node Discovery
# ═══════════════════════════════════════════════════════════════════════════════
log "=== Phase 1: Prerequisites & Node Discovery ==="

# Must not run as root
[[ "${EUID}" -ne 0 ]] || die "This script must not be run as root."

# Must be a DGX Spark
[[ -f /etc/dgx-release ]] \
    || die "/etc/dgx-release not found. This script must be run on a DGX Spark."
grep -q 'DGX_NAME="DGX Spark"' /etc/dgx-release 2>/dev/null \
    || die "DGX_NAME is not 'DGX Spark'. This script must be run on a DGX Spark."

for tool in ibdev2netdev avahi-browse ssh docker; do
    check_command "${tool}"
done

# CX7 interfaces must be UP and have IPs (ring network must be configured)
log "Checking CX7 (QSFP) interfaces via ibdev2netdev..."
mapfile -t CX7_UP_IFACES < <(ibdev2netdev | awk '/Up\)/ {print $5}')
if [[ ${#CX7_UP_IFACES[@]} -eq 0 ]]; then
    die "No CX7 interfaces are UP. Run setup-spark-ring.sh first to configure the ring network."
fi
log "UP CX7 interfaces: ${CX7_UP_IFACES[*]}"

# Use the first UP CX7 interface for inter-node communication
CX7_IFACE="${CX7_UP_IFACES[0]}"
log "Using CX7 interface: ${CX7_IFACE}"

# Derive the RDMA device name for NCCL (e.g. enp1s0f1np1 → rocep1s0f1)
NCCL_IB_HCA=$(ibdev2netdev | awk -v iface="${CX7_IFACE}" '$5==iface {print $1}')
[[ -n "${NCCL_IB_HCA}" ]] || warn "Could not determine RDMA device for ${CX7_IFACE}; NCCL_IB_HCA will be unset."
log "RDMA device (NCCL_IB_HCA): ${NCCL_IB_HCA:-<unset>}"

# Get this node's IP on the CX7 interface — used as master-addr and VLLM_HOST_IP
HEAD_CX7_IP=$(ip -4 addr show "${CX7_IFACE}" | awk '/inet / {split($2,a,"/"); print a[1]}')
if [[ -z "${HEAD_CX7_IP}" ]]; then
    die "No IPv4 address on ${CX7_IFACE}. Run setup-spark-ring.sh first to assign IPs."
fi
log "Head node CX7 IP (master-addr): ${HEAD_CX7_IP}"

# ── Worker IPs via mDNS ──────────────────────────────────────────────────────
# All CX7 interface names — excluded from avahi results so only management IPs
# are returned (avahi-browse sees every interface including CX7 ring ones).
CX7_ALL_IFACES=$(ibdev2netdev | awk '{print $5}' | tr '\n' ',')

log "Discovering worker nodes with prefix '${SPARK_HOSTNAME_PREFIX}' via mDNS..."
mapfile -t DISCOVERED_IPS < <(
    avahi-browse -p -r -f -t _ssh._tcp 2>/dev/null \
        | awk -F';' -v prefix="${SPARK_HOSTNAME_PREFIX}" -v cx7="${CX7_ALL_IFACES}" \
            'BEGIN { n=split(cx7,a,","); for(i=1;i<=n;i++) skip[a[i]]=1 }
             $1=="=" && $3=="IPv4" && !skip[$2] && $7 ~ "^" prefix { print $8 }' \
        | sort -u
)

WORKER_MGMT_IPS=()
if [[ ${#DISCOVERED_IPS[@]} -eq 3 ]]; then
    # Remove this node's own CX7 IP from the list to get only the two workers
    for ip in "${DISCOVERED_IPS[@]}"; do
        [[ "${ip}" != "${HEAD_CX7_IP}" ]] && WORKER_MGMT_IPS+=("${ip}")
    done
fi

if [[ ${#WORKER_MGMT_IPS[@]} -ne 2 ]]; then
    if [[ ${#DISCOVERED_IPS[@]} -eq 0 ]]; then
        warn "mDNS found no nodes with prefix '${SPARK_HOSTNAME_PREFIX}'. Falling back to manual entry."
    else
        warn "mDNS found ${#DISCOVERED_IPS[@]} node(s): ${DISCOVERED_IPS[*]}. Falling back to manual entry."
    fi
    echo ""
    read -r -p "Node 2 management IP or hostname: " WORKER1_IP
    read -r -p "Node 3 management IP or hostname: " WORKER2_IP
    for ip in "${WORKER1_IP}" "${WORKER2_IP}"; do
        [[ -n "${ip}" ]] || die "IP/hostname cannot be empty."
    done
    WORKER_MGMT_IPS=("${WORKER1_IP}" "${WORKER2_IP}")
fi

log "Workers: ${WORKER_MGMT_IPS[*]}"

# Ring setup configured passwordless SSH for the same user on all nodes,
# so the current $USER is the correct SSH username on all nodes.
SSH_USER="${USER}"
log "SSH user: ${SSH_USER}"

# ── Verify passwordless SSH to workers ────────────────────────────────────────
log "Verifying passwordless SSH to worker nodes..."
for ip in "${WORKER_MGMT_IPS[@]}"; do
    timeout 10 remote_run "${SSH_USER}" "${ip}" true 2>/dev/null \
        || die "Passwordless SSH to ${SSH_USER}@${ip} failed. Ensure setup-spark-ring.sh has been run and you are logged in as the cluster user."
    log "  ✓ ${ip}"
done

# ── Get CX7 IPs of worker nodes ───────────────────────────────────────────────
log "Fetching CX7 IP on ${CX7_IFACE} from each worker..."
WORKER_CX7_IPS=()
for ip in "${WORKER_MGMT_IPS[@]}"; do
    cx7_ip=$(remote_run "${SSH_USER}" "${ip}" \
        "ip -4 addr show ${CX7_IFACE} | awk '/inet / {split(\$2,a,\"/\"); print a[1]}'")
    [[ -n "${cx7_ip}" ]] \
        || die "No IP on ${CX7_IFACE} on ${ip}. Ring network may not be fully configured."
    log "  ${ip} -> ${CX7_IFACE} = ${cx7_ip}"
    WORKER_CX7_IPS+=("${cx7_ip}")
done


# ═══════════════════════════════════════════════════════════════════════════════
# Phase 2: Prepare all nodes
# ═══════════════════════════════════════════════════════════════════════════════
log ""
log "=== Phase 2: Preparing all nodes ==="

# ── Pull vLLM Docker image on all nodes in parallel ───────────────────────────
log "Pulling vLLM image '${VLLM_IMAGE}' on all nodes in parallel..."
for ip in "${WORKER_MGMT_IPS[@]}"; do
    remote_run_bg "${SSH_USER}" "${ip}" \
        "docker pull ${VLLM_IMAGE}" \
        "~/vllm-pull.log"
done
docker pull "${VLLM_IMAGE}" &
LOCAL_PULL_PID=$!

log "  (waiting for all pulls to finish — this may take a while)"
wait "${LOCAL_PULL_PID}" || die "docker pull failed locally."
log "  ✓ local image ready"

for ip in "${WORKER_MGMT_IPS[@]}"; do
    until remote_run "${SSH_USER}" "${ip}" \
            "docker image inspect ${VLLM_IMAGE} >/dev/null 2>&1" 2>/dev/null; do
        sleep 10
    done
    log "  ✓ ${ip} image ready"
done

# ── Download model on all nodes in parallel ───────────────────────────────────
log ""
log "Downloading model '${MODEL}' on all nodes..."
log "  (~50 GB — this may take a long time depending on network speed)"
echo ""
read -r -p "  HuggingFace token (press Enter to skip if model is public): " HF_TOKEN
echo ""

# Use the vLLM image itself so huggingface-cli is guaranteed to be present
DOWNLOAD_CMD="docker run --rm \
    -v ${HF_HOME}:/root/.cache/huggingface \
    ${HF_TOKEN:+-e HF_TOKEN=${HF_TOKEN}} \
    ${VLLM_IMAGE} \
    huggingface-cli download ${MODEL}"

log "  Downloading on worker nodes in background..."
for ip in "${WORKER_MGMT_IPS[@]}"; do
    remote_run_bg "${SSH_USER}" "${ip}" "${DOWNLOAD_CMD}" "~/vllm-model-download.log"
    log "  Started on ${ip} (log: ~/vllm-model-download.log)"
done

log "  Downloading locally..."
eval "${DOWNLOAD_CMD}" || die "Model download failed on local node."
log "  ✓ Local download complete"

log "  Waiting for worker downloads to finish..."
for ip in "${WORKER_MGMT_IPS[@]}"; do
    while remote_run "${SSH_USER}" "${ip}" \
            "pgrep -f 'huggingface-cli' >/dev/null 2>&1" 2>/dev/null; do
        sleep 15
    done
    if remote_run "${SSH_USER}" "${ip}" \
            "grep -qi 'error\|failed\|traceback' ~/vllm-model-download.log 2>/dev/null"; then
        warn "Possible download error on ${ip}. Check ~/vllm-model-download.log"
    fi
    log "  ✓ ${ip} download complete"
done

# ── Drop page caches on all nodes to maximise available memory ────────────────
log ""
log "Dropping page caches on all nodes to free memory for model weights..."
drop_cmd="sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'"
if eval "${drop_cmd}" 2>/dev/null; then
    log "  ✓ Caches dropped locally"
else
    warn "  Could not drop caches locally (passwordless sudo may not be configured — skipping)"
fi
for ip in "${WORKER_MGMT_IPS[@]}"; do
    if remote_run "${SSH_USER}" "${ip}" "${drop_cmd}" 2>/dev/null; then
        log "  ✓ Caches dropped on ${ip}"
    else
        warn "  Could not drop caches on ${ip} (skipping)"
    fi
done


# ═══════════════════════════════════════════════════════════════════════════════
# Phase 3: Start vLLM cluster (native multiprocessing backend — no Ray)
# ═══════════════════════════════════════════════════════════════════════════════
log ""
log "=== Phase 3: Starting vLLM cluster ==="
log "Backend: vLLM native multiprocessing (--nnodes ${NUM_NODES}, master ${HEAD_CX7_IP}:${MASTER_PORT})"

# ── Stop any pre-existing vLLM containers ─────────────────────────────────────
log "Stopping any pre-existing vLLM containers..."
docker rm -f "${HEAD_CONTAINER}" 2>/dev/null || true
for ip in "${WORKER_MGMT_IPS[@]}"; do
    remote_run "${SSH_USER}" "${ip}" \
        "docker rm -f ${WORKER_CONTAINER} 2>/dev/null || true" 2>/dev/null || true
done

# ── Start worker nodes (rank 1, 2, …) via SSH ─────────────────────────────────
log "Starting worker containers..."
for i in "${!WORKER_MGMT_IPS[@]}"; do
    mgmt_ip="${WORKER_MGMT_IPS[${i}]}"
    cx7_ip="${WORKER_CX7_IPS[${i}]}"
    rank=$(( i + 1 ))

    # Workers run vllm serve with their rank — no --port since they don't serve HTTP
    worker_cmd="docker run --detach \
        --name ${WORKER_CONTAINER} \
        --network host --gpus all --shm-size 10.24g --ipc host \
        -v ${HF_HOME}:/root/.cache/huggingface \
        -e VLLM_HOST_IP=${cx7_ip} \
        -e NCCL_IB_DISABLE=0 \
        -e NCCL_SOCKET_IFNAME=${CX7_IFACE} \
        -e UCX_NET_DEVICES=${CX7_IFACE} \
        -e NCCL_IB_MERGE_NICS=0 \
        -e NCCL_NET_PLUGIN=none \
        ${NCCL_IB_HCA:+-e NCCL_IB_HCA=${NCCL_IB_HCA}} \
        ${VLLM_IMAGE} \
        vllm serve ${MODEL} \
            --trust-remote-code \
            --tensor-parallel-size ${TENSOR_PARALLEL_SIZE} \
            --distributed-executor-backend mp \
            --nnodes ${NUM_NODES} \
            --node-rank ${rank} \
            --master-addr ${HEAD_CX7_IP} \
            --master-port ${MASTER_PORT}"

    remote_run "${SSH_USER}" "${mgmt_ip}" "${worker_cmd}" \
        && log "  ✓ Worker rank ${rank} started on ${mgmt_ip} (CX7: ${cx7_ip})" \
        || die "Failed to start worker container on ${mgmt_ip}"
done

# ── Start head node (rank 0) locally ──────────────────────────────────────────
log "Starting head container (rank 0, serving on port ${VLLM_PORT})..."
# Build NCCL env flags for the local docker run call
nccl_flags=(
    -e "VLLM_HOST_IP=${HEAD_CX7_IP}"
    -e "NCCL_IB_DISABLE=0"
    -e "NCCL_SOCKET_IFNAME=${CX7_IFACE}"
    -e "UCX_NET_DEVICES=${CX7_IFACE}"
    -e "NCCL_IB_MERGE_NICS=0"
    -e "NCCL_NET_PLUGIN=none"
)
[[ -n "${NCCL_IB_HCA}" ]] && nccl_flags+=(-e "NCCL_IB_HCA=${NCCL_IB_HCA}")

docker run --detach \
    --name "${HEAD_CONTAINER}" \
    --network host \
    --gpus all \
    --shm-size 10.24g \
    --ipc host \
    -v "${HF_HOME}:/root/.cache/huggingface" \
    "${nccl_flags[@]}" \
    "${VLLM_IMAGE}" \
    vllm serve "${MODEL}" \
        --trust-remote-code \
        --tensor-parallel-size "${TENSOR_PARALLEL_SIZE}" \
        --distributed-executor-backend mp \
        --nnodes "${NUM_NODES}" \
        --node-rank 0 \
        --master-addr "${HEAD_CX7_IP}" \
        --master-port "${MASTER_PORT}" \
        --port "${VLLM_PORT}"
log "  ✓ Head container started (name: ${HEAD_CONTAINER})"


# ═══════════════════════════════════════════════════════════════════════════════
# Phase 4: Wait for vLLM to become healthy
# ═══════════════════════════════════════════════════════════════════════════════
log ""
log "=== Phase 4: Waiting for vLLM server (up to ${VLLM_HEALTH_TIMEOUT}s) ==="
log "  Logs: docker logs -f ${HEAD_CONTAINER}"

WAIT_START=$(date +%s)
until curl -sf "http://localhost:${VLLM_PORT}/health" >/dev/null 2>&1; do
    now=$(date +%s)
    elapsed=$(( now - WAIT_START ))
    if (( elapsed > VLLM_HEALTH_TIMEOUT )); then
        die "vLLM server did not become healthy within ${VLLM_HEALTH_TIMEOUT}s. Check: docker logs ${HEAD_CONTAINER}"
    fi
    # Surface any early crash
    if ! docker inspect "${HEAD_CONTAINER}" --format '{{.State.Running}}' 2>/dev/null \
            | grep -q true; then
        die "Head container exited unexpectedly. Check: docker logs ${HEAD_CONTAINER}"
    fi
    log "  Still initialising... (${elapsed}s)"
    sleep 15
done
log "  ✓ vLLM server is healthy."


# ═══════════════════════════════════════════════════════════════════════════════
# Phase 5: Test inference
# ═══════════════════════════════════════════════════════════════════════════════
log ""
log "=== Phase 5: Testing inference ==="

RESPONSE=$(curl -sf "http://localhost:${VLLM_PORT}/v1/completions" \
    -H "Content-Type: application/json" \
    -d "{
        \"model\": \"${MODEL}\",
        \"prompt\": \"Write a haiku about a GPU cluster\",
        \"max_tokens\": 64,
        \"temperature\": 0.7
    }") || die "Inference test failed. Is the server reachable on port ${VLLM_PORT}?"

echo ""
echo "-- Inference response --------------------------------------------------"
echo "${RESPONSE}" | python3 -c "
import sys, json
r = json.load(sys.stdin)
print(r['choices'][0]['text'].strip())
" 2>/dev/null || echo "${RESPONSE}"
echo "------------------------------------------------------------------------"

log ""
log "=== Deployment Complete ==="
log "  vLLM API:       http://localhost:${VLLM_PORT}/v1"
log "  Head logs:      docker logs -f ${HEAD_CONTAINER}"
log ""
log "To stop the cluster:"
log "  docker stop ${HEAD_CONTAINER}"
for ip in "${WORKER_MGMT_IPS[@]}"; do
    log "  ssh ${SSH_USER}@${ip} docker stop ${WORKER_CONTAINER}"
done
log ""
log "To run inference:"
log "  curl http://localhost:${VLLM_PORT}/v1/completions \\"
log "    -H 'Content-Type: application/json' \\"
log "    -d '{\"model\": \"${MODEL}\", \"prompt\": \"Hello\", \"max_tokens\": 64}'"
