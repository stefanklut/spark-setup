# spark-setup

Automated setup scripts for a three-node DGX Spark ring cluster, including network configuration and distributed vLLM inference.

## Prerequisites

- Three DGX Spark nodes physically connected in a ring topology via QSFP cables
- Same username and password on all three nodes
- `avahi-browse` (`avahi-utils`), `ibdev2netdev` (`infiniband-diags`), `git`, `python3` installed on the node running the scripts
- Docker installed on all nodes; user added to the `docker` group

Nodes are discovered automatically via mDNS using the hostname prefix `gx10-` (configurable at the top of each script).

## Scripts

### 1. `scripts/setup-spark-ring.sh`

Configures the three-Spark ring network from a single node. Run this first.

**What it does:**
1. Checks CX7 (QSFP) interfaces are UP via `ibdev2netdev`
2. Discovers all three nodes via mDNS (filtered by hostname prefix, excluding CX7 interfaces)
3. Prompts once for the shared SSH username and password
4. Clones [NVIDIA dgx-spark-playbooks](https://github.com/NVIDIA/dgx-spark-playbooks) and generates a JSON cluster config
5. Runs `spark_cluster_setup.sh --run-setup`, which handles:
   - IP address assignment on all CX7 interfaces via netplan
   - Passwordless SSH key distribution between all nodes
   - NCCL bandwidth test

The generated config file (containing credentials) is securely deleted on exit.

```bash
bash scripts/setup-spark-ring.sh
```

### 2. `scripts/setup-vllm.sh`

Deploys a distributed vLLM inference server across all three nodes. Run after `setup-spark-ring.sh`.

**What it does:**
1. Verifies CX7 interfaces are UP and have IPs; derives the RDMA device for NCCL
2. Discovers nodes via mDNS and verifies passwordless SSH
3. Pulls the vLLM Docker image on all nodes in parallel
4. Downloads `Qwen/Qwen3.6-27B` from HuggingFace on all nodes in parallel
5. Drops page caches on all nodes to maximise available memory
6. Starts worker containers (ranks 1, 2) on remote nodes via SSH
7. Starts the head container (rank 0) locally using vLLM's **native multiprocessing backend** — no Ray
8. Polls the `/health` endpoint until the server is ready, then runs a smoke test

```bash
bash scripts/setup-vllm.sh
```

The vLLM API is served on the head node at `http://localhost:8000/v1`.

**Key configuration** (edit at the top of the script):

| Variable | Default | Description |
|---|---|---|
| `VLLM_IMAGE` | `nvcr.io/nvidia/vllm:26.02-py3` | NGC vLLM container image |
| `MODEL` | `Qwen/Qwen3.6-27B` | HuggingFace model ID |
| `TENSOR_PARALLEL_SIZE` | `3` | Total GPUs across all nodes |
| `VLLM_PORT` | `8000` | HTTP API port |
| `SPARK_HOSTNAME_PREFIX` | `gx10-` | mDNS hostname filter |

## References

- [Connect three DGX Sparks in a ring topology](https://build.nvidia.com/spark/connect-three-sparks/three-sparks-ring)
- [vLLM on multiple Sparks through a switch](https://build.nvidia.com/spark/vllm/multi-sparks-through-switch)
- [NVIDIA dgx-spark-playbooks](https://github.com/NVIDIA/dgx-spark-playbooks)

