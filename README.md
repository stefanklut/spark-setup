# spark-setup
For setting up sparks in a ring config

Follow;
1. https://build.nvidia.com/spark/connect-three-sparks/three-sparks-ring
2. https://build.nvidia.com/spark/vllm/multi-sparks-through-switch

Encountered problems:
 - Cables need to be in a very specific order
 - Use IP that is in the config for HEAD_NODE_IP (Not the $VLLM_HOST_IP)
