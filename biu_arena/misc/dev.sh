#!/usr/bin/env bash

set -euo pipefail

WORKING_DIR=/mnt/biu

source "../scripts/config.sh"

docker compose -f "$COMPOSE_FILE" run --rm \
-v ~/.sccache-auth/aws_credentials:/root/.aws/credentials:ro \
-v $(pwd):/mnt/biu \
-v ${HOME}:/mnt/host_home \
-v /mnt/nvme_ubuntu_test:/mnt/nvme \
-w ${WORKING_DIR} \
"${CONTAINER_NAME}" bash -c '
# Source conda if available
if [ -f "/opt/miniforge/etc/profile.d/conda.sh" ]; then
    source "/opt/miniforge/etc/profile.d/conda.sh"
    conda activate adapters
fi

# Setup HDFS classpath
export CLASSPATH=$(/usr/local/hadoop/bin/hdfs classpath --glob)

# Set a user-friendly prompt showing current directory
export PS1="\[\033[01;32m\]\u@\h\[\033[00m\]:\[\033[01;34m\]\w\[\033[00m\]\$ "

# Start interactive bash
exec bash
'
