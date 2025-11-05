#!/usr/bin/env bash

set -euxo pipefail

# ${parameter:-default}: If parameter is not set, use the default
HOST_USER=${HOST_USER:-biubiuty}
HOST_UID=${HOST_UID:-1000}
HOST_GID=${HOST_GID:-1000}

# Install sudo first
echo "Installing sudo..."
yum install -y sudo

echo "Creating user '${HOST_USER}' with UID=${HOST_UID} and GID=${HOST_GID}"

# Create group if it doesn't exist
if ! getent group ${HOST_GID} > /dev/null 2>&1; then
    groupadd -g ${HOST_GID} ${HOST_USER}
else
    # In "biubiuty:x:1000:", select "biubiuty"
    GROUP_NAME=$(getent group ${HOST_GID} | cut -d: -f1)
    echo "Group with GID ${HOST_GID} already exists: ${GROUP_NAME}"
fi

# Create user if it doesn't exist
if ! id -u ${HOST_USER} > /dev/null 2>&1; then
    useradd -u ${HOST_UID} -g ${HOST_GID} -m -s /bin/bash ${HOST_USER}
    echo "User ${HOST_USER} created"
else
    echo "User ${HOST_USER} already exists"
fi

# Add to sudoers with NOPASSWD
echo "${HOST_USER} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/${HOST_USER}
chmod 0440 /etc/sudoers.d/${HOST_USER}

# Create home directory structure
USER_HOME="/home/${HOST_USER}"
mkdir -p ${USER_HOME}
chown -R ${HOST_UID}:${HOST_GID} ${USER_HOME}

# Copy root's bashrc as template if user doesn't have one
if [ -f /root/.bashrc ] && [ ! -f ${USER_HOME}/.bashrc ]; then
    cp /root/.bashrc ${USER_HOME}/.bashrc
    chown ${HOST_UID}:${HOST_GID} ${USER_HOME}/.bashrc
fi

# Install useful tools
echo "Installing additional tools..."
yum update -y
yum install -y gdb

echo "User setup complete!"


