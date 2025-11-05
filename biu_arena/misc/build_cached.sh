#!/usr/bin/env bash

script_dir=../scripts

cd ${script_dir}

# ./build_centos_deps_image.sh
# ./setup_sccache_auth.sh

# export SCCACHE_AUTH_DIR="${HOME}/.sccache-auth"
./build_velox.sh --sccache --build-type Debug
