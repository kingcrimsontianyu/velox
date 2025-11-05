#!/usr/bin/env bash

BUILD_TYPE=debug
VELOX_CUDF_TPCH_BENCHMARK=/opt/velox-build/${BUILD_TYPE}/velox/experimental/cudf/benchmarks/velox_cudf_tpch_benchmark
SCALE_FACTOR=0.01

# DATA_PATH=s3://biu-velox/velox-tpch-data/sf-${SCALE_FACTOR}
# DATA_PATH=https://biu-velox.s3.us-east-1.amazonaws.com/velox-tpch-data/sf-${SCALE_FACTOR}
DATA_PATH=/mnt/nvme/velox-tpch-data/sf-${SCALE_FACTOR}

OUTPUT_PATH=/mnt/nvme/velox-tpch-result/tmp.txt

# export AWS_CONFIG_FILE="/mnt/host_home/.aws/config"
# export AWS_SHARED_CREDENTIALS_FILE="/mnt/host_home/.aws/credentials"

# export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id)
# export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key)
# export AWS_DEFAULT_REGION=$(aws configure get region)

${VELOX_CUDF_TPCH_BENCHMARK} \
--data_path=${DATA_PATH} --data_format=parquet \
--run_query_verbose=05 --num_repeats=2 --num_drivers=4 --preferred_output_batch_rows=100000 \
--max_output_batch_rows=100000 \
--cudf_chunk_read_limit=1073741824 \
--cudf_pass_read_limit=0 2>&1 | tee ${OUTPUT_PATH}
