#!/usr/bin/env bash

python3 -m venv biu-venv-velox
source biu-venv-velox/bin/activate

data_script_dir=../../benchmark_data_tools
scale_factor=0.01
data_dest_dir=/mnt/nvme_ubuntu_test/velox-tpch-data/sf-${scale_factor}

pip install -r ${data_script_dir}/requirements.txt
python ${data_script_dir}/generate_data_files.py -b tpch -d $data_dest_dir -s ${scale_factor} -c
