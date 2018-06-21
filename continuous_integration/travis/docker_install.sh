#!/usr/bin/env bash
set -xe

packages="grpcio pyyaml cryptography pytest flake8"

if [[ $1 == "2.7" ]]; then
    conda create -n py27 python=2.7 $packages
    source activate py27
else
    conda install $packages
fi

pip install grpcio-tools

# Copy and chown the built files, due to docker permissions issue
cp -r ~/workdir ~/skein
chown -R testuser:testuser /home/testuser/skein

cd ~/skein
pip install -v --no-deps -e .

conda list
