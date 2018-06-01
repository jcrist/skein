#!/usr/bin/env bash
set -xe

conda install grpcio pyyaml cryptography pytest flake8

pip install grpcio-tools

# Copy and chown the built files, due to docker permissions issue
cp -r ~/workdir ~/skein
chown -R testuser:testuser /home/testuser/skein

cd ~/skein
pip install -e .

conda list
