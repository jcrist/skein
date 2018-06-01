#!/usr/bin/env bash
set -xe

conda install grpcio pyyaml cryptography pytest flake8

pip install grpcio-tools

cd ~/workdir
python setup.py install

conda list
