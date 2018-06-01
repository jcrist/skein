#!/usr/bin/env bash
set -xe

conda install maven grpcio pyyaml cryptography pytest flake8

pip install grpcio-tools

python setup.py develop

conda list
