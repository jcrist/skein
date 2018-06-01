#!/usr/bin/env bash
set -xe

conda install grpcio pyyaml cryptography pytest

cd ~/workdir
pip install -e .

conda list
