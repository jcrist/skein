#!/usr/bin/env bash
set -xe

conda install \
    grpcio \
    pyyaml \
    cryptography \
    pytest \
    flake8 \
    requests \
    pyarrow \
    nomkl \
    pykerberos \
    requests-kerberos \
    tornado

pip install grpcio-tools

cd ~/skein
pip install -v --no-deps -e .

conda list
