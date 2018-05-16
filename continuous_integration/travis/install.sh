#!/usr/bin/env bash
set -xe

wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
bash miniconda.sh -b -p $HOME/miniconda
export PATH="$HOME/miniconda/bin:$PATH"

conda config --set always_yes yes --set changeps1 no

conda install maven grpcio pyyaml cryptography pytest flake8

pip install grpcio-tools

if [[ "$DOCS" == "true" ]]; then
    conda install sphinx numpydoc;
    pip install doctr;
fi

pip install --no-deps -e .

conda list
