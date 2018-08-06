set -xe

if [[ "$DOCS" != "true" ]]; then
    # Install runtime and test dependencies on the docker container
    htcluster exec -- ./skein/continuous_integration/travis/docker_install.sh $PYTHON
else
    # Install runtime and documentation dependencies
    pip install grpcio grpcio-tools pyyaml cryptography sphinx numpydoc sphinxcontrib.autoprogram doctr
    # Build only the python library so that docs can be built
    python setup.py develop --no-java
fi

set +xe
