set -xe

if [[ "$DOCS" != "true" ]]; then
    if [[ "$PYTHON" == "2.7" ]]; then
        export CONDA_ENV="/home/testuser/miniconda/envs/py27"
    else
        export CONDA_ENV="/home/testuser/miniconda/"
    fi
    # kinit if needed
    if [[ "$CLUSTER_TYPE" == "kerberos" ]]; then
        htcluster exec -- kinit testuser -kt testuser.keytab
    fi
    # Run py.test inside docker
    for i in `seq 1 20`; do
        exitcode=`htcluster exec -- $CONDA_ENV/bin/py.test skein/skein/ -vv -s -k test_memory_limit_exceeded`
        if [[ "$exitcode" != "0" ]]; then
            break
        fi
    done
    # linting
    htcluster exec -- $CONDA_ENV/bin/flake8 skein/skein/
else
    # Build docs
    pushd docs
    make html
    popd
    # Maybe deploy documentation
    if [[ "$TRAVIS_BRANCH" == "master" && "$TRAVIS_EVENT_TYPE" == "push" ]]; then
        doctr deploy . --built-docs docs/build/html/
    fi
fi

set +xe
