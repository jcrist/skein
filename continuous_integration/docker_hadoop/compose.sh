#! /bin/bash

case "$1" in
base)
    export HADOOP_TESTING_IMAGE=jcrist/hadoop-testing-base
    ;;
kerberos)
    export HADOOP_TESTING_IMAGE=jcrist/hadoop-testing-kerberos
    ;;
--help)
    echo "\
Helper script for working with dockerized hadoop clusters.

./compose.sh CLUSTER_TYPE [--workdir WORKING_DIR] [ARGS...]

Options:
    CLUSTER_TYPE                The type of cluster. Either base or kerberos.
    --workdir WORKING_DIR       If specified, this directory will be mounted as
                                a volume at /home/testuser/working
    ARGS...                     All remaining arguments are forwarded to the
                                underlying docker-compose command.

Example:

    To start a kerberos enabled cluster with the current directory mounted as a
    working directory:

    ./compose.sh kerberos --workdir . up -d
"
    exit 0
    ;;
*)
    echo "Case must be in [base, kerberos], got $1"
    exit 1
    ;;
esac

shift

case "$1" in
-w|--workdir)
    export HADOOP_TESTING_WORKING_DIR="$2"
    shift
    shift
    docker-compose -f docker-compose.yml -f add-workdir.yml "$@"
    ;;
*)
    docker-compose -f docker-compose.yml "$@"
    ;;
esac
