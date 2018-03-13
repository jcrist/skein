#! /bin/bash

case "$1" in
base)
    export HADOOP_TESTING_IMAGE=jcrist/hadoop-testing-base
    ;;
kerberos)
    export HADOOP_TESTING_IMAGE=jcrist/hadoop-testing-kerberos
    ;;
*)
    echo "Case must be in [base, kerberos], got $1"
    exit 1
    ;;
esac

shift

docker-compose -f docker-compose.yml "$@"
