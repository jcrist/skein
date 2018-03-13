#! /bin/bash

kinit $1 -k -t $2 && $3
exit_code=$?
kdestroy
exit $exit_code
