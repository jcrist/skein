#!/bin/bash

# cd into home directory
cd

# initialize as if login shell
if [ -f .bash_profile ]; then
    source .bash_profile;
fi
