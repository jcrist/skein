#! /bin/bash

alternatives --install /etc/hadoop/conf hadoop-conf /etc/hadoop/conf.kerb 50 \
&& alternatives --set hadoop-conf /etc/hadoop/conf.kerb \
&& if [ -n "$1" ]; then supervisorctl -c $1 start main:*; fi
