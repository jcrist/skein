#! /bin/bash

alternatives --install /etc/hadoop/conf hadoop-conf /etc/hadoop/conf.kerb 50 \
&& alternatives --set hadoop-conf /etc/hadoop/conf.kerb \
&& ln -s "/etc/hadoop/conf.kerb/$1-keytabs" /etc/hadoop/conf.kerb/keytabs/ \
&& if [ -n "$2" ]; then supervisorctl -c $2 start main:*; fi
