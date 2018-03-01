#! /bin/bash

sudo -u hdfs hdfs namenode -format \
&& supervisord -c /etc/nn.supervisord.conf
