#! /bin/bash

sudo -u hdfs hdfs namenode -format

/etc/init.d/hadoop-hdfs-namenode start

echo "Namenode Started"

sleep infinity
