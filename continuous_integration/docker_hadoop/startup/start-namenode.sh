#! /bin/bash

sudo -u hdfs hdfs namenode -format

/etc/init.d/hadoop-hdfs-namenode start

# Create /tmp
sudo -u hdfs hadoop fs -mkdir /tmp
sudo -u hdfs hadoop fs -chmod -R 1777 /tmp

/etc/init.d/hadoop-yarn-resourcemanager start

# Create a user directory for each user
sudo -u hdfs hadoop fs -mkdir -p /user/testuser
sudo -u hdfs hadoop fs -chown testuser /user/testuser

sleep infinity
