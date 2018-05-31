# /bin/bash

set -ex

# Configure HDFS
cp /etc/hadoop/conf.empty/log4j.properties /etc/hadoop/conf.test/log4j.properties \
    && alternatives --install /etc/hadoop/conf hadoop-conf /etc/hadoop/conf.test 50 \
    && alternatives --set hadoop-conf /etc/hadoop/conf.test

# Create yarn directories with proper permissions
mkdir -p /var/tmp/hadoop-yarn/local /var/tmp/hadoop-yarn/logs \
    && chown -R yarn:yarn /var/tmp/hadoop-yarn/local /var/tmp/hadoop-yarn/logs

# Create secret key to authenticate web access
dd if=/dev/urandom bs=64 count=1 > /etc/hadoop/conf/http-secret-file
chown hdfs:hadoop /etc/hadoop/conf/http-secret-file
chmod 440 /etc/hadoop/conf/http-secret-file

# Format namenode
sudo -u hdfs hdfs namenode -format -force

# Format filesystem
# NOTE: Even though the worker and master will be different filesystems at
# *runtime*, the directories they write to are different so we can intitialize
# both in the same image. This is a bit of a hack, but makes startup quicker
# and easier.
# XXX: Add to hosts to resolve name temporarily
echo "127.0.0.1 master.example.com" >> /etc/hosts
su -c "hdfs namenode" hdfs&
su -c "hdfs datanode" hdfs&
sudo -u hdfs /root/init-hdfs.sh
killall java
