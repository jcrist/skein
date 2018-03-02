FROM centos:centos7
MAINTAINER jcrist

# Install CDH
RUN cd /etc/yum.repos.d/ && { curl -O https://archive.cloudera.com/cdh5/redhat/7/x86_64/cdh/cloudera-cdh5.repo ; cd -; } \
    && rpm --import https://archive.cloudera.com/cdh5/redhat/7/x86_64/cdh/RPM-GPG-KEY-cloudera \
    && yum install -y \
        sudo \
        bzip2 \
        git \
        java-1.8.0-openjdk-headless \
        hadoop-yarn-resourcemanager \
        hadoop-hdfs-namenode \
        hadoop-hdfs-secondarynamenode \
        hadoop-yarn-nodemanager \
        hadoop-hdfs-datanode \
        hadoop-mapreduce \
        hadoop-mapreduce-historyserver \
        hadoop-yarn-proxyserver \
        hadoop-client \
        hadoop-libhdfs \
    && yum clean all \
    && rm -rf /var/cache/yum

# Install kerberos
RUN yum install -y \
        krb5-libs \
        krb5-server \
        krb5-workstation \
    && yum clean all \
    && rm -rf /var/cache/yum

# Install supervisord
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
    && python get-pip.py \
    && pip install supervisor \
    && rm get-pip.py

# Make a non-privileged account for edge node and install conda for that account
RUN adduser testuser
USER testuser

RUN curl https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh \
    && /bin/bash /tmp/miniconda.sh -b -p /home/testuser/miniconda \
    && rm /tmp/miniconda.sh \
    && echo 'export PATH="/home/testuser/miniconda/bin:$PATH"' >> /home/testuser/.bashrc \
    && /home/testuser/miniconda/bin/conda update conda -y

# Revert to root permissions for rest of file
USER root

# Copy over files
COPY ./files /

# Configure hdfs
# We use the non-kerberized cluster for the dockerfile
# The kerberized cluster will re-run alternatives on startup
RUN cp /etc/hadoop/conf.empty/log4j.properties /etc/hadoop/conf.test/log4j.properties \
    && chmod 6050 /etc/hadoop/conf.kerb/container-executor.cfg \
    && alternatives --install /etc/hadoop/conf hadoop-conf /etc/hadoop/conf.test 50 \
    && alternatives --set hadoop-conf /etc/hadoop/conf.test

RUN mkdir -p /tmp/hadoop-yarn/local /tmp/hadoop-yarn/logs \
    && chown -R yarn:yarn /tmp/hadoop-yarn/local /tmp/hadoop-yarn/logs

# Format namenode
RUN sudo -u hdfs hdfs namenode -format -force

# Configure Kerberos
RUN kdb5_util create -s -P testpass \
    && kadmin.local -q "addprinc -randkey hdfs/nn.example.com@EXAMPLE.COM" \
    && kadmin.local -q "addprinc -randkey mapred/nn.example.com@EXAMPLE.COM" \
    && kadmin.local -q "addprinc -randkey yarn/nn.example.com@EXAMPLE.COM" \
    && kadmin.local -q "addprinc -randkey HTTP/nn.example.com@EXAMPLE.COM" \
    && kadmin.local -q "xst -norandkey -k /etc/hadoop/conf.kerb/hdfs.keytab hdfs/nn.example.com HTTP/nn.example.com" \
    && kadmin.local -q "xst -norandkey -k /etc/hadoop/conf.kerb/mapred.keytab mapred/nn.example.com HTTP/nn.example.com" \
    && kadmin.local -q "xst -norandkey -k /etc/hadoop/conf.kerb/yarn.keytab yarn/nn.example.com HTTP/nn.example.com" \
    && kadmin.local -q "xst -norandkey -k /etc/hadoop/conf.kerb/HTTP.keytab HTTP/nn.example.com" \
    && chown hdfs:mapred /etc/hadoop/conf.kerb/hdfs.keytab \
    && chown mapred:mapred /etc/hadoop/conf.kerb/mapred.keytab \
    && chown yarn:mapred /etc/hadoop/conf.kerb/yarn.keytab \
    && chown hdfs:mapred /etc/hadoop/conf.kerb/HTTP.keytab \
    && chmod 400 /etc/hadoop/conf.kerb/*.keytab

ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.161-0.b14.el7_4.x86_64/jre/
ENV LIBHDFS3_CONF /etc/hadoop/conf/hdfs-site.xml
ENV HADOOP_CONF_DIR /etc/hadoop/conf
ENV HADOOP_HOME /usr/lib/hadoop
ENV HADOOP_COMMON_HOME /usr/lib/hadoop
ENV HADOOP_MAPRED_HOME /usr/lib/hadoop-mapreduce
ENV HADOOP_YARN_HOME /usr/lib/hadoop-yarn
ENV HADOOP_HDFS_HOME /usr/lib/hadoop-hdfs
