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

# Make a non-privileged account for edge node and install conda for that account
RUN adduser testuser
USER testuser

RUN curl https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh \
    && /bin/bash /tmp/miniconda.sh -b -p /home/testuser/miniconda \
    && rm /tmp/miniconda.sh \
    && echo 'export PATH="/home/testuser/miniconda/bin:$PATH"' >> /home/testuser/.bashrc

# Revert to root permissions for rest of file
USER root

# Configure hdfs
COPY conf.test/ /etc/hadoop/conf.test/
RUN alternatives --install /etc/hadoop/conf hadoop-conf /etc/hadoop/conf.test 50 \
    && alternatives --set hadoop-conf /etc/hadoop/conf.test

# Copy over startup scripts
COPY startup/ /tmp/startup/

ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.161-0.b14.el7_4.x86_64/jre/
ENV LIBHDFS3_CONF /etc/hadoop/conf/hdfs-site.xml
