FROM centos:centos7
MAINTAINER jcrist

# Install CDH
RUN cd /etc/yum.repos.d/ && { curl -O https://archive.cloudera.com/cdh5/redhat/7/x86_64/cdh/cloudera-cdh5.repo ; cd -; } \
    && rpm --import https://archive.cloudera.com/cdh5/redhat/7/x86_64/cdh/RPM-GPG-KEY-cloudera \
    && yum install -y \
        sudo \
        bzip2 \
        git \
        java-1.8.0-openjdk-devel \
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

# Install maven
RUN curl -O www.apache.org/dist/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz \
    && tar xzf apache-maven-3.3.9-bin.tar.gz \
    && mkdir /usr/local/maven \
    && mv apache-maven-3.3.9/ /usr/local/maven/ \
    && alternatives --install /usr/bin/mvn mvn /usr/local/maven/apache-maven-3.3.9/bin/mvn 1 \
    && rm apache-maven-3.3.9-bin.tar.gz

# Install supervisord
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
    && python get-pip.py \
    && pip install supervisor \
    && rm get-pip.py

# Make a non-privileged account for edge node and install conda for that account
RUN useradd -m testuser
USER testuser

RUN mkdir /home/testuser/workdir

RUN curl https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh \
    && /bin/bash /tmp/miniconda.sh -b -p /home/testuser/miniconda \
    && rm /tmp/miniconda.sh \
    && echo 'export PATH="/home/testuser/miniconda/bin:$PATH"' >> /home/testuser/.bashrc \
    && /home/testuser/miniconda/bin/conda update conda -y \
    && /home/testuser/miniconda/bin/conda config --set always_yes yes --set changeps1 no

# Copy over files
COPY --chown=testuser ./testuser /home/testuser

# Revert to root permissions for rest of file
USER root

# Copy over files
COPY ./files /

# Configure and setup hadoop
RUN /root/setup-hadoop.sh

ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.171-8.b10.el7_5.x86_64/jre/
ENV LIBHDFS3_CONF /etc/hadoop/conf/hdfs-site.xml
ENV HADOOP_CONF_DIR /etc/hadoop/conf
ENV HADOOP_HOME /usr/lib/hadoop
ENV HADOOP_COMMON_HOME /usr/lib/hadoop
ENV HADOOP_MAPRED_HOME /usr/lib/hadoop-mapreduce
ENV HADOOP_YARN_HOME /usr/lib/hadoop-yarn
ENV HADOOP_HDFS_HOME /usr/lib/hadoop-hdfs
