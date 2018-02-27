FROM centos:centos7
MAINTAINER jcrist

# Install CDH5 in a single node: Pseudo Distributed
# Docs: http://www.cloudera.com/content/www/en-us/documentation/enterprise/latest/topics/cdh_qs_yarn_pseudo.html
RUN cd /etc/yum.repos.d/ && { curl -O https://archive.cloudera.com/cdh5/redhat/7/x86_64/cdh/cloudera-cdh5.repo ; cd -; } && \
    rpm --import https://archive.cloudera.com/cdh5/redhat/7/x86_64/cdh/RPM-GPG-KEY-cloudera && \
    yum install -y sudo bzip2 git java-1.8.0-openjdk-headless hadoop-conf-pseudo hadoop-libhdfs krb-server krb-workstation krb-libs && \
    yum clean all && \
    rm -rf /var/cache/yum

# Comment out default_ccache_name in krb5.conf
RUN sed -i -e 's/#//' -e 's/default_ccache_name/# default_ccache_name/' /etc/krb5.conf

# Install conda:
RUN curl https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh && \
    /bin/bash /tmp/miniconda.sh -b -p /opt/conda && \
    rm /tmp/miniconda.sh
ENV PATH /opt/conda/bin:$PATH

COPY docker-files/core-site.xml /etc/hadoop/conf/
COPY docker-files/hdfs-site.xml /etc/hadoop/conf/
COPY docker-files/yarn-site.xml /etc/hadoop/conf/
COPY docker-files/start-namenode.sh /tmp/
COPY docker-files/start-datanode.sh /tmp/
COPY docker-files/start-kdc.sh /tmp/

ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.161-0.b14.el7_4.x86_64/jre/
ENV LIBHDFS3_CONF /etc/hadoop/conf/hdfs-site.xml
ENV HADOOP_CONF_DIR /etc/hadoop/conf
