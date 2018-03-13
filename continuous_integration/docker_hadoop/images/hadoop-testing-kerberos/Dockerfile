FROM jcrist/hadoop-testing-base
MAINTAINER jcrist

# Install kerberos
RUN yum install -y \
        krb5-libs \
        krb5-server \
        krb5-workstation \
    && yum clean all \
    && rm -rf /var/cache/yum

# Copy over files
COPY ./files /

# Fix container-executor permissions
RUN chmod 6050 /etc/hadoop/conf.test/container-executor.cfg

# Setup kerberos
RUN /root/setup-kerb.sh
