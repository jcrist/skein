#! /bin/bash

kdb5_util -P testpass create -s

## password only user
kadmin.local -q "addprinc  -randkey testuser"
kadmin.local -q "ktadd -k /var/keytabs/testuser.keytab testuser"

kadmin.local -q "addprinc -randkey HTTP/server.example.com"
kadmin.local -q "ktadd -k /var/keytabs/server.keytab HTTP/server.example.com"

kadmin.local -q "addprinc -randkey hdfs/nn.example.com"
kadmin.local -q "addprinc -randkey HTTP/nn.example.com"
kadmin.local -q "addprinc -randkey hdfs/dn1.example.com"
kadmin.local -q "addprinc -randkey HTTP/dn1.example.com"

kadmin.local -q "ktadd -k /var/keytabs/hdfs.keytab hdfs/nn.example.com"
kadmin.local -q "ktadd -k /var/keytabs/hdfs.keytab HTTP/nn.example.com"
kadmin.local -q "ktadd -k /var/keytabs/hdfs.keytab hdfs/dn1.example.com"
kadmin.local -q "ktadd -k /var/keytabs/hdfs.keytab HTTP/dn1.example.com"

chown hdfs /var/keytabs/hdfs.keytab

keytool -genkey -alias nn.example.com -keyalg rsa -keysize 1024 -dname "CN=nn.example.com" -keypass testpass -keystore /var/keytabs/hdfs.jks -storepass testpass
keytool -genkey -alias dn1.example.com -keyalg rsa -keysize 1024 -dname "CN=dn1.example.com" -keypass testpass -keystore /var/keytabs/hdfs.jks -storepass testpass

chmod 700 /var/keytabs/hdfs.jks
chown hdfs /var/keytabs/hdfs.jks

krb5kdc -n
