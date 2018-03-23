# Hadoop Docker Images

For testing purposes, infrastructure for setting up a mini hadoop cluster using
docker is provided here. Two setups are provided:

- `base`: uses `simple` authentication (unix user permissions)
- `kerberos`: uses `kerberos` for authentication

Each cluster has three containers:

- One `master` node running the `hdfs-namenode` and `yarn-resourcemanager` (in
  the `kerberos` setup, the kerberos daemons also run here).
- One `worker` node running the `hdfs-datanode` and `yarn-nodemanager`
- One `edge` node for interacting with the cluster

One user account has also been created for testing purposes:

- Login: `testuser`
- Password: `testpass`

For the `kerberos` setup, a keytab for this user has been put at
`/home/testuser/testuser.keytab`, so you can kinit easily like `kinit -kt
/home/testuser/testuser.keytab testuser`

An admin kerberos principal has also been created for use with `kadmin`:

- Login: `admin/admin`
- Password: `adminpass`

## Ports

The full address is dependent on the IP address of your docker-machine driver,
which can be found at:

```
docker-machine inspect --format {{.Driver.IPAddress}})
```

- NameNode RPC: 9000
- NameNode Webui: 50070
- ResourceManager Webui: 8088
- Kerberos KDC: 88
- Kerberos Kadmin: 749
- DataNode Webui: 50075
- NodeManager Webui: 8042

## The `hcluster` script

To work with either cluster, please use the `hcluster` convenience script. This
is a thin wrapper around `docker-compose`, with utilities for quickly doing
most common actions.

```
$ ./hcluster --help
usage: hcluster [-h] command ...

Helper script for working with dockerized hadoop clusters.

Commands:
    startup         Start up the cluster
    login           Login to one of the nodes
    shutdown        Shutdown the cluster
    compose         Forward commands to underlying docker-compose call
    kerbenv         Output environment variables to setup kerberos locally

Additionally, the following commands are aliases for

    hcluster compose base COMMAND ARGS...

since they work fine regardless of which cluster is running.

Aliases:
    kill            Kill containers
    logs            View output from containers
    pause           Pause services
    ps              List containers
    restart         Restart services
    start           Start services
    stop            Stop services
    top             Display the running processes
    unpause         Unpause services
```

### Starting a cluster

```
./hcluster startup CLUSTER_TYPE
```

### Starting a cluster, mounting the java source as a working directory

```
./hcluster startup CLUSTER_TYPE --workdir ../../java/
```

### Login to the edge node

```
./hcluster login
```

### Shutdown the cluster

```
./hcluster shutdown
```

## Authenticating with Kerberos from outside Docker

In the kerberized cluster, the webui's are secured by kerberos, and so won't be
accessible from your browser unless you configure kerberos properly. This is
doable, but takes a few steps:

1. Kerberos/SPNEGO requires that the requested url matches the hosts domain.
   The easiest way to do this is to modify your `/etc/hosts` and add a line for
   `master.example.com`:

   ```
   # Add a line to /etc/hosts pointing master.example.com to your docker-machine
   # driver ip address.
   # Note that you probably need to run this command as a super user.
   echo "$(docker-machine inspect --format {{.Driver.IPAddress}})  master.example.com" >> /etc/hosts
   ```

2. You must have `kinit` installed locally. You may already have it, otherwise
   it's available through most package managers.

3. You need to tell kerberos where the `krb5.conf` is for this domain. This is
   done with an environment variable. To make this easy, `hcluster` has a
   command to do this:

   ```
   eval $(./hcluster kerbenv)
   ```

4. At this point you should be able to kinit as testuser:

   ```
   kinit testuser@EXAMPLE.COM
   ```

5. To access kerberos secured pages in your browser you'll need to do a bit of
   (simple) configuration. See [this documentation from
   Cloudera](https://www.cloudera.com/documentation/enterprise/5-9-x/topics/cdh_sg_browser_access_kerberos_protected_url.html)
   for information on what's needed for your browser.

6. Since environment variables are only available for processes started in the
   environment, you have three options here:

   - Restart your browser from the shell in which you added the environment
     variables

   - Manually get a ticket for the `HTTP/master.example.com` principal. Note
     that this will delete your other tickets, but works fine if you just want
     to see the webpage

     ```
     kinit -S HTTP/master.example.com testuser
     ```

   - Use `curl` to authenticate the first time, at which point you'll already
     have the proper tickets in your cache, and the browser authentication will
     just work. Note that your version of curl must support the GSS-API.

     ```
     $ curl -V  # Check your version of curl supports GSS-API
     curl 7.59.0 (x86_64-apple-darwin17.2.0) libcurl/7.59.0 SecureTransport zlib/1.2.11
     Release-Date: 2018-03-14
     Protocols: dict file ftp ftps gopher http https imap imaps ldap ldaps pop3 pop3s rtsp smb smbs smtp smtps telnet tftp
     Features: AsynchDNS IPv6 Largefile GSS-API Kerberos SPNEGO NTLM NTLM_WB SSL libz UnixSockets

     $ curl --negotiate -u : http://master.example.com:50070  # get a HTTP ticket for master.example.com
     ```

   After doing one of these, you should be able to access any of the pages from
   your browser.
