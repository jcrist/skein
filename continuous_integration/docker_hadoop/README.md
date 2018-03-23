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
