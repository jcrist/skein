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

## The `compose.sh` script

To work with either cluster, please use the `compose.sh` convenience script,
which properly sets the image environment variable before forwarding the
remaining arguments to `docker-compose`. The syntax is:

```
./compose.sh CLUSTER-TYPE [ARGS...]
```

where `CLUSTER-TYPE` is either `base` or `kerberos`.


### Starting a cluster

```
./compose.sh CLUSTER-TYPE up -d
```

### Login to the edge node

```
./compose.sh CLUSTER-TYPE exec -u testuser edge bash
```

### Shutdown the cluster

```
./compose.sh CLUSTER-TYPE down
```
