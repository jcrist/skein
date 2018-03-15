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
which properly sets the environment variables before forwarding the
remaining arguments to `docker-compose`.

```
$ ./compose.sh --help
Helper script for working with dockerized hadoop clusters.

./compose.sh CLUSTER_TYPE [--workdir WORKING_DIR] [ARGS...]

Options:
    CLUSTER_TYPE                The type of cluster. Either base or kerberos.
    --workdir WORKING_DIR       If specified, this directory will be mounted as
                                a volume at /home/testuser/working
    ARGS...                     All remaining arguments are forwarded to the
                                underlying docker-compose command.

Example:

    To start a kerberos enabled cluster with the current directory mounted as a
    working directory:

    ./compose.sh kerberos --workdir . up -d
```

### Starting a cluster

```
./compose.sh CLUSTER_TYPE up -d
```

### Starting a cluster, mounting the java source as a working directory

```
./compose.sh CLUSTER_TYPE --workdir ../../java/ up -d
```

### Login to the edge node

```
./compose.sh CLUSTER_TYPE exec -u testuser edge bash
```

### Shutdown the cluster

```
./compose.sh CLUSTER_TYPE down
```
