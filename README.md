# DataStax Enterprise Change-Data-Capture example project

This branch contains the sample project for reading DataStax Enterprise(v6.0 and above) commit log file in CDC location and outputs in JSON format.

## Build

In order to build DSE version, first you have to download DSE from https://downloads.datastax.com.
Down load "DataStax Enterprise" tarball and expand it to the project directory.

```bash
$ cd cassandra-cdc-example
$ tar xzvf dse.tar.gz
```

[pom.xml](./pom.xml) uses this DSE directory to reference dependent java libraries.

```bash
$ ./mvnw package -DskipTests
```

This will produce `cassandra-cdc-json-VERSION.tar.gz` in `target` directory.

## Running

Make sure you are running DSE with CDC enabled.
See https://cassandra.apache.org/doc/latest/operating/cdc.html for enabling CDC.

Upload the artifact `cassandra-cdc-json-VERSION.tar.gz` to your DSE nodes, expand it to desired directory.
Then run the following with user who is running DataStax Enterprise.

```
$ cd cassandra-cdc-json-VERSION
$ bin/dse-cdc.sh
```

The application should work if you install DataStax Enterprise with package manager like `yum`, but if not,
set `DSE_ENV` environment variable that points to your DataStax Enterprise installation's `dse-env.sh`.

```
# DSE_ENV=$DSE_HOME/bin/dse-env.sh bin/dse-cdc.sh
```

## ChangeEvent

Partitions inside Mutation are first converted to list of `ChangeEvent`s.
A `ChangeEvent` represents an update happened to certain CQL row or deletion criteria at a specific timestamp.

For details, see [ChangeEvent.java](src/main/java/com/datastax/oss/cdc/cassandra/ChangeEvent.java).

## Limitation

- Since this program only loads schema at the start up, changes made since the application start up cannot be picked up.
- The following CQL patterns are not implemented or tested yet:
    - Complex CQL types: `set`, `list`, `counter` and User Defined Type (UDT)
    - Time to live (TTL)
    - `INSERT JSON`

