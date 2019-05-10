# Apache Cassandra Change-Data-Capture example project

This repository contains the sample project for reading Apache Cassandra commit log file in CDC location and outputs in JSON format.

## Build

```bash
$ ./mvnw package -DskipTests
```

This will produce `cassandra-cdc-json-VERSION.tar.gz` in `target` directory.

## Running

Make sure you are running Apache Cassandra with CDC enabled.
See https://cassandra.apache.org/doc/latest/operating/cdc.html for enabling CDC.

Upload the artifact `cassandra-cdc-json-VERSION.tar.gz` to you cassandra nodes, expand it to desired directory.
Then run the following with user who is running Apache Cassandra.

```
$ cd cassandra-cdc-json-VERSION
$ bin/cassandra-cdc.sh
```

The application should work if you install Apache Cassandra with package manager like `yum`, but if not,
set `CASSANDRA_INCLUDE` environment variable that points to your Apache Cassandra installation's `cassandra.in.sh`.

```
# CASSANDRA_INCLUDE=/path/to/cassandra/bin/cassandra.in.sh bin/cassandra-cdc.sh
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

