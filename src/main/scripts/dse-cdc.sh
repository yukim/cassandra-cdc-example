#!/bin/sh

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [ -z "$DSE_ENV" ]; then
    for include in "$HOME/.dse-env.sh" \
                   "`dirname "$0"`/dse-env.sh" \
                   "`dirname "$0"`/../../../../bin/dse-env.sh" \
                   "/etc/dse/dse-env.sh"; do
        if [ -r "$include" ]; then
            DSE_ENV="$include"
            break
        fi
    done
fi

if [ -z "$DSE_ENV" ]; then
    echo "DSE_ENV could not be determined."
    exit 1
elif [ -r "$DSE_ENV" ]; then
    . "$DSE_ENV"
else
    echo "Location pointed by DSE_ENV not readable: $DSE_ENV"
    exit 1
fi

JVM_OPTS="$DSE_OPTS $JVM_OPTS"
JVM_OPTS="$JVM_OPTS -Djava.library.path=$JAVA_LIBRARY_PATH"

if [ "x$CASSANDRA_INCLUDE" = "x" ]; then
    for include in /usr/share/cassandra/cassandra.in.sh \
                   /usr/local/share/cassandra/cassandra.in.sh \
                   /opt/cassandra/cassandra.in.sh \
                   "$HOME/.cassandra.in.sh" \
                   "`dirname "$0"`/cassandra.in.sh"; do
        if [ -r "$include" ]; then
            . "$include"
            break
        fi
    done
elif [ -r "$CASSANDRA_INCLUDE" ]; then
    . "$CASSANDRA_INCLUDE"
fi

# Add CDC jar to CLASSPATH
for jar in "`dirname "$0"`/.."/lib/*.jar; do
    CLASSPATH="$CLASSPATH:$jar"
done

# Use JAVA_HOME if set, otherwise look for java in PATH
if [ -x "$JAVA_HOME/bin/java" ]; then
    JAVA="$JAVA_HOME/bin/java"
else
    JAVA="`which java`"
fi

if [ "x$JAVA" = "x" ]; then
    echo "Java executable not found (hint: set JAVA_HOME)" >&2
    exit 1
fi

if [ -z "$CLASSPATH" ]; then
    echo "You must set the CLASSPATH var" >&2
    exit 1
fi

if [ "x${MAX_HEAP_SIZE}" = "x" ]; then
    MAX_HEAP_SIZE="256M"
fi

if [ "x${MAX_DIRECT_MEMORY}" = "x" ]; then
    MAX_DIRECT_MEMORY="2G"
fi

JVM_OPTS="${JVM_OPTS} -Xmx${MAX_HEAP_SIZE} -XX:MaxDirectMemorySize=${MAX_DIRECT_MEMORY}"

"${JAVA}" ${JAVA_AGENT} -ea -cp "${CLASSPATH}" ${JVM_OPTS} \
        -Dcassandra.storagedir="${cassandra_storagedir}" \
        -Dlogback.configurationFile=logback-tools.xml \
        com.datastax.bdp.tools.ShellToolWrapper \
        com.datastax.oss.cdc.cassandra.ChangeDataCapture "$@"

# vi:ai sw=4 ts=4 tw=0 et
