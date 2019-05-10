package com.datastax.oss.cdc.cassandra;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.WrappedDataOutputStreamPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.Pair;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class CqlToChangeEventTest {

    private static final String TEST_DIR = "./test_dir";

    private ClientState client;

    @BeforeAll
    public static void initialize() {
        System.setProperty("cassandra.storagedir", System.getProperty("cassandra.storagedir", TEST_DIR));
        if (!DatabaseDescriptor.isToolInitialized()) {
            DatabaseDescriptor.toolInitialization();
            Keyspace.setInitialized();
        }
    }

    @AfterAll
    public static void clearSchema() {
        File testDir = new File(TEST_DIR);
        if (testDir.exists()) {
            testDir.delete();
        }
    }

    @BeforeEach
    public void setUp() {
        // keyspace name is test class name converted to snake case
        String keyspaceName = this.getClass().getSimpleName().replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase();

        // table is created only once
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(keyspaceName);
        if (ksm == null) {
            Tables.Builder tables = Tables.builder();
            createTableStatement()
                    .stream()
                    .filter(tableName -> !Schema.instance.hasCF(Pair.create(keyspaceName, tableName)))
                    .map(t -> CFMetaData.compile(t, keyspaceName))
                    .forEach(tables::add);
            Schema.instance.load(KeyspaceMetadata.create(keyspaceName, KeyspaceParams.simple(1), tables.build()));
        }
        if (client == null) {
            client = ClientState.forInternalCalls();
        }
        client.setKeyspace(keyspaceName);
    }

    protected List<ChangeEvent> run(String cql) {
        return run(cql, System.currentTimeMillis());
    }

    /**
     * Converts given CQL string to {@link Mutation} and then passed to
     *
     * @param cql CQL statement that is converted to JSON
     * @param timestamp timestamp that the CQL is executed in microseconds
     */
    protected List<ChangeEvent> run(String cql, long timestamp) {
        System.out.println(cql);
        return CQLUtil.toMutation(cql, client, timestamp).stream()
                .map(rawMutation -> {
                    try (ByteArrayOutputStream bios = new ByteArrayOutputStream();
                         WrappedDataOutputStreamPlus dos = new WrappedDataOutputStreamPlus(bios)) {
                        Mutation.serializer.serialize(rawMutation, dos, MessagingService.current_version);
                        dos.flush();

                        // Deserialize again for debug
                        return Mutation.serializer.deserialize(new DataInputBuffer(bios.toByteArray()), MessagingService.current_version);
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                })
                .peek(System.out::println)
                .flatMap(mutation -> mutation.getPartitionUpdates().stream())
                .map(PartitionParser::new)
                .collect(ArrayList::new, (l, p) -> l.addAll(p.toChangeEvents()), ArrayList::addAll);
    }

    abstract List<String> createTableStatement();
}
