package com.datastax.oss.cdc.cassandra;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.EncodingVersion;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.WrappedDataOutputStreamPlus;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.QueryState;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

public abstract class CqlToChangeEventTest {

    private static final String TEST_DIR = "./test_dir";

    private QueryState state;

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
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspaceName);
        if (ksm == null) {
            Tables.Builder tables = Tables.builder();
            createTableStatement()
                    .stream()
                    .map(t -> CreateTableStatement.parse(t, keyspaceName).build())
                    .filter(table -> Schema.instance.getTableMetadataIfExists(keyspaceName, table.name) == null)
                    .forEach(tables::add);
            Schema.instance.load(KeyspaceMetadata.create(keyspaceName, KeyspaceParams.simple(1), tables.build()));
        }
        if (state == null) {
            state = QueryState.forInternalCalls();
        }
        state.getClientState().setKeyspace(keyspaceName);
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
        return CQLUtil.toMutation(cql, state, timestamp).stream()
                .map(rawMutation -> {
                    try (ByteArrayOutputStream bios = new ByteArrayOutputStream();
                         WrappedDataOutputStreamPlus dos = new WrappedDataOutputStreamPlus(bios)) {
                        Mutation.MutationSerializer serializer = Mutation.rawSerializers.get(EncodingVersion.last());
                        serializer.serialize(rawMutation, dos);
                        dos.flush();

                        // Deserialize again for debug
                        return serializer.deserialize(new DataInputBuffer(bios.toByteArray()));
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
