package com.datastax.oss.cdc.cassandra;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.datastax.oss.cdc.cassandra.ChangeEventType.DELETE;
import static com.datastax.oss.cdc.cassandra.ChangeEventType.UPDATE;
import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Test for the table that does not contain static columns, clustering columns, nor collections")
class StandardTableTest extends CqlToChangeEventTest {

    @Test
    @DisplayName("Insert into standard table")
    void testInsert() {
        List<ChangeEvent> events = run("INSERT INTO my_table (key, col1, col2, col3) VALUES ('key', 1, 2, 1000)");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);

        assertEquals(1, events.size(), "Should be just one event");

        ChangeEvent event = events.get(0);
        assertEquals("standard_table_test", event.getKeyspaceName());
        assertEquals("my_table", event.getTableName());
        assertEquals(UPDATE, event.getEventType());

        Map<String, Object> columns = event.getRow().getColumns();
        assertEquals("key", columns.remove("key"));
        assertEquals(1, columns.remove("col1"));
        assertEquals(2L, columns.remove("col2"));
        assertNotNull(columns.remove("col3"));
        assertEquals(0, columns.size());

        assertNull(event.getDeletion());
    }

    @Test
    @DisplayName("Insert into standard table using timestamp")
    void testInsertUsingTimestamp() {
        Instant timestamp = Instant.parse("1900-01-02T03:04:05.666Z");
        long timestampValue = TimeUnit.MILLISECONDS.toMicros(timestamp.toEpochMilli());
        List<ChangeEvent> events = run(String.format("INSERT INTO my_table (key, col1, col2) VALUES ('key', 1, 2) USING TIMESTAMP %d;", timestampValue));
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);

        assertEquals(1, events.size(), "Should be just one event");

        ChangeEvent event = events.get(0);
        assertEquals("standard_table_test", event.getKeyspaceName());
        assertEquals("my_table", event.getTableName());
        assertEquals(UPDATE, event.getEventType());
        assertEquals(timestamp, event.getEventTimestamp());

        Map<String, Object> columns = event.getRow().getColumns();
        assertEquals("key", columns.remove("key"));
        assertEquals(1, columns.remove("col1"));
        assertEquals(2L, columns.remove("col2"));
        assertEquals(0, columns.size());

        assertNull(event.getDeletion());
    }

    @Test
    @DisplayName("Insert only partition key")
    void testInsertPartitionKeyOnly() {
        List<ChangeEvent> events = run("INSERT INTO my_table (key) VALUES ('key_only')");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);

        assertEquals(1, events.size(), "Should be just one event");

        ChangeEvent event = events.get(0);
        assertEquals("standard_table_test", event.getKeyspaceName());
        assertEquals("my_table", event.getTableName());
        assertEquals(UPDATE, event.getEventType());

        Map<String, Object> columns = event.getRow().getColumns();
        assertEquals("key_only", columns.remove("key"));
        assertEquals(0, columns.size());

        assertNull(event.getDeletion());
    }

    @Test
    @DisplayName("Insert null values")
    void testInsertNull() {
        List<ChangeEvent> events = run("INSERT INTO my_table (key, col1, col2, col3) VALUES ('key', null, null, null)");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);

        assertEquals(1, events.size());

        ChangeEvent event = events.get(0);
        assertEquals("standard_table_test", event.getKeyspaceName());
        assertEquals("my_table", event.getTableName());
        assertEquals(DELETE, event.getEventType());

        List<String> deletedColumns = event.getDeletion().getColumns();
        assertEquals(3, deletedColumns.size());
        assertEquals("col1", deletedColumns.get(0));
        assertEquals("col2", deletedColumns.get(1));
        assertEquals("col3", deletedColumns.get(2));

        List<Criteria> deletionCriteria = event.getDeletion().getCriteria();
        assertEquals(1, deletionCriteria.size());
        Criteria c1 = deletionCriteria.get(0);
        assertEquals("key", c1.getColumn());
        assertEquals("key", c1.getCondition());

        assertNull(event.getRow());
    }

    @Test
    @DisplayName("Update standard table")
    void testUpdate() {
        List<ChangeEvent> events = run("UPDATE my_table SET col1=1, col2=2 WHERE key='key'");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);

        assertEquals(1, events.size(), "Should be just one event");

        ChangeEvent event = events.get(0);
        assertEquals("standard_table_test", event.getKeyspaceName());
        assertEquals("my_table", event.getTableName());
        assertEquals(UPDATE, event.getEventType());

        Map<String, Object> updated = event.getRow().getColumns();
        assertEquals("key", updated.remove("key"));
        assertEquals(1, updated.remove("col1"));
        assertEquals(2L, updated.remove("col2"));
        assertEquals(0, updated.size());

        assertNull(event.getDeletion());
    }

    @Test
    @DisplayName("Update with null value")
    void testUpdateNull() {
        List<ChangeEvent> events = run("UPDATE my_table SET col1=null WHERE key='key'");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);

        assertEquals(1, events.size(), "Should be just one event");

        ChangeEvent event = events.get(0);
        assertEquals("standard_table_test", event.getKeyspaceName());
        assertEquals("my_table", event.getTableName());
        assertEquals(DELETE, event.getEventType());

        List<String> deletedColumns = event.getDeletion().getColumns();
        assertEquals(1, deletedColumns.size());
        assertEquals("col1", deletedColumns.get(0));

        List<Criteria> deletionCriteria = event.getDeletion().getCriteria();
        assertEquals(1, deletionCriteria.size());
        Criteria c1 = deletionCriteria.get(0);
        assertEquals("key", c1.getColumn());
        assertEquals("key", c1.getCondition());

        assertNull(event.getRow());
    }

    @Test
    @DisplayName("Delete partition")
    void testDeletePartition() {
        List<ChangeEvent> events = run("DELETE FROM my_table WHERE key = '1'");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);

        assertEquals(1, events.size(), "Should be just one event");
        ChangeEvent event = events.get(0);
        assertEquals("standard_table_test", event.getKeyspaceName());
        assertEquals("my_table", event.getTableName());
        assertEquals(DELETE, event.getEventType());

        List<String> deletedColumns = event.getDeletion().getColumns();
        assertTrue(deletedColumns.isEmpty());

        List<Criteria> deletionCriteria = event.getDeletion().getCriteria();
        assertEquals(1, deletionCriteria.size());
        Criteria c1 = deletionCriteria.get(0);
        assertEquals("key", c1.getColumn());
        assertEquals("1", c1.getCondition());

        assertNull(event.getRow());
    }

    @Test
    @DisplayName("Delete column from partition")
    void testDeleteColumn() {
        List<ChangeEvent> events = run("DELETE col1 FROM my_table WHERE key = '1'");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);

        assertEquals(1, events.size());
        ChangeEvent event = events.get(0);
        assertEquals("standard_table_test", event.getKeyspaceName());
        assertEquals("my_table", event.getTableName());
        assertEquals(DELETE, event.getEventType());

        List<String> deletedColumns = event.getDeletion().getColumns();
        assertEquals(1, deletedColumns.size());
        assertEquals("col1", deletedColumns.get(0));

        List<Criteria> deletionCriteria = event.getDeletion().getCriteria();
        assertEquals(1, deletionCriteria.size());
        Criteria c1 = deletionCriteria.get(0);
        assertEquals("key", c1.getColumn());
        assertEquals("1", c1.getCondition());

        assertNull(event.getRow());
    }

    @Test
    @DisplayName("Delete multiple partitions")
    void testDeleteMultiplePartitions() {
        List<ChangeEvent> events = run("DELETE FROM my_table WHERE key in ('1', '2', '3')");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);

        assertEquals(3, events.size());
        for (int i = 1; i <= 3; i++) {
            ChangeEvent event = events.get(i - 1);
            assertEquals("standard_table_test", event.getKeyspaceName());
            assertEquals("my_table", event.getTableName());
            assertEquals(DELETE, event.getEventType());

            List<String> deletedColumns = event.getDeletion().getColumns();
            assertTrue(deletedColumns.isEmpty());

            List<Criteria> deletionCriteria = event.getDeletion().getCriteria();
            assertEquals(1, deletionCriteria.size());
            Criteria c1 = deletionCriteria.get(0);
            assertEquals("key", c1.getColumn());
            assertEquals(Integer.toString(i), c1.getCondition());

            assertNull(event.getRow());

        }
    }

    @Override
    List<String> createTableStatement() {
        return Collections.singletonList("CREATE TABLE my_table (" +
                "  key text PRIMARY KEY," +
                "  col1 int," +
                "  col2 bigint," +
                "  col3 timestamp)");
    }
}
