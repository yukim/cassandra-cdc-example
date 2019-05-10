package com.datastax.oss.cdc.cassandra;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.datastax.oss.cdc.cassandra.ChangeEventType.DELETE;
import static com.datastax.oss.cdc.cassandra.ChangeEventType.UPDATE;
import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Test for the table that contains multiple clustering columns")
class ClusteringTableTest extends CqlToChangeEventTest {
    @Test
    @DisplayName("Insert into clustered partition`")
    void testInsert() {
        List<ChangeEvent> events = run(String.format("INSERT INTO clustering_table (key1, key2, cluster1, cluster2, col1, col2) " +
                "VALUES ('pk', 1, 100, 100, 150, %d);", System.currentTimeMillis()));
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);

        assertEquals(1, events.size());
    }

    @Test
    @DisplayName("Insert clustering columns only")
    void testInsertOnlyClusteringColumns() {
        List<ChangeEvent> events = run("INSERT INTO clustering_table (key1, key2, cluster1, cluster2) VALUES ('pk', 1, 100, 200)");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);

        assertEquals(1, events.size());

        ChangeEvent event = events.get(0);
        assertEquals("clustering_table_test", event.getKeyspaceName());
        assertEquals("clustering_table", event.getTableName());
        assertEquals(UPDATE, event.getEventType());

        Map<String, Object> columns = event.getRow().getColumns();
        assertEquals("pk", columns.remove("key1"));
        assertEquals(1, columns.remove("key2"));
        assertEquals(100, columns.remove("cluster1"));
        assertEquals(200, columns.remove("cluster2"));
        assertTrue(columns.isEmpty());

        assertNull(event.getDeletion());
    }

    @Test
    @DisplayName("Delete whole partition")
    void deleteWithPartitionKeyTest() {
        List<ChangeEvent> events = run("DELETE FROM clustering_table " +
                "WHERE key1 = 'pk' " +
                "AND key2 = 1");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);
        assertEquals(1, events.size());

        ChangeEvent event = events.get(0);
        assertEquals("clustering_table_test", event.getKeyspaceName());
        assertEquals("clustering_table", event.getTableName());
        assertEquals(DELETE, event.getEventType());

        Deletion deletion = event.getDeletion();
        assertTrue(deletion.getColumns().isEmpty());
        List<Criteria> criteria = deletion.getCriteria();
        assertEquals(2, criteria.size());
        assertEquals("key1", criteria.get(0).getColumn());
        assertEquals("pk", criteria.get(0).getCondition());
        assertEquals("key2", criteria.get(1).getColumn());
        assertEquals(1, criteria.get(1).getCondition());

        assertNull(event.getRow());
    }

    @Test
    @DisplayName("Delete single row")
    void deleteWithRowTest() {
        List<ChangeEvent> events = run("DELETE FROM clustering_table " +
                "WHERE key1 = 'pk' " +
                "AND key2 = 1 " +
                "AND cluster1 = 100 " +
                "AND cluster2 = 100");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);
    }

    @Test
    @DisplayName("Delete a column from a row")
    void deleteColumnOfRowTest() {
        List<ChangeEvent> events = run("DELETE col1 FROM clustering_table " +
                "WHERE key1 = 'pk' " +
                "AND key2 = 1 " +
                "AND cluster1 = 100 " +
                "AND cluster2 = 100");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);
    }

    @Test
    @DisplayName("Delete several rows using IN")
    void deleteWithRowsTest() {
        List<ChangeEvent> events = run("DELETE FROM clustering_table " +
                "WHERE key1 = 'pk' " +
                "AND key2 = 1 " +
                "AND cluster1 = 100 " +
                "AND cluster2 in (100, 200, 300)");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);
    }

    @Test
    @DisplayName("Delete several rows with equality")
    void deleteWithWholeClusterTest() {
        List<ChangeEvent> events = run("DELETE FROM clustering_table " +
                "WHERE key1 = 'pk' " +
                "AND key2 = 1 " +
                "AND cluster1 = 100");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);
    }

    @Test
    @DisplayName("Delete several rows with range")
    void deleteWithClusteringRangeTest() {
        List<ChangeEvent> events = run("DELETE FROM clustering_table " +
                "WHERE key1 = 'pk' " +
                "AND key2 = 1 " +
                "AND cluster1 = 100 " +
                "AND cluster2 > 0");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);
    }

    @Override
    List<String> createTableStatement() {
        return Collections.singletonList("CREATE TABLE clustering_table (" +
                "key1 text, " +
                "key2 int, " +
                "cluster1 int, " +
                "cluster2 int, " +
                "col1 bigint, " +
                "col2 timestamp, " +
                "PRIMARY KEY ((key1, key2), cluster1, cluster2)" +
                ")");
    }
}
