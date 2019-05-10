package com.datastax.oss.cdc.cassandra;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;

import static com.datastax.oss.cdc.cassandra.ChangeEventType.DELETE;
import static com.datastax.oss.cdc.cassandra.ChangeEventType.UPDATE;
import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Test for the table with collection(frozen and unfrozen) columns")
class CollectionTableTest extends CqlToChangeEventTest {

    @Test
    void testInsert() {
        List<ChangeEvent> events = run("INSERT INTO map_table (key, col1) VALUES ('key', {'foo': 1, 'bar': 2})");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);

        // INSERT INTO unfrozen map produces deletion also
        assertEquals(2, events.size());

        ChangeEvent event = events.get(0);
        assertEquals("collection_table_test", event.getKeyspaceName());
        assertEquals("map_table", event.getTableName());
        assertEquals(DELETE, event.getEventType());

        List<String> deletedColumns = event.getDeletion().getColumns();
        assertEquals(1, deletedColumns.size());
        assertEquals("col1", deletedColumns.get(0));

        List<Criteria> criteria = event.getDeletion().getCriteria();
        assertEquals(1, criteria.size());
        assertEquals("key", criteria.get(0).getColumn());
        assertEquals("key", criteria.get(0).getCondition());

        assertNull(event.getRow());

        event = events.get(1);
        assertEquals("collection_table_test", event.getKeyspaceName());
        assertEquals("map_table", event.getTableName());
        assertEquals(UPDATE, event.getEventType());

        Map<String, Object> columns = event.getRow().getColumns();
        assertEquals("key", columns.remove("key"));
        Map<String, Integer> col1 = (Map<String, Integer>) columns.remove("col1");
        assertEquals(1, col1.remove("foo"));
        assertEquals(2, col1.remove("bar"));
        assertTrue(col1.isEmpty());
        assertEquals(0, columns.size());

        assertNull(event.getDeletion());
    }

    @Test
    @DisplayName("Insert empty map")
    void testInsertEmptyMap() {
        List<ChangeEvent> events = run("INSERT INTO map_table (key, col1) VALUES ('key', {})");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);

        // Empty map == null == column deletion
        assertEquals(1, events.size());

        ChangeEvent event = events.get(0);
        assertEquals("collection_table_test", event.getKeyspaceName());
        assertEquals("map_table", event.getTableName());
        assertEquals(DELETE, event.getEventType());

        List<String> deletedColumns = event.getDeletion().getColumns();
        assertEquals(1, deletedColumns.size());
        assertEquals("col1", deletedColumns.get(0));

        List<Criteria> criteria = event.getDeletion().getCriteria();
        assertEquals(1, criteria.size());
        assertEquals("key", criteria.get(0).getColumn());
        assertEquals("key", criteria.get(0).getCondition());

        assertNull(event.getRow());
    }

    @Test
    void testUpdate() {
        List<ChangeEvent> events = run("UPDATE map_table " +
                "SET col1['foo']=1, " +
                "    col1['bar']=2 " +
                "WHERE key='key'");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);
    }

    @Test
    void testDeleteWholeMap() {
        List<ChangeEvent> events = run("DELETE col1 FROM map_table WHERE key='key'");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);
    }

    @Test
    void testDeleteMapKey() {
        List<ChangeEvent> events = run("DELETE col1['foo'] FROM map_table WHERE key='key'");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);
    }

    @Test
    void testInsertFrozenMap() {
        List<ChangeEvent> events = run("INSERT INTO frozen_map_table (key, col1) VALUES ('key', {'foo': 1, 'bar': 2})");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);

        assertEquals(1, events.size());

        ChangeEvent event = events.get(0);
        assertEquals("collection_table_test", event.getKeyspaceName());
        assertEquals("frozen_map_table", event.getTableName());
        assertEquals(UPDATE, event.getEventType());

        Map<String, Object> columns = event.getRow().getColumns();
        assertEquals("key", columns.remove("key"));
        Map<String, Integer> col1 = (Map<String, Integer>) columns.remove("col1");
        assertEquals(1, col1.remove("foo"));
        assertEquals(2, col1.remove("bar"));
        assertTrue(col1.isEmpty());
        assertEquals(0, columns.size());

        assertNull(event.getDeletion());
    }

    @Test
    void testDeleteWholeFrozenMap() {
        List<ChangeEvent> events = run("DELETE col1 FROM frozen_map_table " +
                "WHERE key='key'");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);
    }

    @Override
    List<String> createTableStatement() {
        return Arrays.asList("CREATE TABLE map_table (" +
                "  key text PRIMARY KEY," +
                "  col1 map<text, int> " +
                ")",
                "CREATE TABLE frozen_map_table (" +
                "  key text PRIMARY KEY," +
                "  col1 frozen<map<text, int>> " +
                ")");
    }
}
