package com.datastax.oss.cdc.cassandra;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.datastax.oss.cdc.cassandra.ChangeEventType.DELETE;
import static com.datastax.oss.cdc.cassandra.ChangeEventType.UPDATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@DisplayName("Test for the table that contains both static columns and clustering columns")
class StaticColumnsTest extends CqlToChangeEventTest {

    @Test
    @DisplayName("Insert into table with static columns and clustering column")
    void testInsert() {
        List<ChangeEvent> events = run("INSERT INTO my_static_table (key, static1, static2, cl1, col1) VALUES ('key', 'static', 1, 2, 3)");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);

        // Events should have one for static columns insert and the other for row insert
        assertEquals(2, events.size());

        ChangeEvent event = events.get(0);
        assertEquals("static_columns_test", event.getKeyspaceName());
        assertEquals("my_static_table", event.getTableName());
        assertEquals(UPDATE, event.getEventType());
        Map<String, Object> columns = event.getRow().getColumns();
        assertEquals("key", columns.remove("key"));
        assertEquals("static", columns.remove("static1"));
        assertEquals(1, columns.remove("static2"));
        assertEquals(0, columns.size());
        assertNull(event.getDeletion());

        event = events.get(1);
        assertEquals("static_columns_test", event.getKeyspaceName());
        assertEquals("my_static_table", event.getTableName());
        assertEquals(UPDATE, event.getEventType());
        columns = event.getRow().getColumns();
        assertEquals("key", columns.remove("key"));
        assertEquals(2, columns.remove("cl1"));
        assertEquals(3, columns.remove("col1"));
        assertEquals(0, columns.size());
        assertNull(event.getDeletion());
    }

    @Test
    @DisplayName("Insert only partition key and static column")
    void testInsertStaticColumnOnly() {
        List<ChangeEvent> events = run("INSERT INTO my_static_table (key, static1) VALUES ('key', 'static_only')");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);
        assertEquals(1, events.size());

        ChangeEvent event = events.get(0);
        assertEquals("static_columns_test", event.getKeyspaceName());
        assertEquals("my_static_table", event.getTableName());
        assertEquals(UPDATE, event.getEventType());

        Map<String, Object> columns = event.getRow().getColumns();
        assertEquals("key", columns.remove("key"));
        assertEquals("static_only", columns.remove("static1"));
        assertEquals(0, columns.size());

        assertNull(event.getDeletion());
    }

    @Test
    @DisplayName("Update only static column")
    void testUpdateStaticColumnOnly() {
        List<ChangeEvent> events = run("UPDATE my_static_table SET static1 = 'new' WHERE key = 'key'");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);
        assertEquals(1, events.size());

        ChangeEvent event = events.get(0);
        assertEquals("static_columns_test", event.getKeyspaceName());
        assertEquals("my_static_table", event.getTableName());
        assertEquals(UPDATE, event.getEventType());

        Map<String, Object> columns = event.getRow().getColumns();
        assertEquals("key", columns.remove("key"));
        assertEquals("new", columns.remove("static1"));
        assertEquals(0, columns.size());

        assertNull(event.getDeletion());
    }

    @Test
    @DisplayName("Delete static column")
    void testDeleteStaticColumn() {
        List<ChangeEvent> events = run("DELETE static1 FROM my_static_table WHERE key = '1'");
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);
        assertEquals(1, events.size());

        ChangeEvent event = events.get(0);
        assertEquals("static_columns_test", event.getKeyspaceName());
        assertEquals("my_static_table", event.getTableName());
        assertEquals(DELETE, event.getEventType());

        List<String> deletedColumns = event.getDeletion().getColumns();
        assertEquals(1, deletedColumns.size());
        assertEquals("static1", deletedColumns.get(0));

        List<Criteria> deletionCriteria = event.getDeletion().getCriteria();
        assertEquals(1, deletionCriteria.size());
        Criteria c1 = deletionCriteria.get(0);
        assertEquals("key", c1.getColumn());
        assertEquals("1", c1.getCondition());

        assertNull(event.getRow());
    }

    @Test
    @DisplayName("Insert static column and several rows using BATCH")
    void testInsertStaticAndClusteringColumn() {
        String sb = "BEGIN BATCH\n" +
                "INSERT INTO my_static_table (key, static1) VALUES ('key', 'static_col');\n" +
                "INSERT INTO my_static_table (key, cl1, col1) VALUES ('key', 1, 100);\n" +
                "INSERT INTO my_static_table (key, cl1, col1) VALUES ('key', 2, 200);\n" +
                "INSERT INTO my_static_table (key, cl1, col1) VALUES ('key', 3, 300);\n" +
                "APPLY BATCH";
        List<ChangeEvent> events = run(sb);
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);
        assertEquals(4, events.size());

        ChangeEvent event = events.get(0);
        assertEquals("static_columns_test", event.getKeyspaceName());
        assertEquals("my_static_table", event.getTableName());
        assertEquals(UPDATE, event.getEventType());

        Map<String, Object> columns = event.getRow().getColumns();
        assertEquals("key", columns.remove("key"));
        assertEquals("static_col", columns.remove("static1"));
        assertEquals(0, columns.size());

        assertNull(event.getDeletion());
    }
    @Override
    List<String> createTableStatement() {
        return Collections.singletonList("CREATE TABLE my_static_table (" +
                "  key text," +
                "  static1 text static," +
                "  static2 int static," +
                "  cl1 int," +
                "  col1 int," +
                "  PRIMARY KEY ((key), cl1)" +
                ")");
    }
}
