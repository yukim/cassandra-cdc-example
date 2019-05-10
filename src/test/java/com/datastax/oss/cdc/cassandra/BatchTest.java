package com.datastax.oss.cdc.cassandra;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

@DisplayName("Test for combining multiple DML with BATCH statement")
class BatchTest extends CqlToChangeEventTest {

    @Test
    @DisplayName("Insert rows into the same partition")
    void testBatchSamePartitionKey() {
        long now = System.currentTimeMillis();
        String sb = "BEGIN BATCH\n" +
                "INSERT INTO cluster_table (key, cl1, val) VALUES ('key', " + now + ", 1);\n" +
                "INSERT INTO cluster_table (key, cl1, val) VALUES ('key', " + (now + 100) + ", 2);\n" +
                "INSERT INTO cluster_table (key, cl1, val) VALUES ('key', " + (now + 200) + ", 3);\n" +
                "INSERT INTO cluster_table (key, cl1, val) VALUES ('key', " + (now + 300) + ", 4);\n" +
                "APPLY BATCH";
        List<ChangeEvent> events = run(sb);
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);
    }

    @Test
    @DisplayName("Delete whole partition then insert row at later timestamp")
    void testBatchInsertDelete() {
        long now = System.currentTimeMillis();
        String sb = "BEGIN BATCH\n" +
                "DELETE FROM cluster_table USING TIMESTAMP 100 WHERE key = 'key';\n" +
                "INSERT INTO cluster_table (key, cl1, val) VALUES ('key', " + (now + 100) + ", 2) USING TIMESTAMP 200;\n" +
                "APPLY BATCH";
        List<ChangeEvent> events = run(sb);
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);
    }

    @Test
    void testBatchSamePartitionKeyAcrossTables() {
        String sb = "BEGIN BATCH\n" +
                "INSERT INTO table1 (key1, col1, col2, col3) VALUES ('key', 1, 2, 3);\n" +
                "INSERT INTO table2 (key2, col1) VALUES ('key', 100);\n" +
                "APPLY BATCH";
        List<ChangeEvent> events = run(sb);
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);
    }

    @Test
    void testBatchDifferentPartitionKeys() {
        String sb = "BEGIN BATCH\n" +
                "INSERT INTO table1 (key1, col1, col2, col3) VALUES ('key1', 1, 3, 5);\n" +
                "INSERT INTO table1 (key1, col1, col2, col3) VALUES ('key2', 2, 4, 6);\n" +
                "APPLY BATCH";
        List<ChangeEvent> events = run(sb);
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);
    }

    @Test
    void testBatchWithDeleteAndUpdate() {
        long now = System.currentTimeMillis();
        String sb = "BEGIN BATCH\n" +
                "DELETE FROM cluster_table WHERE key='key1' AND cl1 < " + (now - 100) + ";\n" +
                "INSERT INTO cluster_table (key, cl1, val) VALUES ('key1', " + now + ", 1);\n" +
                "APPLY BATCH";
        List<ChangeEvent> events = run(sb);
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);
    }

    @Test
    void testBatchWithDelete() {
        long now = System.currentTimeMillis();
        String sb = "BEGIN BATCH\n" +
                "DELETE FROM cluster_table USING TIMESTAMP 1 WHERE key='key' AND cl1 > 0 AND cl1 < 100;\n" +
                "DELETE FROM cluster_table USING TIMESTAMP 2 WHERE key='key' AND cl1 > 90 AND cl1 < 200;\n" +
                "APPLY BATCH";
        List<ChangeEvent> events = run(sb);
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);
    }

    @Test
    void testBatchWithDeletes() {
        long now = System.currentTimeMillis();
        String sb = "BEGIN BATCH\n" +
                "DELETE FROM cluster_table USING TIMESTAMP 1000 WHERE key='key1' AND cl1 < " + (now - 100) + ";\n" +
                "DELETE FROM cluster_table USING TIMESTAMP 2000 WHERE key='key1' AND cl1 > " + (now - 100) + ";\n" +
//                "DELETE FROM cluster_table USING TIMESTAMP 3000 WHERE key='key1' AND cl1 >= " + (now - 200) + " AND cl1 <= " + (now + 100) + ";\n" +
                "APPLY BATCH";
        List<ChangeEvent> events = run(sb);
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);
    }

    @Test
    void testBatchUpdateColumnsWithDifferentTimestamp() {
        String sb = "BEGIN BATCH\n" +
                "INSERT INTO table1 (key1, col1, col2, col3) VALUES ('key', 1, 2, 3) USING TIMESTAMP 1000;\n" +
                "UPDATE table1 USING TIMESTAMP 2000 SET col1=4 WHERE key1='key';\n" +
                "UPDATE table1 USING TIMESTAMP 3000 SET col2=5 WHERE key1='key';\n" +
                "UPDATE table1 USING TIMESTAMP 4000 SET col3=6 WHERE key1='key';\n" +
                "APPLY BATCH";
        List<ChangeEvent> events = run(sb);
        events.stream().map(JsonOutput::toJson).forEach(System.out::println);
    }

    @Override
    List<String> createTableStatement() {
        return Arrays.asList(
            "CREATE TABLE table1 (" +
            "  key1 text PRIMARY KEY," +
            "  col1 int, " +
            "  col2 int, " +
            "  col3 int " +
            ")",
            "CREATE TABLE table2 (" +
            "  key2 text PRIMARY KEY," +
            "  col1 int " +
            ")",
            "CREATE TABLE cluster_table (" +
            "  key text," +
            "  cl1 timestamp, " +
            "  val int, " +
            "  PRIMARY KEY ((key), cl1)" +
            ")"
        );
    }
}
