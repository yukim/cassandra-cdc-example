package com.datastax.oss.cdc.cassandra;

import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ChangeEventBuilder {
    private final TableMetadata metadata;
    private final List<Column> partitionKeys = new ArrayList<>();
    private Stack<RowEvent> rowEvents = new Stack<>();
    private List<ChangeEvent> parsedEvents = new ArrayList<>();

    /**
     * @param metadata
     */
    public ChangeEventBuilder(TableMetadata metadata) {
        this.metadata = metadata;
    }

    public void addPartitionKey(String name, Object value) {
        partitionKeys.add(new Column(name, value));
    }

    /**
     * Mark this partition is deleted at given timestamp
     *
     * @param timestamp timestamp this partition is deleted in microseconds
     */
    public void partitionIsDeletedAt(long timestamp) {
        DeletionImpl deletion = new DeletionImpl();
        for (Column c : partitionKeys) {
            deletion.addCriteria(Criteria.equals(c.name, c.value));
        }
        ChangeEvent event = new DefaultChangeEvent(
                metadata.keyspace,
                metadata.name,
                metadata.id.asUUID(),
                Instant.ofEpochMilli(TimeUnit.MICROSECONDS.toMillis(timestamp)),
                deletion);
        parsedEvents.add(event);
    }

    public void addRangeTombstone(RangeTombstone rt) {
        Instant timestamp = Instant.ofEpochMilli(TimeUnit.MICROSECONDS.toMillis(rt.deletionTime().markedForDeleteAt()));

        DeletionImpl deletion = new DeletionImpl();
        for (Column c : partitionKeys) {
            deletion.addCriteria(Criteria.equals(c.name, c.value));
        }
        ClusteringBound start = rt.deletedSlice().start();
        ClusteringBound end = rt.deletedSlice().end();
        for (int i = 0; i < Math.max(start.size(), end.size()); i++) {
            ColumnMetadata def = metadata.clusteringColumns().get(i);
            Object startValue = i < start.size() ? def.type.getSerializer().deserialize(start.get(i)) : null;
            Object endValue = i < end.size() ? def.type.getSerializer().deserialize(end.get(i)) : null;
            if (startValue != null && startValue.equals(endValue)) {
                deletion.addCriteria(Criteria.equals(def.name.toString(), startValue));
            } else {
                deletion.addCriteria(Criteria.range(def.name.toString(), startValue, endValue, start.isInclusive(), end.isInclusive()));
            }
        }
        parsedEvents.add(new DefaultChangeEvent(metadata.keyspace,
                metadata.name,
                metadata.id.asUUID(),
                timestamp,
                deletion));
    }

    public void addStatic() {
        rowEvents.add(new RowEvent(LivenessInfo.NO_TIMESTAMP));
    }

    public void newRow(long timestamp) {
        rowEvents.add(new RowEvent(timestamp));
    }

    public void addClusteringColumn(String name, Object value) {
        RowEvent currentRow = rowEvents.peek();
        if (currentRow != null) {
            currentRow.addClusteringColumn(name, value);
        }
    }

    /**
     * Add column name and value seen with associated timestamp.
     *
     * @param name name of column
     * @param value value of column
     * @param timestamp in microseconds
     */
    public void addColumn(String name, Object value, long timestamp) {
        RowEvent currentRow = rowEvents.peek();
        if (currentRow != null) {
            currentRow.addColumn(name, value, timestamp);
        }
    }

    public void addDeletedColumn(String name, long timestamp) {
        RowEvent currentRow = rowEvents.peek();
        if (currentRow != null) {
            currentRow.addDeletedColumn(name, timestamp);
        }
    }

    public void markDeletedAt(long timestamp) {
        RowEvent currentRow = rowEvents.peek();
        if (currentRow != null) {
            currentRow.deletedAt(timestamp);
        }
    }

    public List<ChangeEvent> build(TableMetadata metadata) {
        for (RowEvent rowEvent : rowEvents) {
            parsedEvents.addAll(rowEvent.build(metadata, partitionKeys));
        }
        return parsedEvents;
    }

    private static class RowEvent {
        private long rowTimestamp;
        private final List<Column> clusteringColumns = new ArrayList<>();
        private Map<Instant, List<Column>> columnsByTime = new HashMap<>();
        private Map<Instant, List<String>> deletedColumnsByTime = new HashMap<>();
        private boolean deletion = false;

        public RowEvent(long timestamp) {
            this.rowTimestamp = timestamp;
        }

        private void deletedAt(long timestamp) {
            deletion = true;
            rowTimestamp = timestamp;
        }

        private void addClusteringColumn(String name, Object value) {
            clusteringColumns.add(new Column(name, value));
        }

        private void addColumn(String name, Object value, long timestamp) {
            Instant ts = Instant.ofEpochMilli(TimeUnit.MICROSECONDS.toMillis(timestamp));
            List<Column> columns = this.columnsByTime.computeIfAbsent(ts, k -> new ArrayList<>());
            columns.add(new Column(name, value));
        }

        private void addDeletedColumn(String name, long timestamp) {
            Instant ts = Instant.ofEpochMilli(TimeUnit.MICROSECONDS.toMillis(timestamp));
            List<String> columns = this.deletedColumnsByTime.computeIfAbsent(ts, k -> new ArrayList<>());
            columns.add(name);
        }

        public List<ChangeEvent> build(TableMetadata metadata, List<Column> partitionKeys) {
            List<ChangeEvent> events = new ArrayList<>();
            for (Map.Entry<Instant, Row> e : getUpdatedByTimestamp(partitionKeys).entrySet()) {
                events.add(new DefaultChangeEvent(metadata.keyspace,
                        metadata.name,
                        metadata.id.asUUID(),
                        e.getKey(),
                        e.getValue()));
            }
            for (Map.Entry<Instant, Deletion> e : getDeletionByTimestamp(partitionKeys).entrySet()) {
                events.add(new DefaultChangeEvent(metadata.keyspace,
                        metadata.name,
                        metadata.id.asUUID(),
                        e.getKey(),
                        e.getValue()));
            }
            // if no columns or deletions in this row, then clustering columns only change
            if (events.isEmpty()) {
                Instant ts = Instant.ofEpochMilli(TimeUnit.MICROSECONDS.toMillis(rowTimestamp));
                if (deletion) {
                    List<Criteria> criteria = new ArrayList<>();
                    for (Column c : partitionKeys) {
                        criteria.add(Criteria.equals(c.name, c.value));
                    }
                    for (Column c : clusteringColumns) {
                        criteria.add(Criteria.equals(c.name, c.value));
                    }
                    DeletionImpl deletion = new DeletionImpl();
                    deletion.criteria.addAll(criteria);
                    events.add(new DefaultChangeEvent(metadata.keyspace,
                            metadata.name,
                            metadata.id.asUUID(),
                            ts,
                            deletion));
                } else {
                    Map<String, Object> columns = new LinkedHashMap<>();
                    for (Column col : partitionKeys) {
                        columns.put(col.name, col.value);
                    }
                    for (Column col : clusteringColumns) {
                        columns.put(col.name, col.value);
                    }
                    events.add(new DefaultChangeEvent(metadata.keyspace,
                            metadata.name,
                            metadata.id.asUUID(),
                            ts,
                            () -> columns));
                }
            }

            Collections.sort(events);
            return events;
        }

        public Map<Instant, Deletion> getDeletionByTimestamp(List<Column> partitionKeys) {
            SortedMap<Instant, Deletion> map = new TreeMap<>();

            List<Criteria> criteria = new ArrayList<>();
            for (Column c : partitionKeys) {
                criteria.add(Criteria.equals(c.name, c.value));
            }
            for (Column c : clusteringColumns) {
                criteria.add(Criteria.equals(c.name, c.value));
            }
            for (Map.Entry<Instant, List<String>> c : this.deletedColumnsByTime.entrySet()) {
                DeletionImpl deletion = new DeletionImpl();
                deletion.criteria.addAll(criteria);
                deletion.columns.addAll(c.getValue());
                map.put(c.getKey(), deletion);
            }
            return map;
        }

        public Map<Instant, Row> getUpdatedByTimestamp(List<Column> partitionKeys) {
            SortedMap<Instant, Row> map = new TreeMap<>();

            List<Column> primaryKeys = new ArrayList<>();
            primaryKeys.addAll(partitionKeys);
            primaryKeys.addAll(clusteringColumns);
            for (Map.Entry<Instant, List<Column>> c : this.columnsByTime.entrySet()) {
                Map<String, Object> columns = new LinkedHashMap<>();
                for (Column col : primaryKeys) {
                    columns.put(col.name, col.value);
                }
                for (Column col : c.getValue()) {
                    columns.put(col.name, col.value);
                }
                map.put(c.getKey(), () -> columns);
            }
            return map;
        }
    }

    private static class DeletionImpl implements Deletion {
        private final List<String> columns = new ArrayList<>();
        private final List<Criteria> criteria = new ArrayList<>();

        void addColumn(String column) {
            this.columns.add(Objects.requireNonNull(column));
        }

        void addCriteria(Criteria criteria) {
            this.criteria.add(criteria);
        }

        @Override
        public List<String> getColumns() {
            return Collections.unmodifiableList(columns);
        }

        @Override
        public List<Criteria> getCriteria() {
            return Collections.unmodifiableList(criteria);
        }
    }

    private static class Column {
        private final String name;
        private final Object value;

        private Column(String name, Object value) {
            this.name = name;
            this.value = value;
        }
    }
}
