package com.datastax.oss.cdc.cassandra;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

class DefaultChangeEvent implements ChangeEvent {
    private final String keyspace;
    private final String table;
    private final UUID tableId;
    private final Instant timestamp;
    private final ChangeEventType eventType;
    private final Row row;
    private final Deletion deletion;

    DefaultChangeEvent(String keyspace,
                       String table,
                       UUID tableId,
                       Instant timestamp,
                       Deletion deletion) {
        this.keyspace = keyspace;
        this.table = table;
        this.tableId = tableId;
        this.timestamp = timestamp;
        this.eventType = ChangeEventType.DELETE;
        this.deletion = Objects.requireNonNull(deletion);
        this.row = null;
    }

    DefaultChangeEvent(String keyspace,
                       String table,
                       UUID tableId,
                       Instant timestamp,
                       Row row) {
        this.keyspace = keyspace;
        this.table = table;
        this.tableId = tableId;
        this.timestamp = timestamp;
        this.eventType = ChangeEventType.UPDATE;
        this.row = Objects.requireNonNull(row);
        this.deletion = null;
    }

    @Override
    public Instant getEventTimestamp() {
        return timestamp;
    }

    @Override
    public String getKeyspaceName() {
        return keyspace;
    }

    @Override
    public String getTableName() {
        return table;
    }

    @Override
    public UUID getTableId() {
        return tableId;
    }

    @Override
    public ChangeEventType getEventType() {
        return eventType;
    }

    @Override
    public Deletion getDeletion() {
        return deletion;
    }

    @Override
    public Row getRow() {
        return row;
    }
}
