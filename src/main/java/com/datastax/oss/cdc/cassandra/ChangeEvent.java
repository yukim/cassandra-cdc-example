package com.datastax.oss.cdc.cassandra;

import java.time.Instant;
import java.util.UUID;

/**
 * This interface represents a change made to certain row or deletion at certain timestamp.
 */
public interface ChangeEvent extends Comparable<ChangeEvent> {

    @Override
    default int compareTo(ChangeEvent o) {
        return getEventTimestamp().compareTo(o.getEventTimestamp());
    }

    /**
     * Returns timestamp associated with this event.
     *
     * Note that in Apache Cassandra, user can supply timestamp with query.
     *
     * @return timestamp of this event
     */
    Instant getEventTimestamp();

    /**
     * Returns the name of keyspace where this event happened
     *
     * @return Name of keyspace
     */
    String getKeyspaceName();

    /**
     * Returns the name of the table this event happened
     *
     * @return Name of the table this change is made
     */
    String getTableName();

    /**
     * Returns the id of the table this event happened
     *
     * @return Table ID
     */
    UUID getTableId();

    /**
     * Returns the type of this event
     *
     * @return Change event type: UPDATE or DELETE
     */
    ChangeEventType getEventType();

    /**
     * Returns the deletion criteria if this event is for deletion.
     *
     * @return Deletion object when this event is {@link ChangeEventType#DELETE}, else null
     */
    Deletion getDeletion();

    /**
     * Returns the row objects that contains columns that is inserted or updated if this event is for update.
     *
     * @return Row object containing changes when this event is {@link ChangeEventType#UPDATE}, else null
     */
    Row getRow();
}
