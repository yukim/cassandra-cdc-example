package com.datastax.oss.cdc.cassandra;

import java.util.Map;

/**
 * Object that contains
 */
public interface Row {

    /**
     * Get all columns in this Row
     *
     * @return
     */
    Map<String, Object> getColumns();
}
