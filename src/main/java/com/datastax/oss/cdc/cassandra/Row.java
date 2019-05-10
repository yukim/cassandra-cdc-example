package com.datastax.oss.cdc.cassandra;

import javax.validation.constraints.NotNull;
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
    @NotNull
    Map<String, Object> getColumns();
}
