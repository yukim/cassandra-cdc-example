package com.datastax.oss.cdc.cassandra;

import java.util.List;

/**
 * This interface describes the range of CQL rows by provided
 */
public interface Deletion {

    /**
     * When deleting column, this methods returns the names of columns.
     *
     * Each column name is in the following format:
     *
     * <ul>
     *     <li>column name - <code>column_name</code></li>
     *     <li>column name with key (i.e. map type) - <code>column_name '[' term ']'</code></li>
     *     <li>column name with UDT field - <code>column_name '.' `field_name</code></li>
     * </ul>
     *
     * @return list of deleted columns, or empty list when not specified
     */
    List<String> getColumns();

    /**
     *
     * @return
     */
    List<Criteria> getCriteria();
}
