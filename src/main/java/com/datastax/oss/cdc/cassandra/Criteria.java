package com.datastax.oss.cdc.cassandra;

import java.util.Objects;

/**
 * Criteria of change for certain column.
 *
 * This object can represents equality of the column (col = 'val'),
 * but also the range of values for the column. (val1 < col <= val2).
 */
public class Criteria {
    private final String column;

    private final Object startValue;
    private final Object endValue;

    private final boolean startInclusive;
    private final boolean endInclusive;

    /**
     * Creates 'equal' criteria that represents condition <code>'columnName' = value</code>
     *
     * @param columnName name of the column
     * @param value
     * @return
     */
    public static Criteria equals(String columnName, Object value) {
        return new Criteria(columnName, value, value, true, true);
    }

    public static Criteria range(String columnName, Object start, Object end, boolean startInclusive, boolean endInclusive) {
        return new Criteria(columnName, start, end, startInclusive, endInclusive);
    }

    private Criteria(String column, Object startValue, Object endValue, boolean startInclusive, boolean endInclusive) {
        this.column = column;
        this.startValue = startValue;
        this.endValue = endValue;
        this.startInclusive = startInclusive;
        this.endInclusive = endInclusive;
    }

    public String getColumn() {
        return column;
    }

    public Object getCondition() {
        if (isEqual()) {
            return startValue;
        } else {
            // TODO make it Range object
            StringBuilder sb = new StringBuilder();
            if (startInclusive) {
                sb.append("[");
            } else {
                sb.append("(");
            }
            if (startValue != null) {
                sb.append(startValue.toString());
            }
            sb.append(", ");
            if (endValue != null) {
                sb.append(endValue.toString());
            }
            if (endInclusive) {
                sb.append("]");
            } else {
                sb.append(")");
            }
            return sb.toString();
        }
    }

    public boolean isEqual() {
        return Objects.equals(startValue, endValue);
    }
}
