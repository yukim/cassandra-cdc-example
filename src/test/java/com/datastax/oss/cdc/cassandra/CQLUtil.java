package com.datastax.oss.cdc.cassandra;

import io.reactivex.Single;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.service.QueryState;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public final class CQLUtil {

    /**
     * Convert DML and batch statement to Mutation object.
     * CQL is parsed using QueryProcessor with default query options.
     *
     * @param cql CQL to parse
     * @param client Client state with keyspace set
     * @param timestamp Query execution timestamp in milliseconds
     * @return Converted Mutations
     */
    public static Collection<Mutation> toMutation(String cql, QueryState client, long timestamp) {
        long timestampInMicro = TimeUnit.MILLISECONDS.toMicros(timestamp);
        long timestampInNano = TimeUnit.MILLISECONDS.toNanos(timestamp);

        ParsedStatement.Prepared stmt = QueryProcessor.getStatement(cql, client);
        Method toMutation;
        Object queryOptions;
        try {
            if (stmt.statement instanceof BatchStatement) {
                toMutation = BatchStatement.class.getDeclaredMethod("getMutations", BatchQueryOptions.class, boolean.class, long.class, long.class);
                queryOptions = BatchQueryOptions.DEFAULT;
            } else if (stmt.statement instanceof ModificationStatement) {
                toMutation = ModificationStatement.class.getDeclaredMethod("getMutations", QueryOptions.class, boolean.class, long.class, long.class);
                queryOptions = QueryOptions.DEFAULT;
            } else {
                throw new UnsupportedOperationException("CQL not supported: " + cql);
            }
            toMutation.setAccessible(true);
            return ((Single<Collection<Mutation>>) toMutation.invoke(stmt.statement, queryOptions, true, timestampInMicro, timestampInNano)).blockingGet();
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
