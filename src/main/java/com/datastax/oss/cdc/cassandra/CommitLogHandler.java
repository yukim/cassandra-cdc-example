package com.datastax.oss.cdc.cassandra;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;

public class CommitLogHandler implements CommitLogReadHandler {
    @Override
    public void handleMutation(Mutation m, int size, int entryLocation, CommitLogDescriptor desc) {
        for (PartitionUpdate partition : m.getPartitionUpdates()) {
            PartitionParser p = new PartitionParser(partition);
            List<ChangeEvent> events = p.toChangeEvents();
            events.stream().map(JsonOutput::toJson).forEach(System.out::println);
        }
    }

    public boolean shouldSkipSegmentOnError(CommitLogReadException e) throws IOException {
        return false;
    }

    public void handleUnrecoverableError(CommitLogReadException e) throws IOException {
        throw e;
    }
}
