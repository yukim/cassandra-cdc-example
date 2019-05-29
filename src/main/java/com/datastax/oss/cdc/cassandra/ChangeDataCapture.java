package com.datastax.oss.cdc.cassandra;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogReader;
import org.apache.cassandra.schema.Schema;

import java.io.IOException;
import java.nio.file.*;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

public class ChangeDataCapture {

    private final CommitLogReader reader = new CommitLogReader();
    private final CommitLogHandler handler = new CommitLogHandler();

    public void start(Path cdcDirectory) throws InterruptedException, IOException {
        WatchService watchService = cdcDirectory.getFileSystem().newWatchService();
        WatchKey key = cdcDirectory.register(watchService, ENTRY_CREATE);

        while (true) {
            WatchKey watchKey = watchService.take();
            for (WatchEvent<?> event : watchKey.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();
                if (!kind.equals(ENTRY_CREATE)) {
                    continue;
                }

                WatchEvent<Path> ev = (WatchEvent<Path>) event;
                Path relativePath = ev.context();
                Path absolutePath = cdcDirectory.resolve(relativePath);
                read(absolutePath);
                Files.delete(absolutePath);
            }
            key.reset();
        }
    }

    public void read(Path absolutePath) throws IOException {
        // the last arg tolerateTruncation is false because Cassandra has a bug that can cause infinite loop
        // when ignoring exception
        try {
            reader.readCommitLogSegment(handler, absolutePath.toFile(), false);
        } finally {
            // TODO how to display invalid mutations
            if (!reader.getInvalidMutations().isEmpty()) {
                System.err.println(reader.getInvalidMutations());
            }
        }
    }

    public static void main(String[] args) throws Exception {

        // Initialize for Apache Cassandra classes
        DatabaseDescriptor.toolInitialization();

        // Check if CDC is enabled
        // Exit with -1 if not
        if (!DatabaseDescriptor.isCDCEnabled()) {
            System.err.println("CDC is not enabled for this node. Enable CDC by editing cassandra.yaml and restarting the node.");
            System.exit(-1);
        }

        // Load schema from disk without updating schema version
        Schema.instance.loadFromDisk(false);

        // Check CDC location
        Path cdcLocation;
        // Use the location if it is passed to the program
        // if not specified, take the CDC location from cassandra.yaml
        if (args.length > 0) {
            cdcLocation = Paths.get(args[0]);
        } else {
            cdcLocation = Paths.get(DatabaseDescriptor.getCDCLogLocation());
        }
        if (Files.notExists(cdcLocation)) {
            String message = String.format("CDC log location %s not found.", cdcLocation);
            System.err.println(message);
            System.exit(-1);
        }
        ChangeDataCapture cdc = new ChangeDataCapture();
        if (Files.isDirectory(cdcLocation)) {
            // Start watching
            cdc.start(cdcLocation);
        } else {
            // Process file
            cdc.read(cdcLocation);
        }
    }
}
