package com.datastax.oss.cdc.cassandra;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;

/**
 * Converts {@link PartitionUpdate} into the list of {@link ChangeEvent}.
 *
 * This class first scans contents of {@link PartitionUpdate} and collects the changes by timestamp.
 * Then, it creates each {@link ChangeEvent} for each Row / RangeTombstone at the timestamp.
 */
public class PartitionParser {

    private final PartitionUpdate partition;
    private final ChangeEventBuilder changeEventBuilder;

    public PartitionParser(PartitionUpdate partition) {
        Objects.requireNonNull(partition);
        this.partition = partition;
        this.changeEventBuilder = new ChangeEventBuilder(partition.metadata());
    }

    public List<ChangeEvent> toChangeEvents() {

        // Partition keys
        ByteBuffer[] rawPartitionKeys = getComponents(partition.metadata(), partition.partitionKey());
        // partitionKeyColumns() returns in position order (but not explicitly stated in API doc)
        partition.metadata()
                .partitionKeyColumns()
                .forEach(def ->
                        changeEventBuilder.addPartitionKey(
                                def.name.toString(),
                                def.type.getSerializer().deserialize(rawPartitionKeys[def.position()])));

        // Check deletion info
        DeletionInfo deletionInfo = partition.deletionInfo();
        // Check if this is partition level deletion
        if (!deletionInfo.getPartitionDeletion().isLive()) {
            changeEventBuilder.partitionIsDeletedAt(deletionInfo.getPartitionDeletion().markedForDeleteAt());
        }
        // Range tombstones
        if (deletionInfo.hasRanges()) {
            Iterator<RangeTombstone> tombstones = deletionInfo.rangeIterator(false);
            while (tombstones.hasNext()) {
                changeEventBuilder.addRangeTombstone(tombstones.next());
            }
        }

        // static columns
        Row staticRow = partition.staticRow();
        if (!staticRow.isEmpty()) {
            changeEventBuilder.addStatic();
            for (ColumnData cd : staticRow) {
                visitColumn(cd);
            }
        }
        // rows
        if (partition.hasRows()) {
            for (Row row : partition) {
                // new row with primary Key Liveness info
                changeEventBuilder.newRow(row.primaryKeyLivenessInfo().timestamp());

                // row deletion
                if (!row.deletion().isLive()) {
                    changeEventBuilder.markDeletedAt(row.deletion().time().markedForDeleteAt());
                }

                // clustering columns
                int i = 0;
                for (ColumnMetadata def : partition.metadata().clusteringColumns()) {
                    changeEventBuilder.addClusteringColumn(
                            def.toString(),
                            def.type.getSerializer().deserialize(row.clustering().get(i)));
                    i++;
                }
                for (ColumnData cd : row) {
                    visitColumn(cd);
                }
            }
        }

        return changeEventBuilder.build(partition.metadata());
    }

    private void visitCell(Cell cell) {
        ColumnMetadata col = cell.column();
        if (cell.isTombstone()) {
            changeEventBuilder.addDeletedColumn(col.name.toString(),
                    cell.timestamp());
        } else {
            changeEventBuilder.addColumn(col.name.toString(),
                    col.type.getSerializer().deserialize(cell.value()),
                    cell.timestamp());
        }
    }

    private void visitColumn(ColumnData cd) {
        if (cd.column().isSimple()) {
            visitCell((Cell) cd);
        } else {
            // Complex deletion is added when the column is deleted
            ComplexColumnData complexData = (ComplexColumnData) cd;
            if (!complexData.complexDeletion().isLive()) {
                changeEventBuilder.addDeletedColumn(cd.column().name.toString(),
                        complexData.complexDeletion().markedForDeleteAt());
            }

            Function<Cell, Object> keyMapper = cell -> cell.column().name.toString();
            Function<Cell, Object> valueMapper = cell -> cell.column().type.getSerializer().deserialize(cell.value());
            if (cd.column().type.isCollection()) {
                CollectionType ct = (CollectionType) cd.column().type;
                keyMapper = cell -> ct.nameComparator().getSerializer().deserialize(cell.path().get(0));
                valueMapper = cell -> ct.valueComparator().getSerializer().deserialize(cell.value());
            } else if (cd.column().type.isUDT()) {
                UserType ut = (UserType) cd.column().type;
                keyMapper = cell -> {
                    Short fId = ut.nameComparator().getSerializer().deserialize(cell.path().get(0));
                    return ut.fieldNameAsString(fId);
                };
                valueMapper = cell -> {
                    Short fId = ut.nameComparator().getSerializer().deserialize(cell.path().get(0));
                    return ut.fieldType(fId).getSerializer().deserialize(cell.value());
                };
            }
            Map m = new HashMap();
            for (Cell c : complexData) {
                m.put(keyMapper.apply(c), valueMapper.apply(c));
            }
            // TODO timestamp
            if (!m.isEmpty()) {
                changeEventBuilder.addColumn(complexData.column().name.toString(),
                        m,
                        complexData.maxTimestamp());
            }
        }
    }

    private static ByteBuffer[] getComponents(TableMetadata metadata, DecoratedKey partitionKey) {
        ByteBuffer key = partitionKey.getKey();
        if (metadata.partitionKeyType instanceof CompositeType) {
            return ((CompositeType) metadata.partitionKeyType).split(key);
        } else {
            return new ByteBuffer[]{ key };
        }
    }
}
