/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package org.elasticsearch.paimon;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Snapshot-pinned random-access reader used to hydrate Elasticsearch search hits. */
final class PaimonRowLoader implements Closeable {

    private final Settings nodeSettings;
    private final SecureString ossAccessKeySecret;
    private final Environment environment;
    private final Map<TableKey, CachedTable> tables = new HashMap<>();
    private boolean closed;

    PaimonRowLoader(
            Settings nodeSettings,
            SecureString ossAccessKeySecret,
            Environment environment) {
        this.nodeSettings = nodeSettings;
        this.ossAccessKeySecret = ossAccessKeySecret;
        this.environment = environment;
    }

    Map<String, Object> load(
            String tablePath, long snapshotId, long rowId, List<String> returnFields)
            throws IOException {
        CachedTable cached = table(tablePath, snapshotId);
        RowType rowType = cached.table.rowType();
        validateReturnFields(rowType, returnFields);

        ReadBuilder readBuilder =
                cached.table
                        .newReadBuilder()
                        .withReadType(rowType)
                        .withRowRanges(List.of(new Range(rowId, rowId)))
                        .withLimit(2);
        Map<String, Object> result = null;
        int records = 0;
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                try {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        records++;
                        if (records > 1) {
                            throw new IOException(
                                    "Paimon Row-ID "
                                            + rowId
                                            + " returned multiple rows from snapshot "
                                            + snapshotId
                                            + " at "
                                            + tablePath);
                        }
                        result = PaimonJsonRowConverter.convert(row, rowType, returnFields);
                    }
                } finally {
                    batch.releaseBatch();
                }
            }
        } catch (RuntimeException e) {
            throw new IOException(
                    "Failed to read Paimon Row-ID "
                            + rowId
                            + " from snapshot "
                            + snapshotId
                            + " at "
                            + tablePath,
                    e);
        }
        if (records != 1) {
            throw new IOException(
                    "Paimon Row-ID "
                            + rowId
                            + " was not found in snapshot "
                            + snapshotId
                            + " at "
                            + tablePath);
        }
        return result;
    }

    private synchronized CachedTable table(String tablePath, long snapshotId) throws IOException {
        if (closed) {
            throw new IOException("Paimon row loader is closed");
        }
        TableKey key = new TableKey(tablePath, snapshotId);
        CachedTable existing = tables.get(key);
        if (existing != null) {
            return existing;
        }

        PaimonSnapshotPlanner.TableSession session =
                new PaimonSnapshotPlanner(nodeSettings, ossAccessKeySecret, environment)
                        .openTable(tablePath);
        try {
            FileStoreTable baseTable = session.table();
            Snapshot snapshot = baseTable.snapshotManager().snapshot(snapshotId);
            if (snapshot == null) {
                throw new IOException(
                        "Paimon snapshot " + snapshotId + " no longer exists at " + tablePath);
            }
            FileStoreTable snapshotTable =
                    baseTable.copy(
                            Map.of(
                                    CoreOptions.SCAN_SNAPSHOT_ID.key(),
                                    Long.toString(snapshotId)));
            CachedTable created = new CachedTable(session, snapshotTable);
            tables.put(key, created);
            return created;
        } catch (Exception e) {
            try {
                session.close();
            } catch (IOException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            if (e instanceof IOException ioException) {
                throw ioException;
            }
            throw new IOException(
                    "Failed to pin Paimon snapshot " + snapshotId + " at " + tablePath, e);
        }
    }

    private static void validateReturnFields(RowType rowType, List<String> returnFields)
            throws IOException {
        for (String field : returnFields) {
            if (rowType.containsField(field) == false) {
                throw new IOException(
                        "Paimon return field ["
                                + field
                                + "] does not exist in snapshot schema; available fields="
                                + rowType.getFieldNames());
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOException failure = null;
        for (CachedTable table : tables.values()) {
            try {
                table.session.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        tables.clear();
        if (failure != null) {
            throw failure;
        }
    }

    private record TableKey(String tablePath, long snapshotId) {}

    private static final class CachedTable {
        private final PaimonSnapshotPlanner.TableSession session;
        private final FileStoreTable table;

        private CachedTable(PaimonSnapshotPlanner.TableSession session, FileStoreTable table) {
            this.session = session;
            this.table = table;
        }
    }
}
