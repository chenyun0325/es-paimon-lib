/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V.
 * Licensed under the Elastic License 2.0.
 */
package org.elasticsearch.eslib.mount;

import java.io.IOException;
import java.util.Objects;

/** A Paimon Global Index file and the inclusive table row-id range represented by it. */
public final class LakeShardDescriptor {

    private final String archiveLocation;
    private final long archiveLength;
    private final long rowRangeStart;
    private final long rowRangeEnd;
    private final long rowCount;
    private final ESIndexArchiveMetadata metadata;

    public LakeShardDescriptor(
            String archiveLocation,
            long archiveLength,
            long rowRangeStart,
            long rowRangeEnd,
            long rowCount,
            byte[] indexMetadata)
            throws IOException {
        this(
                archiveLocation,
                archiveLength,
                rowRangeStart,
                rowRangeEnd,
                rowCount,
                ESIndexArchiveMetadata.parse(indexMetadata));
    }

    public LakeShardDescriptor(
            String archiveLocation,
            long archiveLength,
            long rowRangeStart,
            long rowRangeEnd,
            long rowCount,
            ESIndexArchiveMetadata metadata)
            throws IOException {
        if (archiveLocation == null || archiveLocation.trim().isEmpty()) {
            throw new IllegalArgumentException("archiveLocation must not be empty");
        }
        if (rowRangeStart < 0 || rowRangeEnd < rowRangeStart) {
            throw new IllegalArgumentException(
                    "Invalid inclusive Paimon row range ["
                            + rowRangeStart
                            + ","
                            + rowRangeEnd
                            + "]");
        }
        long expectedRowCount;
        try {
            expectedRowCount = Math.addExact(Math.subtractExact(rowRangeEnd, rowRangeStart), 1L);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Paimon row range length overflow", e);
        }
        if (rowCount != expectedRowCount) {
            throw new IllegalArgumentException(
                    "rowCount "
                            + rowCount
                            + " does not match inclusive Paimon row range length "
                            + expectedRowCount);
        }
        if (rowCount > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "A mounted Lucene shard cannot contain more than Integer.MAX_VALUE rows: "
                            + rowCount);
        }
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        metadata.validateArchiveLength(archiveLength);
        this.archiveLocation = archiveLocation;
        this.archiveLength = archiveLength;
        this.rowRangeStart = rowRangeStart;
        this.rowRangeEnd = rowRangeEnd;
        this.rowCount = rowCount;
    }

    public String archiveLocation() {
        return archiveLocation;
    }

    public long archiveLength() {
        return archiveLength;
    }

    public long rowRangeStart() {
        return rowRangeStart;
    }

    public long rowRangeEnd() {
        return rowRangeEnd;
    }

    public long rowCount() {
        return rowCount;
    }

    public ESIndexArchiveMetadata metadata() {
        return metadata;
    }

    /** Convert a shard-local Lucene doc id to the absolute Paimon row id. */
    public long globalRowId(int luceneDocId) {
        if (luceneDocId < 0 || luceneDocId >= rowCount) {
            throw new IllegalArgumentException(
                    "Lucene doc id " + luceneDocId + " is outside [0," + rowCount + ")");
        }
        return rowRangeStart + luceneDocId;
    }
}
