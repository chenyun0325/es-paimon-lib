/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V.
 * Licensed under the Elastic License 2.0.
 */
package org.elasticsearch.eslib.mount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Immutable, validated mapping from one Paimon snapshot's global-index files to Elasticsearch
 * shard numbers.
 */
public final class PaimonMountPlan {

    private final long snapshotId;
    private final List<MountedShard> shards;

    private PaimonMountPlan(long snapshotId, List<MountedShard> shards) {
        this.snapshotId = snapshotId;
        this.shards = Collections.unmodifiableList(shards);
    }

    public static PaimonMountPlan create(long snapshotId, List<LakeShardDescriptor> descriptors)
            throws IOException {
        if (snapshotId < 0) {
            throw new IllegalArgumentException("snapshotId must not be negative");
        }
        Objects.requireNonNull(descriptors, "descriptors");
        if (descriptors.isEmpty()) {
            throw new IOException("Snapshot " + snapshotId + " contains no es-index shards");
        }

        List<LakeShardDescriptor> sorted = new ArrayList<>(descriptors);
        sorted.sort(
                Comparator.comparingLong(LakeShardDescriptor::rowRangeStart)
                        .thenComparing(LakeShardDescriptor::archiveLocation));

        ESIndexArchiveMetadata fieldLayout = sorted.get(0).metadata();
        Set<String> locations = new HashSet<>();
        List<MountedShard> mounted = new ArrayList<>(sorted.size());
        LakeShardDescriptor previous = null;
        for (int shardId = 0; shardId < sorted.size(); shardId++) {
            LakeShardDescriptor descriptor = sorted.get(shardId);
            if (locations.add(descriptor.archiveLocation()) == false) {
                throw new IOException(
                        "Duplicate es-index archive in snapshot "
                                + snapshotId
                                + ": "
                                + descriptor.archiveLocation());
            }
            if (previous != null && descriptor.rowRangeStart() <= previous.rowRangeEnd()) {
                throw new IOException(
                        "Overlapping Paimon row ranges: ["
                                + previous.rowRangeStart()
                                + ","
                                + previous.rowRangeEnd()
                                + "] and ["
                                + descriptor.rowRangeStart()
                                + ","
                                + descriptor.rowRangeEnd()
                                + "]");
            }
            if (fieldLayout.hasSameFieldLayout(descriptor.metadata()) == false) {
                throw new IOException(
                        "Inconsistent es-index field layout in archive "
                                + descriptor.archiveLocation());
            }
            mounted.add(new MountedShard(shardId, descriptor));
            previous = descriptor;
        }
        return new PaimonMountPlan(snapshotId, mounted);
    }

    public long snapshotId() {
        return snapshotId;
    }

    public int numberOfShards() {
        return shards.size();
    }

    public List<MountedShard> shards() {
        return shards;
    }

    public MountedShard shard(int shardId) {
        if (shardId < 0 || shardId >= shards.size()) {
            throw new IllegalArgumentException(
                    "Elasticsearch shard id " + shardId + " is outside [0," + shards.size() + ")");
        }
        return shards.get(shardId);
    }

    public ESIndexArchiveMetadata fieldLayout() {
        return shards.get(0).descriptor.metadata();
    }

    /** One stable Elasticsearch shard assignment in this plan. */
    public static final class MountedShard {
        private final int shardId;
        private final LakeShardDescriptor descriptor;

        private MountedShard(int shardId, LakeShardDescriptor descriptor) {
            this.shardId = shardId;
            this.descriptor = descriptor;
        }

        public int shardId() {
            return shardId;
        }

        public LakeShardDescriptor descriptor() {
            return descriptor;
        }

        public long globalRowId(int luceneDocId) {
            return descriptor.globalRowId(luceneDocId);
        }
    }
}
