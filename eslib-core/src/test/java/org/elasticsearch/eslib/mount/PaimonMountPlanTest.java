/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V.
 * Licensed under the Elastic License 2.0.
 */
package org.elasticsearch.eslib.mount;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PaimonMountPlanTest {

    @Test
    void assignsShardNumbersByInclusiveRowRange() throws Exception {
        ESIndexArchiveMetadata metadata =
                ESIndexArchiveMetadata.parse(ESIndexArchiveMetadataTest.vectorMetadata("v"));
        LakeShardDescriptor second = shard("file:///lake/es-2", 10, 19, metadata);
        LakeShardDescriptor first = shard("file:///lake/es-1", 0, 9, metadata);

        PaimonMountPlan plan = PaimonMountPlan.create(42L, List.of(second, first));

        assertEquals(2, plan.numberOfShards());
        assertEquals("file:///lake/es-1", plan.shard(0).descriptor().archiveLocation());
        assertEquals(0L, plan.shard(0).globalRowId(0));
        assertEquals(9L, plan.shard(0).globalRowId(9));
        assertEquals(10L, plan.shard(1).globalRowId(0));
        assertEquals(19L, plan.shard(1).globalRowId(9));
    }

    @Test
    void rejectsOverlappingLakeRanges() throws Exception {
        ESIndexArchiveMetadata metadata =
                ESIndexArchiveMetadata.parse(ESIndexArchiveMetadataTest.vectorMetadata("v"));
        assertThrows(
                IOException.class,
                () ->
                        PaimonMountPlan.create(
                                1L,
                                List.of(
                                        shard("file:///lake/a", 0, 9, metadata),
                                        shard("file:///lake/b", 9, 18, metadata))));
    }

    private static LakeShardDescriptor shard(
            String location, long start, long end, ESIndexArchiveMetadata metadata)
            throws IOException {
        return new LakeShardDescriptor(
                location, 260L, start, end, end - start + 1L, metadata);
    }
}
