/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V.
 * Licensed under the Elastic License 2.0.
 */
package org.elasticsearch.paimon;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.eslib.api.ArchiveDataProvider;
import org.elasticsearch.eslib.mount.LakeShardDescriptor;
import org.elasticsearch.env.Environment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ShardMountSpecTest {

    @Test
    void roundTripsAndOpensNativeLocalPath(@TempDir Path tempDir) throws Exception {
        byte[] archive = new byte[] {3, 1, 4};
        Path archivePath = tempDir.resolve("shard.index");
        Files.write(archivePath, archive);
        LakeShardDescriptor descriptor =
                new LakeShardDescriptor(
                        archivePath.toString(),
                        archive.length,
                        100,
                        100,
                        1,
                        legacyMetadata("segments_1", 0, archive.length));

        ShardMountSpec decoded = ShardMountSpec.decode(ShardMountSpec.encode(descriptor));

        assertEquals(archivePath.toString(), decoded.archiveLocation);
        assertEquals(100, decoded.rowRangeStart);
        assertArrayEquals(new long[] {0, archive.length}, decoded.fileOffsets.get("segments_1"));
        Settings nodeSettings =
                Settings.builder()
                        .put("path.home", tempDir.resolve("es-home"))
                        .putList("path.repo", tempDir.toString())
                        .build();
        try (ArchiveDataProvider provider =
                MountArchiveProviderFactory.open(
                        decoded, nodeSettings, null, new Environment(nodeSettings, null))) {
            assertArrayEquals(archive, provider.readRange(0, archive.length));
        }
    }

    @Test
    void rejectsTamperedClusterStateDescriptor(@TempDir Path tempDir) throws Exception {
        Path archivePath = tempDir.resolve("shard.index");
        Files.write(archivePath, new byte[] {7});
        LakeShardDescriptor descriptor =
                new LakeShardDescriptor(
                        archivePath.toString(),
                        1,
                        0,
                        0,
                        1,
                        legacyMetadata("segments_1", 0, 1));
        byte[] bytes = Base64.getDecoder().decode(ShardMountSpec.encode(descriptor));
        bytes[bytes.length / 2] ^= 1;

        assertThrows(
                IOException.class,
                () -> ShardMountSpec.decode(Base64.getEncoder().encodeToString(bytes)));
    }

    @Test
    void rendersReadableMountStatusWithOptionalFileOffsets(@TempDir Path tempDir)
            throws Exception {
        Path archivePath = tempDir.resolve("shard.index");
        ShardMountSpec spec =
                ShardMountSpec.decode(
                        ShardMountSpec.encode(
                                new LakeShardDescriptor(
                                        archivePath.toString(),
                                        7,
                                        100,
                                        102,
                                        3,
                                        legacyMetadata("_0.si", 2, 5))));

        Map<String, Object> detailed =
                RestPaimonMountStatusAction.shardView(4, spec, true);
        assertEquals(4, detailed.get("shard"));
        assertEquals(100L, detailed.get("row_range_start"));
        assertEquals(102L, detailed.get("row_range_end"));
        assertEquals(1, detailed.get("file_count"));
        assertEquals(
                List.of(Map.of("name", "_0.si", "offset", 2L, "length", 5L)),
                detailed.get("files"));

        Map<String, Object> summary =
                RestPaimonMountStatusAction.shardView(4, spec, false);
        assertFalse(summary.containsKey("files"));
    }

    private static byte[] legacyMetadata(String name, long offset, long length)
            throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
            out.writeInt(1);
            out.writeInt(nameBytes.length);
            out.write(nameBytes);
            out.writeLong(offset);
            out.writeLong(length);
        }
        return bytes.toByteArray();
    }
}
