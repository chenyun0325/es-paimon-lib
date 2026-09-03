/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V.
 * Licensed under the Elastic License 2.0.
 */
package org.elasticsearch.paimon;

import org.elasticsearch.eslib.mount.ESIndexArchiveMetadata;
import org.elasticsearch.eslib.mount.LakeShardDescriptor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.zip.CRC32;

/** Compact index-setting representation of one mounted shard. Contains no credentials. */
final class ShardMountSpec {

    private static final int MAGIC = 0x504D5331; // PMS1
    private static final int VERSION = 1;
    private static final int MAX_FILES = 1_000_000;
    private static final int MAX_STRING_BYTES = 16 * 1024 * 1024;

    final String archiveLocation;
    final long archiveLength;
    final long rowRangeStart;
    final long rowRangeEnd;
    final long rowCount;
    final Map<String, long[]> fileOffsets;

    private ShardMountSpec(
            String archiveLocation,
            long archiveLength,
            long rowRangeStart,
            long rowRangeEnd,
            long rowCount,
            Map<String, long[]> fileOffsets) {
        this.archiveLocation = Objects.requireNonNull(archiveLocation, "archiveLocation");
        this.archiveLength = archiveLength;
        this.rowRangeStart = rowRangeStart;
        this.rowRangeEnd = rowRangeEnd;
        this.rowCount = rowCount;
        this.fileOffsets = fileOffsets;
    }

    static String encode(LakeShardDescriptor shard) throws IOException {
        ByteArrayOutputStream payloadBytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(payloadBytes)) {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            writeString(out, shard.archiveLocation());
            out.writeLong(shard.archiveLength());
            out.writeLong(shard.rowRangeStart());
            out.writeLong(shard.rowRangeEnd());
            out.writeLong(shard.rowCount());
            Map<String, ESIndexArchiveMetadata.FileRange> files = shard.metadata().files();
            out.writeInt(files.size());
            for (Map.Entry<String, ESIndexArchiveMetadata.FileRange> entry : files.entrySet()) {
                writeString(out, entry.getKey());
                out.writeLong(entry.getValue().offset());
                out.writeLong(entry.getValue().length());
            }
        }
        byte[] payload = payloadBytes.toByteArray();
        CRC32 crc = new CRC32();
        crc.update(payload);
        ByteArrayOutputStream encoded = new ByteArrayOutputStream(payload.length + Long.BYTES);
        encoded.write(payload);
        try (DataOutputStream out = new DataOutputStream(encoded)) {
            out.writeLong(crc.getValue());
        }
        return Base64.getEncoder().encodeToString(encoded.toByteArray());
    }

    static ShardMountSpec decode(String encoded) throws IOException {
        final byte[] allBytes;
        try {
            allBytes = Base64.getDecoder().decode(encoded);
        } catch (IllegalArgumentException e) {
            throw new IOException("Mounted shard setting is not valid Base64", e);
        }
        if (allBytes.length < Integer.BYTES * 2 + Long.BYTES) {
            throw new EOFException("Mounted shard setting is truncated");
        }

        int payloadLength = allBytes.length - Long.BYTES;
        DataInputStream checksumInput =
                new DataInputStream(new ByteArrayInputStream(allBytes, payloadLength, Long.BYTES));
        long expectedCrc = checksumInput.readLong();
        CRC32 crc = new CRC32();
        crc.update(allBytes, 0, payloadLength);
        if (crc.getValue() != expectedCrc) {
            throw new IOException("Mounted shard setting checksum mismatch");
        }

        DataInputStream in =
                new DataInputStream(new ByteArrayInputStream(allBytes, 0, payloadLength));
        if (in.readInt() != MAGIC) {
            throw new IOException("Invalid mounted shard setting magic");
        }
        int version = in.readInt();
        if (version != VERSION) {
            throw new IOException("Unsupported mounted shard setting version: " + version);
        }
        String location = readString(in);
        long archiveLength = in.readLong();
        long rangeStart = in.readLong();
        long rangeEnd = in.readLong();
        long rowCount = in.readLong();
        int fileCount = in.readInt();
        if (fileCount <= 0 || fileCount > MAX_FILES) {
            throw new IOException("Invalid mounted Lucene file count: " + fileCount);
        }
        Map<String, long[]> files = new LinkedHashMap<>();
        for (int i = 0; i < fileCount; i++) {
            String name = readString(in);
            long offset = in.readLong();
            long length = in.readLong();
            if (offset < 0
                    || length < 0
                    || offset > Long.MAX_VALUE - length
                    || offset + length > archiveLength) {
                throw new IOException("Invalid mounted archive range for Lucene file " + name);
            }
            if (files.put(name, new long[] {offset, length}) != null) {
                throw new IOException("Duplicate mounted Lucene file: " + name);
            }
        }
        if (in.available() != 0) {
            throw new IOException("Trailing mounted shard setting bytes: " + in.available());
        }
        long expectedRows;
        try {
            expectedRows = Math.addExact(Math.subtractExact(rangeEnd, rangeStart), 1L);
        } catch (ArithmeticException e) {
            throw new IOException("Mounted shard row range overflows", e);
        }
        if (rangeStart < 0 || rangeEnd < rangeStart || rowCount != expectedRows) {
            throw new IOException(
                    "Invalid mounted shard row range ["
                            + rangeStart
                            + ","
                            + rangeEnd
                            + "] for row count "
                            + rowCount);
        }
        return new ShardMountSpec(
                location,
                archiveLength,
                rangeStart,
                rangeEnd,
                rowCount,
                files);
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_STRING_BYTES || length > in.available()) {
            throw new EOFException("Invalid string length in mounted shard setting: " + length);
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
