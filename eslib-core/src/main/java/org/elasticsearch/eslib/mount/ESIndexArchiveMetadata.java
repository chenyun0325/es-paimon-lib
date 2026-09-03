/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.elasticsearch.eslib.mount;

import org.elasticsearch.eslib.api.model.BuiltinAnalyzer;
import org.elasticsearch.eslib.api.model.FieldIndexConfig;
import org.elasticsearch.eslib.api.model.ScalarFieldType;
import org.elasticsearch.eslib.api.model.VectorAlgorithm;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Public reader for the metadata stored in a Paimon {@code es-index} global-index entry.
 *
 * <p>The archive itself is a concatenation of Lucene files. This metadata maps every logical
 * Lucene file to its byte range in the archive and, for versioned metadata, carries the field
 * configuration required to construct an Elasticsearch mapping.
 */
public final class ESIndexArchiveMetadata {

    /** {@code ESM1}. */
    public static final int MAGIC = 0x45534D31;
    public static final int CONFIG_ONLY_VERSION = 1;
    public static final int CURRENT_VERSION = 2;

    private static final int MAX_ENTRY_COUNT = 1_000_000;
    private static final int MAX_STRING_BYTES = 16 * 1024 * 1024;

    private final int version;
    private final List<String> indexedFieldNames;
    private final List<String> indexedFieldTypes;
    private final Map<String, FieldIndexConfig> fieldConfigs;
    private final Map<String, FileRange> files;

    private ESIndexArchiveMetadata(
            int version,
            List<String> indexedFieldNames,
            List<String> indexedFieldTypes,
            Map<String, FieldIndexConfig> fieldConfigs,
            Map<String, FileRange> files) {
        this.version = version;
        this.indexedFieldNames = Collections.unmodifiableList(indexedFieldNames);
        this.indexedFieldTypes = Collections.unmodifiableList(indexedFieldTypes);
        this.fieldConfigs = Collections.unmodifiableMap(fieldConfigs);
        this.files = Collections.unmodifiableMap(files);
    }

    /** Parse current, version-1, or legacy offset-only metadata. */
    public static ESIndexArchiveMetadata parse(byte[] bytes) throws IOException {
        Objects.requireNonNull(bytes, "metadata bytes");
        if (bytes.length < Integer.BYTES) {
            throw new EOFException("es-index metadata is empty or truncated");
        }

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
        int first = in.readInt();
        if (first != MAGIC) {
            Map<String, FileRange> files = readFileRanges(in, first);
            requireFullyConsumed(in);
            return new ESIndexArchiveMetadata(
                    0,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyMap(),
                    files);
        }

        int version = in.readInt();
        if (version != CONFIG_ONLY_VERSION && version != CURRENT_VERSION) {
            throw new IOException("Unsupported es-index metadata version: " + version);
        }

        List<String> fieldNames = new ArrayList<>();
        List<String> fieldTypes = new ArrayList<>();
        if (version >= CURRENT_VERSION) {
            int fieldCount = readCount(in, "indexed field");
            for (int i = 0; i < fieldCount; i++) {
                String name = readString(in);
                if (fieldNames.contains(name)) {
                    throw new IOException("Duplicate indexed field in es-index metadata: " + name);
                }
                fieldNames.add(name);
                fieldTypes.add(readString(in));
            }
        }

        int configCount = readCount(in, "field config");
        Map<String, FieldIndexConfig> configs = new LinkedHashMap<>();
        for (int i = 0; i < configCount; i++) {
            String fieldName = readString(in);
            FieldIndexConfig.IndexType indexType =
                    parseEnum(FieldIndexConfig.IndexType.class, readString(in), "index type");
            VectorAlgorithm algorithm = readNullableEnum(in, VectorAlgorithm.class, "algorithm");
            int dimension = in.readInt();
            String metric = readNullableString(in);
            BuiltinAnalyzer analyzer = readNullableEnum(in, BuiltinAnalyzer.class, "analyzer");
            ScalarFieldType scalarType =
                    readNullableEnum(in, ScalarFieldType.class, "scalar type");

            int parameterCount = readCount(in, "algorithm parameter");
            Map<String, String> parameters = new LinkedHashMap<>();
            for (int j = 0; j < parameterCount; j++) {
                String parameterName = readString(in);
                String parameterValue = readString(in);
                if (parameters.put(parameterName, parameterValue) != null) {
                    throw new IOException(
                            "Duplicate algorithm parameter for field '"
                                    + fieldName
                                    + "': "
                                    + parameterName);
                }
            }

            FieldIndexConfig config;
            try {
                config =
                        FieldIndexConfig.builder(fieldName, indexType)
                                .algorithm(algorithm)
                                .dimension(dimension)
                                .metric(metric)
                                .analyzer(analyzer)
                                .scalarType(scalarType)
                                .algorithmParams(parameters)
                                .build();
            } catch (IllegalArgumentException | NullPointerException e) {
                throw new IOException(
                        "Invalid field config in es-index metadata for field '" + fieldName + "'",
                        e);
            }
            if (configs.put(fieldName, config) != null) {
                throw new IOException("Duplicate field config in es-index metadata: " + fieldName);
            }
        }

        Map<String, FileRange> files = readFileRanges(in, readCount(in, "file"));
        requireFullyConsumed(in);

        if (version == CONFIG_ONLY_VERSION) {
            fieldNames = inferVersionOnePrimaryFields(configs);
        }
        for (String fieldName : fieldNames) {
            if (configs.containsKey(fieldName) == false) {
                throw new IOException(
                        "Missing primary field config in es-index metadata: " + fieldName);
            }
        }
        return new ESIndexArchiveMetadata(version, fieldNames, fieldTypes, configs, files);
    }

    public int version() {
        return version;
    }

    public boolean hasFieldConfigs() {
        return version > 0;
    }

    public List<String> indexedFieldNames() {
        return indexedFieldNames;
    }

    public List<String> indexedFieldTypes() {
        return indexedFieldTypes;
    }

    public Map<String, FieldIndexConfig> fieldConfigs() {
        return fieldConfigs;
    }

    public Map<String, FileRange> files() {
        return files;
    }

    /** Return the deep-copy shape accepted by {@code ArchiveDirectory}. */
    public Map<String, long[]> archiveFileOffsets() {
        Map<String, long[]> result = new LinkedHashMap<>(files.size());
        for (Map.Entry<String, FileRange> entry : files.entrySet()) {
            result.put(entry.getKey(), new long[] {entry.getValue().offset, entry.getValue().length});
        }
        return result;
    }

    /** Validate that no declared Lucene file extends beyond the physical archive. */
    public void validateArchiveLength(long archiveLength) throws IOException {
        if (archiveLength < 0) {
            throw new IOException("Negative es-index archive length: " + archiveLength);
        }
        if (files.isEmpty()) {
            throw new IOException("es-index metadata contains no Lucene files");
        }
        for (Map.Entry<String, FileRange> entry : files.entrySet()) {
            FileRange range = entry.getValue();
            if (range.endExclusive() > archiveLength) {
                throw new IOException(
                        "Lucene file '"
                                + entry.getKey()
                                + "' ends at "
                                + range.endExclusive()
                                + " beyond archive length "
                                + archiveLength);
            }
        }
    }

    /** Whether two archives can safely be mounted as shards of one Elasticsearch index. */
    public boolean hasSameFieldLayout(ESIndexArchiveMetadata other) {
        if (other == null
                || indexedFieldNames.equals(other.indexedFieldNames) == false
                || indexedFieldTypes.equals(other.indexedFieldTypes) == false
                || fieldConfigs.keySet().equals(other.fieldConfigs.keySet()) == false) {
            return false;
        }
        for (String name : fieldConfigs.keySet()) {
            if (sameConfig(fieldConfigs.get(name), other.fieldConfigs.get(name)) == false) {
                return false;
            }
        }
        return true;
    }

    private static boolean sameConfig(FieldIndexConfig left, FieldIndexConfig right) {
        return left.indexType() == right.indexType()
                && left.algorithm() == right.algorithm()
                && left.dimension() == right.dimension()
                && Objects.equals(left.metric(), right.metric())
                && left.analyzer() == right.analyzer()
                && left.scalarType() == right.scalarType()
                && left.algorithmParams().equals(right.algorithmParams());
    }

    private static List<String> inferVersionOnePrimaryFields(
            Map<String, FieldIndexConfig> configs) {
        // Paimon's v1 writer emitted a primary config followed by generated multi-fields.
        List<String> fields = new ArrayList<>();
        List<Map.Entry<String, FieldIndexConfig>> entries = new ArrayList<>(configs.entrySet());
        for (int i = 0; i < entries.size(); ) {
            Map.Entry<String, FieldIndexConfig> primary = entries.get(i++);
            String fieldName = primary.getKey();
            fields.add(fieldName);
            if (i < entries.size()
                    && isGeneratedSubField(fieldName, entries.get(i).getKey())) {
                i++;
            }
            if (i < entries.size()
                    && entries.get(i).getKey().equals(fieldName + ".__paimon_array_present")) {
                i++;
            }
        }
        return fields;
    }

    private static boolean isGeneratedSubField(String fieldName, String candidate) {
        return candidate.equals(fieldName + ".keyword")
                || candidate.equals(fieldName + ".fulltext");
    }

    private static Map<String, FileRange> readFileRanges(DataInputStream in, int fileCount)
            throws IOException {
        if (fileCount < 0 || fileCount > MAX_ENTRY_COUNT) {
            throw new IOException("Invalid file count in es-index metadata: " + fileCount);
        }
        Map<String, FileRange> result = new LinkedHashMap<>();
        for (int i = 0; i < fileCount; i++) {
            String name = readString(in);
            long offset = in.readLong();
            long length = in.readLong();
            FileRange range;
            try {
                range = new FileRange(offset, length);
            } catch (IllegalArgumentException e) {
                throw new IOException(
                        "Invalid file offset/length in es-index metadata: " + name, e);
            }
            if (result.put(name, range) != null) {
                throw new IOException("Duplicate file offset in es-index metadata: " + name);
            }
        }
        return result;
    }

    private static int readCount(DataInputStream in, String description) throws IOException {
        int count = in.readInt();
        if (count < 0 || count > MAX_ENTRY_COUNT) {
            throw new IOException(
                    "Invalid " + description + " count in es-index metadata: " + count);
        }
        return count;
    }

    private static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_STRING_BYTES || length > in.available()) {
            throw new EOFException("Invalid string length in es-index metadata: " + length);
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static String readNullableString(DataInputStream in) throws IOException {
        return in.readBoolean() ? readString(in) : null;
    }

    private static <E extends Enum<E>> E readNullableEnum(
            DataInputStream in, Class<E> enumClass, String description) throws IOException {
        String value = readNullableString(in);
        return value == null ? null : parseEnum(enumClass, value, description);
    }

    private static <E extends Enum<E>> E parseEnum(
            Class<E> enumClass, String value, String description) throws IOException {
        try {
            return Enum.valueOf(enumClass, value);
        } catch (IllegalArgumentException e) {
            throw new IOException("Unknown " + description + " in es-index metadata: " + value, e);
        }
    }

    private static void requireFullyConsumed(DataInputStream in) throws IOException {
        if (in.available() != 0) {
            throw new IOException("Trailing bytes in es-index metadata: " + in.available());
        }
    }

    /** Immutable byte range in the combined Lucene archive. */
    public static final class FileRange {
        private final long offset;
        private final long length;

        public FileRange(long offset, long length) {
            if (offset < 0 || length < 0 || offset > Long.MAX_VALUE - length) {
                throw new IllegalArgumentException(
                        "Invalid archive range: offset=" + offset + ", length=" + length);
            }
            this.offset = offset;
            this.length = length;
        }

        public long offset() {
            return offset;
        }

        public long length() {
            return length;
        }

        public long endExclusive() {
            return offset + length;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof FileRange == false) {
                return false;
            }
            FileRange other = (FileRange) obj;
            return offset == other.offset && length == other.length;
        }

        @Override
        public int hashCode() {
            return Objects.hash(offset, length);
        }

        @Override
        public String toString() {
            return "[" + offset + "," + endExclusive() + ")";
        }
    }
}
