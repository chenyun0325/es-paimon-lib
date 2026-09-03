/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V.
 * Licensed under the Elastic License 2.0.
 */
package org.elasticsearch.eslib.mount;

import org.elasticsearch.eslib.api.model.FieldIndexConfig;
import org.elasticsearch.eslib.api.model.VectorAlgorithm;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ESIndexArchiveMetadataTest {

    @Test
    void parsesVersionTwoMetadataAndValidatesRanges() throws Exception {
        ESIndexArchiveMetadata metadata = ESIndexArchiveMetadata.parse(vectorMetadata("embedding"));

        assertEquals(2, metadata.version());
        assertEquals(List.of("embedding"), metadata.indexedFieldNames());
        assertEquals(List.of("VECTOR<FLOAT,3>"), metadata.indexedFieldTypes());
        FieldIndexConfig config = metadata.fieldConfigs().get("embedding");
        assertEquals(FieldIndexConfig.IndexType.VECTOR, config.indexType());
        assertEquals(VectorAlgorithm.HNSW, config.algorithm());
        assertEquals(3, config.dimension());
        assertEquals(2, metadata.files().size());
        metadata.validateArchiveLength(260L);

        IOException error =
                assertThrows(IOException.class, () -> metadata.validateArchiveLength(259L));
        assertTrue(error.getMessage().contains("b.cfs"));
    }

    @Test
    void rejectsTrailingBytes() throws Exception {
        byte[] valid = vectorMetadata("embedding");
        byte[] invalid = java.util.Arrays.copyOf(valid, valid.length + 1);
        assertThrows(IOException.class, () -> ESIndexArchiveMetadata.parse(invalid));
    }

    static byte[] vectorMetadata(String fieldName) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(ESIndexArchiveMetadata.MAGIC);
            out.writeInt(ESIndexArchiveMetadata.CURRENT_VERSION);
            out.writeInt(1);
            writeString(out, fieldName);
            writeString(out, "VECTOR<FLOAT,3>");
            out.writeInt(1);
            writeString(out, fieldName);
            writeString(out, "VECTOR");
            writeNullable(out, "HNSW");
            out.writeInt(3);
            writeNullable(out, "cosine");
            writeNullable(out, null);
            writeNullable(out, null);
            out.writeInt(1);
            writeString(out, "max_connections");
            writeString(out, "16");
            out.writeInt(2);
            writeString(out, "segments_1");
            out.writeLong(100L);
            out.writeLong(20L);
            writeString(out, "b.cfs");
            out.writeLong(200L);
            out.writeLong(60L);
        }
        return bytes.toByteArray();
    }

    private static void writeNullable(DataOutputStream out, String value) throws IOException {
        out.writeBoolean(value != null);
        if (value != null) {
            writeString(out, value);
        }
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(valueBytes.length);
        out.write(valueBytes);
    }
}
