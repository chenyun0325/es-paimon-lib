/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V.
 * Licensed under the Elastic License 2.0.
 */
package org.elasticsearch.paimon;

import org.elasticsearch.eslib.api.model.FieldIndexConfig;
import org.elasticsearch.eslib.api.model.ScalarFieldType;
import org.elasticsearch.eslib.api.model.VectorAlgorithm;
import org.elasticsearch.eslib.mount.ESIndexArchiveMetadata;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ElasticsearchMappingBuilderTest {

    @Test
    @SuppressWarnings("unchecked")
    void emitsElasticsearchNamesForScalarAndVectorTypes() throws Exception {
        Map<String, FieldIndexConfig> configs = new LinkedHashMap<>();
        configs.put(
                "price",
                FieldIndexConfig.builder("price", FieldIndexConfig.IndexType.SCALAR)
                        .scalarType(ScalarFieldType.INT)
                        .build());
        configs.put(
                "embedding",
                FieldIndexConfig.builder("embedding", FieldIndexConfig.IndexType.VECTOR)
                        .algorithm(VectorAlgorithm.DISKBBQ)
                        .dimension(384)
                        .metric("cosine")
                        .build());
        ESIndexArchiveMetadata metadata =
                ESIndexArchiveMetadata.parse(
                        metadata(
                                List.of("price", "embedding"),
                                List.of("INT", "ARRAY<FLOAT>"),
                                configs));

        Map<String, Object> mapping = ElasticsearchMappingBuilder.build(metadata);
        Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");
        assertEquals("integer", ((Map<String, Object>) properties.get("price")).get("type"));
        Map<String, Object> vector = (Map<String, Object>) properties.get("embedding");
        assertEquals("dense_vector", vector.get("type"));
        assertEquals("cosine", vector.get("similarity"));
        assertEquals("bbq_disk", ((Map<String, Object>) vector.get("index_options")).get("type"));
        assertEquals(Map.of("enabled", false), mapping.get("_source"));

        Map<String, Object> hydratedMapping =
                ElasticsearchMappingBuilder.build(metadata, true);
        assertEquals(Map.of("enabled", true), hydratedMapping.get("_source"));
    }

    @Test
    void rejectsNativeVectorFormat() throws Exception {
        Map<String, FieldIndexConfig> configs =
                Map.of(
                        "embedding",
                        FieldIndexConfig.builder("embedding", FieldIndexConfig.IndexType.VECTOR)
                                .algorithm(VectorAlgorithm.NATIVE)
                                .dimension(8)
                                .build());
        ESIndexArchiveMetadata metadata =
                ESIndexArchiveMetadata.parse(
                        metadata(List.of("embedding"), List.of("ARRAY<FLOAT>"), configs));

        IOException error =
                assertThrows(
                        IOException.class, () -> ElasticsearchMappingBuilder.build(metadata));
        assertTrue(error.getMessage().contains("Native ESLib vector indexes"));
    }

    private static byte[] metadata(
            List<String> names, List<String> types, Map<String, FieldIndexConfig> configs)
            throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(ESIndexArchiveMetadata.MAGIC);
            out.writeInt(ESIndexArchiveMetadata.CURRENT_VERSION);
            out.writeInt(names.size());
            for (int i = 0; i < names.size(); i++) {
                writeString(out, names.get(i));
                writeString(out, types.get(i));
            }
            out.writeInt(configs.size());
            for (Map.Entry<String, FieldIndexConfig> entry : configs.entrySet()) {
                FieldIndexConfig config = entry.getValue();
                writeString(out, entry.getKey());
                writeString(out, config.indexType().name());
                writeNullable(out, config.algorithm());
                out.writeInt(config.dimension());
                writeNullableString(out, config.metric());
                writeNullable(out, config.analyzer());
                writeNullable(out, config.scalarType());
                out.writeInt(config.algorithmParams().size());
                for (Map.Entry<String, String> parameter :
                        config.algorithmParams().entrySet()) {
                    writeString(out, parameter.getKey());
                    writeString(out, parameter.getValue());
                }
            }
            out.writeInt(1);
            writeString(out, "segments_1");
            out.writeLong(0);
            out.writeLong(1);
        }
        return bytes.toByteArray();
    }

    private static void writeNullable(DataOutputStream out, Enum<?> value) throws IOException {
        writeNullableString(out, value == null ? null : value.name());
    }

    private static void writeNullableString(DataOutputStream out, String value)
            throws IOException {
        out.writeBoolean(value != null);
        if (value != null) {
            writeString(out, value);
        }
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(encoded.length);
        out.write(encoded);
    }
}
