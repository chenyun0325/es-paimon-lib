/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V.
 * Licensed under the Elastic License 2.0.
 */
package org.elasticsearch.paimon;

import org.elasticsearch.eslib.api.model.FieldIndexConfig;
import org.elasticsearch.eslib.api.model.ScalarFieldType;
import org.elasticsearch.eslib.api.model.VectorAlgorithm;
import org.elasticsearch.eslib.mount.ESIndexArchiveMetadata;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/** Converts ESLib field metadata into the mapping used to parse Elasticsearch queries. */
final class ElasticsearchMappingBuilder {

    private ElasticsearchMappingBuilder() {}

    static Map<String, Object> build(ESIndexArchiveMetadata metadata) throws IOException {
        if (metadata.hasFieldConfigs() == false || metadata.indexedFieldNames().isEmpty()) {
            throw new IOException(
                    "The selected es-index uses legacy offset-only metadata and cannot infer an "
                            + "Elasticsearch mapping");
        }
        Map<String, Object> properties = new LinkedHashMap<>();
        for (String fieldName : metadata.indexedFieldNames()) {
            FieldIndexConfig primary = metadata.fieldConfigs().get(fieldName);
            Map<String, Object> fieldMapping = mappingFor(primary);
            Map<String, Object> subFields = new LinkedHashMap<>();
            addSubField(metadata, fieldName, ".keyword", "keyword", subFields);
            addSubField(metadata, fieldName, ".fulltext", "fulltext", subFields);
            if (subFields.isEmpty() == false) {
                fieldMapping.put("fields", subFields);
            }
            properties.put(fieldName, fieldMapping);
        }

        Map<String, Object> mapping = new LinkedHashMap<>();
        mapping.put("dynamic", "strict");
        mapping.put("_source", Map.of("enabled", false));
        mapping.put("properties", properties);
        return mapping;
    }

    private static void addSubField(
            ESIndexArchiveMetadata metadata,
            String primaryName,
            String suffix,
            String mappingName,
            Map<String, Object> subFields)
            throws IOException {
        FieldIndexConfig config = metadata.fieldConfigs().get(primaryName + suffix);
        if (config != null) {
            subFields.put(mappingName, mappingFor(config));
        }
    }

    private static Map<String, Object> mappingFor(FieldIndexConfig config) throws IOException {
        if (config == null) {
            throw new IOException("Missing ESLib field configuration");
        }
        Map<String, Object> mapping = new LinkedHashMap<>();
        switch (config.indexType()) {
            case VECTOR:
                mapping.put("type", "dense_vector");
                mapping.put("dims", config.dimension());
                mapping.put("index", true);
                mapping.put("similarity", similarity(config.metric()));
                Map<String, Object> indexOptions = vectorIndexOptions(config.algorithm());
                if (indexOptions.isEmpty() == false) {
                    mapping.put("index_options", indexOptions);
                }
                break;
            case FULLTEXT:
                mapping.put("type", "text");
                if (config.analyzer() != null) {
                    mapping.put("analyzer", config.analyzer().getName());
                }
                break;
            case KEYWORD:
                mapping.put("type", "keyword");
                break;
            case SCALAR:
                ScalarFieldType scalarType = config.scalarType();
                if (scalarType == null) {
                    throw new IOException(
                            "Missing scalar type for ESLib field " + config.fieldName());
                }
                mapping.put("type", scalarMappingType(scalarType));
                break;
            case GEO_POINT:
                mapping.put("type", "geo_point");
                break;
            case DATE:
                mapping.put("type", "date");
                break;
            default:
                throw new IOException(
                        "Unsupported ESLib index type for mount: " + config.indexType());
        }
        return mapping;
    }

    private static String similarity(String metric) throws IOException {
        if (metric == null) {
            return "l2_norm";
        }
        switch (metric.toLowerCase(java.util.Locale.ROOT)) {
            case "l2":
            case "euclidean":
            case "l2_norm":
                return "l2_norm";
            case "cosine":
                return "cosine";
            case "dot":
            case "dot_product":
            case "inner_product":
                return "dot_product";
            case "max_inner_product":
            case "maximum_inner_product":
                return "max_inner_product";
            default:
                throw new IOException("Unsupported vector metric for mount: " + metric);
        }
    }

    private static String scalarMappingType(ScalarFieldType scalarType) {
        // Lucene/ESLib calls this scalar type INT; Elasticsearch's mapping name is "integer".
        // The remaining names happen to be identical, but spelling the mapping out prevents the
        // two independently versioned type systems from being conflated again.
        switch (scalarType) {
            case INT:
                return "integer";
            case LONG:
                return "long";
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case KEYWORD:
                return "keyword";
            case DATE:
                return "date";
            case GEO_POINT:
                return "geo_point";
            default:
                throw new IllegalArgumentException("Unsupported scalar type: " + scalarType);
        }
    }

    private static Map<String, Object> vectorIndexOptions(VectorAlgorithm algorithm)
            throws IOException {
        if (algorithm == null) {
            return Map.of();
        }
        switch (algorithm) {
            case DISKBBQ:
                return Map.of("type", "bbq_disk");
            case INT8_HNSW:
                return Map.of("type", "int8_hnsw");
            case HNSW:
                return Map.of("type", "hnsw");
            case NATIVE:
                throw new IOException(
                        "Native ESLib vector indexes do not have a compatible Elasticsearch "
                                + "dense_vector mapping");
            default:
                throw new IOException("Unsupported vector algorithm for mount: " + algorithm);
        }
    }
}
