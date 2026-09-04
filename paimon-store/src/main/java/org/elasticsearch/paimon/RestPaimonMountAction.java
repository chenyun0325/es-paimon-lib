/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V.
 * Licensed under the Elastic License 2.0.
 */
package org.elasticsearch.paimon;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.eslib.mount.PaimonMountPlan;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

/** {@code POST /_paimon/mount}: snapshot metadata -> read-only Elasticsearch index + alias. */
final class RestPaimonMountAction extends BaseRestHandler {

    private static final Set<String> REQUEST_FIELDS =
            Set.of(
                    "table_path",
                    "index_name",
                    "snapshot_id",
                    "vector_field_name",
                    "storage_mode",
                    "source_enabled",
                    "return_fields",
                    "auth_type");

    private final Settings nodeSettings;
    private final SecureString ossAccessKeySecret;
    private final Environment environment;
    private final ThreadPool threadPool;

    RestPaimonMountAction(
            Settings nodeSettings,
            SecureString ossAccessKeySecret,
            Environment environment,
            ThreadPool threadPool) {
        this.nodeSettings = nodeSettings;
        this.ossAccessKeySecret = ossAccessKeySecret;
        this.environment = environment;
        this.threadPool = threadPool;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_paimon/mount"));
    }

    @Override
    public String getName() {
        return "paimon_mount_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client)
            throws IOException {
        if (request.hasContent() == false) {
            throw new IllegalArgumentException("Paimon mount request requires a JSON body");
        }
        final Map<String, Object> body;
        try (XContentParser parser = request.contentParser()) {
            body = parser.mapOrdered();
        }
        for (String field : body.keySet()) {
            if (REQUEST_FIELDS.contains(field) == false) {
                throw new IllegalArgumentException("Unknown Paimon mount field [" + field + "]");
            }
        }

        final String tablePath = requiredString(body, "table_path");
        final String alias = requiredString(body, "index_name");
        final String selectedField = optionalString(body, "vector_field_name");
        final Long snapshotId = optionalLong(body, "snapshot_id");
        final boolean sourceEnabled = optionalBoolean(body, "source_enabled", false);
        final List<String> returnFields = optionalStringList(body, "return_fields");
        final String storageMode = optionalString(body, "storage_mode");
        final String authType = optionalString(body, "auth_type");
        if (sourceEnabled == false && returnFields.isEmpty() == false) {
            throw new IllegalArgumentException(
                    "return_fields requires source_enabled=true");
        }
        if (storageMode != null
                && storageMode.equals("mmap") == false
                && storageMode.equals("remote") == false) {
            throw new IllegalArgumentException(
                    "storage_mode must be mmap or remote, got [" + storageMode + "]");
        }
        if (authType != null && authType.equals("node") == false) {
            throw new IllegalArgumentException(
                    "Only auth_type=node is supported: put OSS credentials in the Elasticsearch "
                            + "keystore so secrets are never copied into cluster-state index settings");
        }

        final var masterTimeout = getMasterNodeTimeout(request);
        final var ackTimeout = getAckTimeout(request);
        return channel ->
                threadPool
                        .executor(ThreadPool.Names.GENERIC)
                        .execute(
                                () -> {
                                    RestToXContentListener<CreateIndexResponse> failureListener =
                                            new RestToXContentListener<>(channel);
                                    try {
                                        PaimonMountPlan plan =
                                                new PaimonSnapshotPlanner(
                                                                nodeSettings,
                                                                ossAccessKeySecret,
                                                                environment)
                                                        .plan(
                                                                tablePath,
                                                                snapshotId,
                                                                selectedField);
                                        String physicalIndex = alias + "_" + plan.snapshotId();
                                        List<String> encodedShards =
                                                new ArrayList<>(plan.numberOfShards());
                                        for (PaimonMountPlan.MountedShard shard : plan.shards()) {
                                            encodedShards.add(
                                                    ShardMountSpec.encode(shard.descriptor()));
                                        }

                                        Settings indexSettings =
                                                Settings.builder()
                                                        .put("index.store.type", PaimonStorePlugin.STORE_TYPE)
                                                        .put("index.number_of_shards", plan.numberOfShards())
                                                        // Peer recovery would copy immutable lake segments. Each
                                                        // global shard is therefore mounted once as a primary.
                                                        .put("index.number_of_replicas", 0)
                                                        .put("index.blocks.write", true)
                                                        .put(
                                                                PaimonStorePlugin.INDEX_TABLE_PATH.getKey(),
                                                                tablePath)
                                                        .put(
                                                                PaimonStorePlugin.INDEX_SNAPSHOT_ID.getKey(),
                                                                plan.snapshotId())
                                                        .put(
                                                                PaimonStorePlugin.INDEX_SOURCE_ENABLED.getKey(),
                                                                sourceEnabled)
                                                        .putList(
                                                                PaimonStorePlugin.INDEX_RETURN_FIELDS.getKey(),
                                                                returnFields)
                                                        .putList(
                                                                PaimonStorePlugin.INDEX_SHARDS.getKey(),
                                                                encodedShards)
                                                        .build();
                                        CreateIndexRequest create =
                                                new CreateIndexRequest(physicalIndex)
                                                        .settings(indexSettings)
                                                        .mapping(
                                                                ElasticsearchMappingBuilder.build(
                                                                        plan.fieldLayout(),
                                                                        sourceEnabled));
                                        create.masterNodeTimeout(masterTimeout);
                                        create.ackTimeout(ackTimeout);

                                        client.admin()
                                                .indices()
                                                .create(
                                                        create,
                                                        ActionListener.wrap(
                                                                response ->
                                                                        switchAlias(
                                                                                client,
                                                                                channel,
                                                                                alias,
                                                                                physicalIndex,
                                                                                plan,
                                                                                sourceEnabled,
                                                                                returnFields,
                                                                                masterTimeout,
                                                                                ackTimeout),
                                                                failureListener::onFailure));
                                    } catch (LinkageError e) {
                                        // Third-party codecs can fail during static initialization
                                        // (for example when an ES entitlement is missing). Convert
                                        // that dependency failure into a REST error instead of letting
                                        // it escape the generic executor and terminate the ES process.
                                        failureListener.onFailure(
                                                new IOException(
                                                        "Paimon mount dependency initialization failed",
                                                        e));
                                    } catch (Exception e) {
                                        failureListener.onFailure(e);
                                    }
                                });
    }

    private static void switchAlias(
            NodeClient client,
            org.elasticsearch.rest.RestChannel channel,
            String alias,
            String physicalIndex,
            PaimonMountPlan plan,
            boolean sourceEnabled,
            List<String> returnFields,
            org.elasticsearch.core.TimeValue masterTimeout,
            org.elasticsearch.core.TimeValue ackTimeout) {
        IndicesAliasesRequest aliases = new IndicesAliasesRequest(masterTimeout, ackTimeout);
        aliases.addAliasAction(
                IndicesAliasesRequest.AliasActions.remove()
                        .indices(alias + "_*")
                        .aliases(alias)
                        .mustExist(false));
        aliases.addAliasAction(
                IndicesAliasesRequest.AliasActions.add()
                        .index(physicalIndex)
                        .alias(alias)
                        .writeIndex(false));

        client.admin()
                .indices()
                .aliases(
                        aliases,
                        new RestBuilderListener<IndicesAliasesResponse>(channel) {
                            @Override
                            public RestResponse buildResponse(
                                    IndicesAliasesResponse response, XContentBuilder builder)
                                    throws Exception {
                                builder.startObject();
                                builder.field("acknowledged", response.isAcknowledged());
                                builder.field("alias", alias);
                                builder.field("index", physicalIndex);
                                builder.field("snapshot_id", plan.snapshotId());
                                builder.field("shards", plan.numberOfShards());
                                builder.field("source_enabled", sourceEnabled);
                                builder.field("return_fields", returnFields);
                                builder.endObject();
                                return new RestResponse(RestStatus.OK, builder);
                            }
                        });
    }

    private static String requiredString(Map<String, Object> body, String name) {
        String value = optionalString(body, name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required Paimon mount field [" + name + "]");
        }
        return value;
    }

    private static String optionalString(Map<String, Object> body, String name) {
        Object value = body.get(name);
        if (value == null) {
            return null;
        }
        if (value instanceof String == false) {
            throw new IllegalArgumentException("Paimon mount field [" + name + "] must be a string");
        }
        return (String) value;
    }

    private static Long optionalLong(Map<String, Object> body, String name) {
        Object value = body.get(name);
        if (value == null) {
            return null;
        }
        if (value instanceof Number == false) {
            throw new IllegalArgumentException("Paimon mount field [" + name + "] must be a number");
        }
        return ((Number) value).longValue();
    }

    private static boolean optionalBoolean(
            Map<String, Object> body, String name, boolean defaultValue) {
        Object value = body.get(name);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean == false) {
            throw new IllegalArgumentException("Paimon mount field [" + name + "] must be boolean");
        }
        return (Boolean) value;
    }

    private static List<String> optionalStringList(Map<String, Object> body, String name) {
        Object value = body.get(name);
        if (value == null) {
            return List.of();
        }
        if (value instanceof List<?> == false) {
            throw new IllegalArgumentException(
                    "Paimon mount field [" + name + "] must be an array of strings");
        }
        LinkedHashSet<String> fields = new LinkedHashSet<>();
        for (Object element : (List<?>) value) {
            if (element instanceof String == false || ((String) element).isBlank()) {
                throw new IllegalArgumentException(
                        "Paimon mount field ["
                                + name
                                + "] must contain only non-blank strings");
            }
            fields.add((String) element);
        }
        return List.copyOf(fields);
    }
}
