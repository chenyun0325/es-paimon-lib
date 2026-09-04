/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V.
 * Licensed under the Elastic License 2.0.
 */
package org.elasticsearch.paimon;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

/** {@code GET /_paimon/mount/{index}}: decoded mount metadata for an index or alias. */
final class RestPaimonMountStatusAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_paimon/mount/{index}"));
    }

    @Override
    public String getName() {
        return "paimon_mount_status_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client)
            throws IOException {
        String requestedIndex = request.param("index");
        if (requestedIndex == null || requestedIndex.isBlank()) {
            throw new IllegalArgumentException("Paimon mount status requires an index or alias");
        }
        boolean includeFiles = request.paramAsBoolean("include_files", true);
        Integer selectedShard = request.paramAsInteger("shard", null);
        if (selectedShard != null && selectedShard < 0) {
            throw new IllegalArgumentException("Paimon mount shard must be non-negative");
        }

        GetSettingsRequest getSettings =
                new GetSettingsRequest(getMasterNodeTimeout(request))
                        .indices(requestedIndex)
                        .includeDefaults(false)
                        .names(
                                IndexModule.INDEX_STORE_TYPE_SETTING.getKey(),
                                PaimonStorePlugin.INDEX_TABLE_PATH.getKey(),
                                PaimonStorePlugin.INDEX_SNAPSHOT_ID.getKey(),
                                PaimonStorePlugin.INDEX_SHARDS.getKey() + "*");

        return channel ->
                client.admin()
                        .indices()
                        .getSettings(
                                getSettings,
                                new RestBuilderListener<GetSettingsResponse>(channel) {
                                    @Override
                                    public RestResponse buildResponse(
                                            GetSettingsResponse response, XContentBuilder builder)
                                            throws Exception {
                                        builder.startObject();
                                        builder.field("requested_index", requestedIndex);
                                        builder.field("include_files", includeFiles);
                                        builder.startArray("indices");

                                        List<Map.Entry<String, Settings>> indices =
                                                new ArrayList<>(
                                                        response.getIndexToSettings().entrySet());
                                        indices.sort(Map.Entry.comparingByKey());
                                        for (Map.Entry<String, Settings> entry : indices) {
                                            writeIndex(
                                                    builder,
                                                    entry.getKey(),
                                                    entry.getValue(),
                                                    selectedShard,
                                                    includeFiles);
                                        }

                                        builder.endArray();
                                        builder.endObject();
                                        return new RestResponse(RestStatus.OK, builder);
                                    }
                                });
    }

    private static void writeIndex(
            XContentBuilder builder,
            String index,
            Settings settings,
            Integer selectedShard,
            boolean includeFiles)
            throws IOException {
        String storeType = settings.get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey());
        if (PaimonStorePlugin.STORE_TYPE.equals(storeType) == false) {
            throw new IllegalArgumentException(
                    "Index [" + index + "] is not a Paimon mounted index");
        }

        List<String> encodedShards = PaimonStorePlugin.INDEX_SHARDS.get(settings);
        if (encodedShards.isEmpty()) {
            throw new IOException("Index [" + index + "] has no Paimon shard descriptors");
        }
        if (selectedShard != null && selectedShard >= encodedShards.size()) {
            throw new IllegalArgumentException(
                    "Paimon mount shard ["
                            + selectedShard
                            + "] is outside index ["
                            + index
                            + "] shard range [0,"
                            + (encodedShards.size() - 1)
                            + "]");
        }

        builder.startObject();
        builder.field("index", index);
        builder.field("store_type", storeType);
        builder.field(
                "table_path", PaimonStorePlugin.INDEX_TABLE_PATH.get(settings));
        builder.field(
                "snapshot_id", PaimonStorePlugin.INDEX_SNAPSHOT_ID.get(settings));
        builder.field("number_of_shards", encodedShards.size());
        builder.startArray("shards");
        if (selectedShard == null) {
            for (int shard = 0; shard < encodedShards.size(); shard++) {
                builder.map(
                        shardView(
                                shard,
                                ShardMountSpec.decode(encodedShards.get(shard)),
                                includeFiles));
            }
        } else {
            builder.map(
                    shardView(
                            selectedShard,
                            ShardMountSpec.decode(encodedShards.get(selectedShard)),
                            includeFiles));
        }
        builder.endArray();
        builder.endObject();
    }

    static Map<String, Object> shardView(
            int shard, ShardMountSpec spec, boolean includeFiles) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("shard", shard);
        result.put("archive_location", spec.archiveLocation);
        result.put("archive_length", spec.archiveLength);
        result.put("row_range_start", spec.rowRangeStart);
        result.put("row_range_end", spec.rowRangeEnd);
        result.put("row_count", spec.rowCount);
        result.put("file_count", spec.fileOffsets.size());
        if (includeFiles) {
            List<Map<String, Object>> files = new ArrayList<>(spec.fileOffsets.size());
            for (Map.Entry<String, long[]> entry : spec.fileOffsets.entrySet()) {
                Map<String, Object> file = new LinkedHashMap<>();
                file.put("name", entry.getKey());
                file.put("offset", entry.getValue()[0]);
                file.put("length", entry.getValue()[1]);
                files.add(file);
            }
            result.put("files", files);
        }
        return result;
    }
}
