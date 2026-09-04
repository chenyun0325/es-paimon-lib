/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V.
 * Licensed under the Elastic License 2.0.
 */
package org.elasticsearch.paimon;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/** Hydrates mounted Lucene hits from the same immutable Paimon snapshot. */
final class PaimonSourceFetchSubPhase implements FetchSubPhase {

    private final Supplier<PaimonRowLoader> rowLoaderSupplier;

    PaimonSourceFetchSubPhase(Supplier<PaimonRowLoader> rowLoaderSupplier) {
        this.rowLoaderSupplier = rowLoaderSupplier;
    }

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) throws IOException {
        FetchSourceContext sourceContext = fetchContext.fetchSourceContext();
        if (sourceContext == null || sourceContext.fetchSource() == false) {
            return null;
        }

        Settings settings =
                fetchContext
                        .getSearchExecutionContext()
                        .getIndexSettings()
                        .getSettings();
        if (PaimonStorePlugin.STORE_TYPE.equals(
                        IndexModule.INDEX_STORE_TYPE_SETTING.get(settings))
                == false) {
            return null;
        }
        if (PaimonStorePlugin.INDEX_SOURCE_ENABLED.get(settings) == false) {
            return null;
        }

        int shardId = fetchContext.getSearchExecutionContext().getShardId();
        List<String> shardSettings = PaimonStorePlugin.INDEX_SHARDS.get(settings);
        if (shardId < 0 || shardId >= shardSettings.size()) {
            throw new IOException(
                    "Missing Paimon source descriptor for shard "
                            + shardId
                            + " of index "
                            + fetchContext.getIndexName());
        }
        ShardMountSpec shard = ShardMountSpec.decode(shardSettings.get(shardId));
        String tablePath = PaimonStorePlugin.INDEX_TABLE_PATH.get(settings);
        long snapshotId = PaimonStorePlugin.INDEX_SNAPSHOT_ID.get(settings);
        List<String> returnFields = PaimonStorePlugin.INDEX_RETURN_FIELDS.get(settings);
        PaimonRowLoader rowLoader =
                Objects.requireNonNull(
                        rowLoaderSupplier.get(), "Paimon row loader is not initialized");

        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(org.apache.lucene.index.LeafReaderContext readerContext) {}

            @Override
            public void process(FetchSubPhase.HitContext hitContext) throws IOException {
                int shardDocId;
                try {
                    shardDocId =
                            Math.addExact(
                                    hitContext.readerContext().docBase,
                                    hitContext.docId());
                } catch (ArithmeticException e) {
                    throw new IOException("Mounted shard document ID overflow", e);
                }
                long rowId = rowId(shard, shardDocId);
                Map<String, Object> row =
                        rowLoader.load(tablePath, snapshotId, rowId, returnFields);
                Source source = Source.fromMap(row, XContentType.JSON);
                if (sourceContext.filter() != null) {
                    source = source.filter(sourceContext.filter());
                }
                hitContext.hit().sourceRef(source.internalSourceRef());
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return StoredFieldsSpec.NO_REQUIREMENTS;
            }
        };
    }

    static long rowId(ShardMountSpec shard, int shardDocId) throws IOException {
        if (shardDocId < 0 || (long) shardDocId >= shard.rowCount) {
            throw new IOException(
                    "Lucene document ID "
                            + shardDocId
                            + " is outside mounted row range ["
                            + shard.rowRangeStart
                            + ","
                            + shard.rowRangeEnd
                            + "]");
        }
        try {
            return Math.addExact(shard.rowRangeStart, (long) shardDocId);
        } catch (ArithmeticException e) {
            throw new IOException("Mounted Paimon Row-ID overflow", e);
        }
    }
}
