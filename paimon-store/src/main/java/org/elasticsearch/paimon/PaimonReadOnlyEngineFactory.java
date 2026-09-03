/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V.
 * Licensed under the Elastic License 2.0.
 */
package org.elasticsearch.paimon;

import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.TranslogStats;

import java.io.UncheckedIOException;
import java.util.Map;
import java.util.function.Function;

/** Activates the lake directory at engine-open time and prevents all index mutations. */
final class PaimonReadOnlyEngineFactory implements EngineFactory {

    private final Map<ShardId, SwitchableMountDirectory> directories;

    PaimonReadOnlyEngineFactory(Map<ShardId, SwitchableMountDirectory> directories) {
        this.directories = directories;
    }

    @Override
    public Engine newReadWriteEngine(EngineConfig config) {
        SwitchableMountDirectory directory = directories.get(config.getShardId());
        if (directory == null) {
            throw new IllegalStateException(
                    "No Paimon mount directory registered for " + config.getShardId());
        }
        try {
            directory.activateLakeIndex();
        } catch (java.io.IOException e) {
            throw new UncheckedIOException(
                    "Failed to activate Paimon lake shard " + config.getShardId(), e);
        }

        // The lake index is an immutable point-in-time snapshot with no Elasticsearch operation
        // history. Explicit empty stats avoid opening a translog against the archive commit.
        return new ReadOnlyEngine(
                config,
                new SeqNoStats(-1L, -1L, -1L),
                new TranslogStats(),
                true,
                Function.identity(),
                false,
                true);
    }
}
