/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V.
 * Licensed under the Elastic License 2.0.
 */
package org.elasticsearch.paimon;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.eslib.api.ArchiveDataProvider;
import org.elasticsearch.eslib.io.ArchiveDirectory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.search.fetch.FetchSubPhase;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.function.Supplier;

/** Elasticsearch 9.4 plugin that mounts Paimon ESLib archives as immutable shards. */
public final class PaimonStorePlugin extends Plugin
        implements ActionPlugin, IndexStorePlugin, EnginePlugin, SearchPlugin {

    public static final String STORE_TYPE = "paimon";

    static final Setting<List<String>> INDEX_SHARDS =
            Setting.stringListSetting(
                    "index.paimon.shards",
                    List.of(),
                    Setting.Property.IndexScope);
    static final Setting<Long> INDEX_SNAPSHOT_ID =
            Setting.longSetting(
                    "index.paimon.snapshot_id",
                    -1L,
                    -1L,
                    Setting.Property.IndexScope,
                    Setting.Property.Final);
    static final Setting<String> INDEX_TABLE_PATH =
            Setting.simpleString(
                    "index.paimon.table_path",
                    "",
                    Setting.Property.IndexScope,
                    Setting.Property.Final);
    static final Setting<Boolean> INDEX_SOURCE_ENABLED =
            Setting.boolSetting(
                    "index.paimon.source_enabled",
                    false,
                    Setting.Property.IndexScope,
                    Setting.Property.Final);
    static final Setting<List<String>> INDEX_RETURN_FIELDS =
            Setting.stringListSetting(
                    "index.paimon.return_fields",
                    List.of(),
                    Setting.Property.IndexScope,
                    Setting.Property.Final);

    static final Setting<String> OSS_ENDPOINT =
            Setting.simpleString("paimon.oss.endpoint", "", Setting.Property.NodeScope);
    static final Setting<String> OSS_ACCESS_KEY_ID =
            Setting.simpleString(
                    "paimon.oss.access_key_id",
                    "",
                    Setting.Property.NodeScope,
                    Setting.Property.Filtered);
    static final Setting<SecureString> OSS_ACCESS_KEY_SECRET =
            SecureSetting.secureString(
                    "paimon.oss.access_key_secret",
                    null,
                    Setting.Property.NodeScope);

    private final Settings nodeSettings;
    private final SecureString ossAccessKeySecret;
    private final Map<ShardId, SwitchableMountDirectory> directories =
            new ConcurrentHashMap<>();
    private volatile ThreadPoolHolder threadPoolHolder;
    private volatile Environment environment;
    private volatile PaimonRowLoader rowLoader;

    public PaimonStorePlugin(Settings settings) {
        this.nodeSettings = settings;
        this.ossAccessKeySecret = copyOssAccessKeySecret(settings);
    }

    /**
     * Copies the secure value while Elasticsearch's keystore is still open during plugin
     * construction. The node closes its SecureSettings after startup, so deferred REST/shard
     * operations must never call {@link #OSS_ACCESS_KEY_SECRET} against nodeSettings again.
     */
    static SecureString copyOssAccessKeySecret(Settings settings) {
        if (OSS_ACCESS_KEY_SECRET.exists(settings) == false) {
            return null;
        }
        try (SecureString loaded = OSS_ACCESS_KEY_SECRET.get(settings)) {
            return loaded.clone();
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
                INDEX_SHARDS,
                INDEX_SNAPSHOT_ID,
                INDEX_TABLE_PATH,
                INDEX_SOURCE_ENABLED,
                INDEX_RETURN_FIELDS,
                OSS_ENDPOINT,
                OSS_ACCESS_KEY_ID,
                OSS_ACCESS_KEY_SECRET);
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        threadPoolHolder = new ThreadPoolHolder(services.threadPool());
        environment = services.environment();
        rowLoader = new PaimonRowLoader(nodeSettings, ossAccessKeySecret, environment);
        return List.of();
    }

    @Override
    public List<FetchSubPhase> getFetchSubPhases(FetchPhaseConstructionContext context) {
        return List.of(new PaimonSourceFetchSubPhase(() -> rowLoader));
    }

    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        return Map.of(STORE_TYPE, this::newDirectory);
    }

    private Directory newDirectory(IndexSettings indexSettings, org.elasticsearch.index.shard.ShardPath shardPath)
            throws IOException {
        List<String> shardSettings = INDEX_SHARDS.get(indexSettings.getSettings());
        int shardNumber = shardPath.getShardId().id();
        if (shardNumber < 0 || shardNumber >= shardSettings.size()) {
            throw new IOException(
                    "Missing Paimon mount descriptor for "
                            + shardPath.getShardId()
                            + "; descriptor count="
                            + shardSettings.size());
        }
        ShardMountSpec spec = ShardMountSpec.decode(shardSettings.get(shardNumber));
        Environment currentEnvironment = environment;
        if (currentEnvironment == null) {
            throw new IOException("Paimon store plugin components are not initialized");
        }
        ArchiveDataProvider provider =
                MountArchiveProviderFactory.open(
                        spec, nodeSettings, ossAccessKeySecret, currentEnvironment);
        ArchiveDirectory lake = null;
        try {
            lake = new ArchiveDirectory(provider, spec.fileOffsets);
            FSDirectory local = FSDirectory.open(shardPath.resolveIndex());
            SwitchableMountDirectory mounted = new SwitchableMountDirectory(local, lake);
            SwitchableMountDirectory replaced =
                    directories.put(shardPath.getShardId(), mounted);
            if (replaced != null) {
                replaced.close();
            }
            return mounted;
        } catch (Exception e) {
            if (lake != null) {
                lake.close();
            } else {
                provider.close();
            }
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new IOException("Failed to create Paimon shard directory", e);
        }
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (STORE_TYPE.equals(IndexModule.INDEX_STORE_TYPE_SETTING.get(indexSettings.getSettings()))) {
            return Optional.of(new PaimonReadOnlyEngineFactory(directories));
        }
        return Optional.empty();
    }

    @Override
    public Collection<RestHandler> getRestHandlers(
            RestHandlersServices services,
            Supplier<org.elasticsearch.cluster.node.DiscoveryNodes> nodesInCluster,
            Predicate<org.elasticsearch.features.NodeFeature> clusterSupportsFeature) {
        ThreadPoolHolder holder = threadPoolHolder;
        if (holder == null) {
            throw new IllegalStateException("Paimon store plugin components are not initialized");
        }
        Environment currentEnvironment = environment;
        if (currentEnvironment == null) {
            throw new IllegalStateException("Paimon store plugin environment is not initialized");
        }
        return List.of(
                new RestPaimonMountAction(
                        services.settings(),
                        ossAccessKeySecret,
                        currentEnvironment,
                        holder.threadPool),
                new RestPaimonMountStatusAction());
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        PaimonRowLoader currentRowLoader = rowLoader;
        rowLoader = null;
        if (currentRowLoader != null) {
            try {
                currentRowLoader.close();
            } catch (IOException e) {
                failure = e;
            }
        }
        for (SwitchableMountDirectory directory : directories.values()) {
            try {
                directory.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        directories.clear();
        if (ossAccessKeySecret != null) {
            ossAccessKeySecret.close();
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static final class ThreadPoolHolder {
        private final org.elasticsearch.threadpool.ThreadPool threadPool;

        private ThreadPoolHolder(org.elasticsearch.threadpool.ThreadPool threadPool) {
            this.threadPool = threadPool;
        }
    }
}
