/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package org.elasticsearch.paimon;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.eslib.mount.ESIndexArchiveMetadata;
import org.elasticsearch.eslib.mount.LakeShardDescriptor;
import org.elasticsearch.eslib.mount.PaimonMountPlan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Reads one immutable Paimon snapshot and selects its live ESLib global-index files. */
final class PaimonSnapshotPlanner {

    private static final String ES_INDEX_TYPE = "es-index";

    private final Settings nodeSettings;
    private final SecureString ossAccessKeySecret;
    private final Environment environment;

    PaimonSnapshotPlanner(
            Settings nodeSettings,
            SecureString ossAccessKeySecret,
            Environment environment) {
        this.nodeSettings = nodeSettings;
        this.ossAccessKeySecret = ossAccessKeySecret;
        this.environment = environment;
    }

    PaimonMountPlan plan(String tablePath, Long requestedSnapshotId, String selectedField)
            throws IOException {
        Options options = new Options();
        options.set("path", resolveTablePath(tablePath));
        configureObjectStore(options, tablePath);

        try (FileIO fileIO = createStaticFileIO(options, tablePath)) {
            final FileStoreTable table;
            try {
                table = FileStoreTableFactory.create(fileIO, options);
            } catch (RuntimeException e) {
                throw new IOException("Failed to open Paimon table " + tablePath, e);
            }

            Snapshot snapshot;
            try {
                snapshot =
                        requestedSnapshotId == null
                                ? table.snapshotManager().latestSnapshot()
                                : table.snapshotManager().snapshot(requestedSnapshotId);
            } catch (RuntimeException e) {
                throw new IOException(
                        "Failed to load Paimon snapshot "
                                + requestedSnapshotId
                                + " for "
                                + tablePath,
                        e);
            }
            if (snapshot == null) {
                throw new IOException("Paimon table has no snapshot: " + tablePath);
            }
            if (snapshot.indexManifest() == null) {
                throw new IOException(
                        "Paimon snapshot " + snapshot.id() + " has no global-index manifest");
            }

            IndexFileHandler handler = table.store().newIndexFileHandler();
            List<LakeShardDescriptor> candidates = new ArrayList<>();
            for (IndexManifestEntry entry : handler.scan(snapshot, ES_INDEX_TYPE)) {
                if (entry.kind() != FileKind.ADD) {
                    continue;
                }
                IndexFileMeta file = entry.indexFile();
                GlobalIndexMeta global = file.globalIndexMeta();
                if (global == null || global.indexMeta() == null) {
                    throw new IOException(
                            "es-index file " + file.fileName() + " has no global index metadata");
                }
                ESIndexArchiveMetadata archiveMetadata =
                        ESIndexArchiveMetadata.parse(global.indexMeta());
                if (selectedField != null
                        && archiveMetadata.indexedFieldNames().contains(selectedField) == false) {
                    continue;
                }
                candidates.add(
                        new LakeShardDescriptor(
                                handler.filePath(entry).toString(),
                                file.fileSize(),
                                global.rowRangeStart(),
                                global.rowRangeEnd(),
                                file.rowCount(),
                                archiveMetadata));
            }

            if (candidates.isEmpty() && selectedField != null) {
                throw new IOException(
                        "Snapshot "
                                + snapshot.id()
                                + " has no es-index containing field '"
                                + selectedField
                                + "'");
            }
            return PaimonMountPlan.create(snapshot.id(), candidates);
        }
    }

    /** Avoids both Paimon's component class loader and its JDK-incompatible Hadoop OSS adapter. */
    static FileIO createStaticFileIO(Options options, String tablePath) throws IOException {
        FileIO fileIO;
        if (tablePath.regionMatches(true, 0, "oss://", 0, "oss://".length())) {
            fileIO = new PaimonOssFileIO();
        } else {
            fileIO = LocalFileIO.create();
        }
        try {
            fileIO.configure(CatalogContext.create(options));
            return fileIO;
        } catch (RuntimeException e) {
            try {
                fileIO.close();
            } catch (IOException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw new IOException("Failed to configure Paimon FileIO for " + tablePath, e);
        }
    }

    private void configureObjectStore(Options options, String tablePath) throws IOException {
        if (tablePath.regionMatches(true, 0, "oss://", 0, "oss://".length()) == false) {
            return;
        }
        String endpoint = PaimonStorePlugin.OSS_ENDPOINT.get(nodeSettings);
        String accessKeyId = PaimonStorePlugin.OSS_ACCESS_KEY_ID.get(nodeSettings);
        if (endpoint.isEmpty() || accessKeyId.isEmpty()) {
            throw new IOException(
                    "OSS table mounts require node settings paimon.oss.endpoint and "
                            + "paimon.oss.access_key_id");
        }
        if (ossAccessKeySecret == null || ossAccessKeySecret.length() == 0) {
            throw new IOException(
                    "OSS table mounts require secure node setting "
                            + "paimon.oss.access_key_secret");
        }
        options.set("fs.oss.endpoint", endpoint);
        options.set("fs.oss.accessKeyId", accessKeyId);
        options.set("fs.oss.accessKeySecret", ossAccessKeySecret.toString());
    }

    private String resolveTablePath(String tablePath) throws IOException {
        if (tablePath.regionMatches(true, 0, "oss://", 0, "oss://".length())) {
            return tablePath;
        }
        try {
            if (tablePath.matches("^[A-Za-z]:[\\\\/].*")) {
                return requireRepoPath(tablePath).toString();
            }
            java.net.URI uri = new java.net.URI(tablePath);
            if (uri.getScheme() == null) {
                return requireRepoPath(tablePath).toString();
            }
            if (uri.getScheme().equalsIgnoreCase("file")) {
                java.net.URL resolved = environment.resolveRepoURL(uri.toURL());
                if (resolved == null) {
                    throw new IOException(
                            "Local Paimon table is outside every path.repo root: " + tablePath);
                }
                return resolved.toString();
            }
            throw new IOException(
                    "Unsupported Paimon table scheme '"
                            + uri.getScheme()
                            + "'; supported schemes are file and oss");
        } catch (java.net.URISyntaxException | IllegalArgumentException e) {
            throw new IOException("Invalid Paimon table path: " + tablePath, e);
        }
    }

    private java.nio.file.Path requireRepoPath(String tablePath) throws IOException {
        java.nio.file.Path resolved = environment.resolveRepoDir(tablePath);
        if (resolved == null) {
            throw new IOException(
                    "Local Paimon table is outside every path.repo root: " + tablePath);
        }
        return resolved;
    }
}
