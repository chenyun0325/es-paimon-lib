/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V.
 * Licensed under the Elastic License 2.0.
 */
package org.elasticsearch.paimon;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Lets Elasticsearch bootstrap its normal empty-store recovery locally, then atomically exposes
 * the lake archive as the shard's immutable Lucene directory.
 *
 * <p>Activation writes only a tiny local {@code segments_N} overlay. It references the remote
 * segment files but carries Elasticsearch's bootstrap commit metadata (history UUID, translog
 * UUID, sequence numbers and index version), so normal shard invariants remain valid without
 * rewriting or copying the lake index.
 */
final class SwitchableMountDirectory extends Directory {

    private final Directory local;
    private final Directory lake;
    private final boolean resumingMountedShard;
    private volatile boolean active;
    private volatile String overlaySegmentsFile;
    private volatile boolean closed;

    SwitchableMountDirectory(Directory local, Directory lake) throws IOException {
        this.local = local;
        this.lake = lake;
        this.resumingMountedShard = containsCommit(local.listAll());
    }

    synchronized void activateLakeIndex() throws IOException {
        ensureOpen();
        if (active) {
            return;
        }

        // On restart the latest local commit is the synthetic overlay and its .si files live in
        // the lake directory, so read it through this directory's pre-active fallback view.
        SegmentInfos bootstrap = SegmentInfos.readLatestCommit(this);
        SegmentInfos remote = SegmentInfos.readLatestCommit(lake);
        remote.setUserData(new HashMap<>(bootstrap.getUserData()), false);

        // A node restart leaves the previous synthetic segments_N in the local directory. Advance
        // the lake SegmentInfos generation in memory until its next commit cannot collide, then
        // copy only that tiny commit file into the durable local overlay.
        long localGeneration = SegmentInfos.getLastCommitGeneration(local);
        try (Directory staging = new ByteBuffersDirectory()) {
            do {
                remote.commit(staging);
            } while (remote.getGeneration() <= localGeneration);
            String newOverlay = remote.getSegmentsFileName();
            local.copyFrom(staging, newOverlay, newOverlay, IOContext.DEFAULT);
            local.sync(List.of(newOverlay));
            local.syncMetaData();
            overlaySegmentsFile = newOverlay;
        }
        active = true;
    }

    boolean isLakeIndexActive() {
        return active;
    }

    @Override
    public String[] listAll() throws IOException {
        ensureOpen();
        if (active == false && resumingMountedShard == false) {
            return local.listAll();
        }
        if (active == false) {
            List<String> names = new ArrayList<>();
            Set<String> seen = new HashSet<>();
            for (String name : local.listAll()) {
                if (seen.add(name)) {
                    names.add(name);
                }
            }
            addLakeDataFiles(names, seen);
            return names.toArray(new String[0]);
        }
        List<String> names = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        addLakeDataFiles(names, seen);
        if (seen.add(overlaySegmentsFile)) {
            names.add(overlaySegmentsFile);
        }
        return names.toArray(new String[0]);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        ensureWritable();
        local.deleteFile(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
        ensureOpen();
        if (active == false && resumingMountedShard == false) {
            return local.fileLength(name);
        }
        if (active && overlaySegmentsFile.equals(name)) {
            return local.fileLength(name);
        }
        if (isCommitFile(name)) {
            if (active) {
                throw new NoSuchFileException(name);
            }
            return local.fileLength(name);
        }
        if (active == false) {
            try {
                return local.fileLength(name);
            } catch (NoSuchFileException ignored) {
                // The synthetic local commit references immutable lake segment files.
            }
        }
        return lake.fileLength(name);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureWritable();
        return local.createOutput(name, context);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
            throws IOException {
        ensureWritable();
        return local.createTempOutput(prefix, suffix, context);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        ensureWritable();
        local.sync(names);
    }

    @Override
    public void syncMetaData() throws IOException {
        ensureWritable();
        local.syncMetaData();
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        ensureWritable();
        local.rename(source, dest);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        if (active == false && resumingMountedShard == false) {
            return local.openInput(name, context);
        }
        if (active && overlaySegmentsFile.equals(name)) {
            return local.openInput(name, context);
        }
        if (isCommitFile(name)) {
            if (active) {
                throw new NoSuchFileException(name);
            }
            return local.openInput(name, context);
        }
        if (active == false) {
            try {
                return local.openInput(name, context);
            } catch (NoSuchFileException ignored) {
                // Fall through to the lake archive.
            }
        }
        return lake.openInput(name, context);
    }

    @Override
    public Lock obtainLock(String name) throws IOException {
        ensureOpen();
        return active ? lake.obtainLock(name) : local.obtainLock(name);
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        ensureOpen();
        return active ? Set.of() : local.getPendingDeletions();
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOException failure = null;
        try {
            lake.close();
        } catch (IOException e) {
            failure = e;
        }
        try {
            local.close();
        } catch (IOException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void ensureWritable() throws IOException {
        ensureOpen();
        if (active) {
            throw new IOException("Mounted Paimon shard is read-only");
        }
    }

    @Override
    protected void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Mounted directory is closed");
        }
    }

    private static boolean isCommitFile(String name) {
        return name.startsWith(IndexFileNames.SEGMENTS + "_")
                || name.startsWith(IndexFileNames.PENDING_SEGMENTS + "_");
    }

    private void addLakeDataFiles(List<String> names, Set<String> seen) throws IOException {
        for (String name : lake.listAll()) {
            if (isCommitFile(name) == false && seen.add(name)) {
                names.add(name);
            }
        }
    }

    private static boolean containsCommit(String[] names) {
        for (String name : names) {
            if (name.startsWith(IndexFileNames.SEGMENTS + "_")) {
                return true;
            }
        }
        return false;
    }
}
