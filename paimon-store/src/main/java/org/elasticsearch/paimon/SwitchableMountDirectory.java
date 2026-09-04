/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V.
 * Licensed under the Elastic License 2.0.
 */
package org.elasticsearch.paimon;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
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
 * <p>Activation writes a tiny in-memory {@code segments_N} overlay. It references the remote
 * segment files but carries Elasticsearch's bootstrap commit metadata (history UUID, translog
 * UUID, sequence numbers and index version). The durable local bootstrap commit remains
 * self-contained so Elasticsearch can validate and allocate the shard after a node restart.
 */
final class SwitchableMountDirectory extends Directory {

    private final Directory local;
    private final Directory lake;
    private final Directory overlay;
    private volatile boolean active;
    private volatile String overlaySegmentsFile;
    private volatile boolean closed;

    SwitchableMountDirectory(Directory local, Directory lake) {
        this.local = local;
        this.lake = lake;
        this.overlay = new ByteBuffersDirectory();
    }

    synchronized void activateLakeIndex() throws IOException {
        ensureOpen();
        if (active) {
            return;
        }

        // Keep the durable local store self-contained. Elasticsearch's gateway allocator inspects
        // the shard path through a plain FSDirectory before this custom Directory is constructed;
        // persisting a segments_N that references lake-only files makes that pre-allocation check
        // report a corrupt store after a node restart.
        SegmentInfos bootstrap = SegmentInfos.readLatestCommit(local);
        SegmentInfos remote = SegmentInfos.readLatestCommit(lake);
        remote.setUserData(new HashMap<>(bootstrap.getUserData()), false);

        // Build the synthetic commit in memory. On every engine open (including after restart) it
        // is reconstructed from the immutable lake commit and the durable bootstrap metadata.
        long localGeneration = SegmentInfos.getLastCommitGeneration(local);
        do {
            remote.commit(overlay);
        } while (remote.getGeneration() <= localGeneration);
        String newOverlay = remote.getSegmentsFileName();
        overlay.sync(List.of(newOverlay));
        overlay.syncMetaData();
        overlaySegmentsFile = newOverlay;
        active = true;
    }

    boolean isLakeIndexActive() {
        return active;
    }

    @Override
    public String[] listAll() throws IOException {
        ensureOpen();
        if (active == false) {
            return local.listAll();
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
        if (active == false) {
            return local.fileLength(name);
        }
        if (overlaySegmentsFile.equals(name)) {
            return overlay.fileLength(name);
        }
        if (isCommitFile(name)) {
            throw new NoSuchFileException(name);
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
        if (active == false) {
            return local.openInput(name, context);
        }
        if (overlaySegmentsFile.equals(name)) {
            return overlay.openInput(name, context);
        }
        if (isCommitFile(name)) {
            throw new NoSuchFileException(name);
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
            overlay.close();
        } catch (IOException e) {
            failure = e;
        }
        try {
            lake.close();
        } catch (IOException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
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

}
