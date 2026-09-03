/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V.
 * Licensed under the Elastic License 2.0.
 */
package org.elasticsearch.eslib.io;

import org.elasticsearch.eslib.api.ArchiveDataProvider;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/** Positional, forkable archive reader for local or shared filesystems. */
public final class FileArchiveDataProvider implements ArchiveDataProvider {

    private final Path path;
    private final FileChannel channel;
    private volatile boolean closed;

    public FileArchiveDataProvider(Path path) throws IOException {
        this.path = Objects.requireNonNull(path, "path").toAbsolutePath().normalize();
        this.channel = FileChannel.open(this.path, StandardOpenOption.READ);
    }

    public Path path() {
        return path;
    }

    public long length() throws IOException {
        ensureOpen();
        return channel.size();
    }

    @Override
    public byte[] readRange(long offset, int length) throws IOException {
        ensureOpen();
        if (offset < 0 || length < 0 || offset > Long.MAX_VALUE - length) {
            throw new IllegalArgumentException(
                    "Invalid file range: offset=" + offset + ", length=" + length);
        }
        byte[] bytes = new byte[length];
        ByteBuffer target = ByteBuffer.wrap(bytes);
        long position = offset;
        while (target.hasRemaining()) {
            int read = channel.read(target, position);
            if (read < 0) {
                throw new EOFException(
                        "Read past end of archive "
                                + path
                                + ": offset="
                                + offset
                                + ", length="
                                + length);
            }
            if (read == 0) {
                Thread.yield();
                continue;
            }
            position += read;
        }
        return bytes;
    }

    @Override
    public ArchiveDataProvider fork() throws IOException {
        ensureOpen();
        return new FileArchiveDataProvider(path);
    }

    @Override
    public void close() throws IOException {
        if (closed == false) {
            closed = true;
            channel.close();
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Archive provider is closed: " + path);
        }
    }
}
