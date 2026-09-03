/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V.
 * Licensed under the Elastic License 2.0.
 */
package org.elasticsearch.eslib.io;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;

import org.elasticsearch.eslib.api.ArchiveDataProvider;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Forkable range reader for an OSS object. Forks share one thread-safe OSS client and close it
 * when the last provider is closed.
 */
public final class OSSArchiveDataProvider implements ArchiveDataProvider {

    private final SharedClient shared;
    private final String bucket;
    private final String key;
    private final AtomicBoolean closed = new AtomicBoolean();

    public OSSArchiveDataProvider(
            String endpoint,
            String accessKeyId,
            String accessKeySecret,
            String bucket,
            String key) {
        this(
                new SharedClient(
                        new OSSClientBuilder()
                                .build(
                                        requireText(endpoint, "endpoint"),
                                        requireText(accessKeyId, "accessKeyId"),
                                        requireText(accessKeySecret, "accessKeySecret"))),
                bucket,
                key,
                false);
    }

    private OSSArchiveDataProvider(SharedClient shared, String bucket, String key, boolean retain) {
        this.shared = Objects.requireNonNull(shared, "shared");
        this.bucket = requireText(bucket, "bucket");
        this.key = requireText(key, "key");
        if (retain) {
            shared.retain();
        }
    }

    @Override
    public byte[] readRange(long offset, int length) throws IOException {
        ensureOpen();
        if (offset < 0 || length < 0 || offset > Long.MAX_VALUE - length) {
            throw new IllegalArgumentException(
                    "Invalid OSS range: offset=" + offset + ", length=" + length);
        }
        if (length == 0) {
            return new byte[0];
        }

        GetObjectRequest request = new GetObjectRequest(bucket, key);
        request.setRange(offset, offset + length - 1L);
        byte[] result = new byte[length];
        try (OSSObject object = shared.client.getObject(request);
                InputStream input = object.getObjectContent()) {
            int position = 0;
            while (position < length) {
                int read = input.read(result, position, length - position);
                if (read < 0) {
                    throw new EOFException(
                            "Short OSS range response for oss://"
                                    + bucket
                                    + "/"
                                    + key
                                    + ": expected "
                                    + length
                                    + " bytes, got "
                                    + position);
                }
                position += read;
            }
            return result;
        } catch (RuntimeException e) {
            throw new IOException(
                    "Failed to read OSS range oss://"
                            + bucket
                            + "/"
                            + key
                            + " at "
                            + offset
                            + " length "
                            + length,
                    e);
        }
    }

    @Override
    public ArchiveDataProvider fork() throws IOException {
        ensureOpen();
        return new OSSArchiveDataProvider(shared, bucket, key, true);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            shared.release();
        }
    }

    private void ensureOpen() throws IOException {
        if (closed.get()) {
            throw new IOException("OSS archive provider is closed: oss://" + bucket + "/" + key);
        }
    }

    private static String requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return value;
    }

    private static final class SharedClient {
        private final OSS client;
        private final AtomicInteger references = new AtomicInteger(1);

        private SharedClient(OSS client) {
            this.client = client;
        }

        private void retain() {
            while (true) {
                int current = references.get();
                if (current == 0) {
                    throw new IllegalStateException("OSS client is already closed");
                }
                if (references.compareAndSet(current, current + 1)) {
                    return;
                }
            }
        }

        private void release() {
            int remaining = references.decrementAndGet();
            if (remaining == 0) {
                client.shutdown();
            } else if (remaining < 0) {
                throw new IllegalStateException("OSS client reference count underflow");
            }
        }
    }
}
