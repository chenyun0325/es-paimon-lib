/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package org.elasticsearch.paimon;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Objects;

/**
 * Read-only Paimon FileIO backed directly by the Aliyun OSS SDK.
 *
 * <p>Paimon's bundled OSS FileIO delegates to Hadoop 3.3.4. That Hadoop release calls the removed
 * {@code Subject.getSubject(AccessControlContext)} API and cannot run on the JDK shipped with
 * Elasticsearch 9.4. Mount planning needs only metadata reads, so using the OSS SDK directly also
 * keeps Hadoop and its process-wide filesystem cache out of the Elasticsearch node.
 */
final class PaimonOssFileIO implements FileIO {

    private static final long serialVersionUID = 1L;
    private static final int READ_BUFFER_SIZE = 1024 * 1024;

    private transient OSS client;

    PaimonOssFileIO() {}

    PaimonOssFileIO(OSS client) {
        this.client = Objects.requireNonNull(client, "client");
    }

    @Override
    public boolean isObjectStore() {
        return true;
    }

    @Override
    public void configure(CatalogContext context) {
        Options options = context.options();
        String endpoint = requireOption(options, "fs.oss.endpoint");
        String accessKeyId = requireOption(options, "fs.oss.accessKeyId");
        String accessKeySecret = requireOption(options, "fs.oss.accessKeySecret");
        closeClient();
        client = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret);
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        ObjectLocation location = location(path);
        if (location.key().isEmpty()) {
            throw new FileNotFoundException("OSS path is a bucket, not an object: " + path);
        }
        ObjectMetadata metadata = metadata(location, path);
        return new OssSeekableInputStream(
                requireClient(), location.bucket(), location.key(), metadata.getContentLength());
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        throw readOnly("create", path);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        ObjectLocation location = location(path);
        if (location.key().isEmpty()) {
            return new OssFileStatus(0L, true, path, 0L);
        }
        try {
            if (requireClient().doesObjectExist(location.bucket(), location.key())) {
                ObjectMetadata metadata =
                        requireClient().getObjectMetadata(location.bucket(), location.key());
                return fileStatus(path, metadata);
            }
            if (directoryExists(location)) {
                return new OssFileStatus(0L, true, path, 0L);
            }
        } catch (RuntimeException e) {
            throw ossFailure("stat", path, e);
        }
        throw new FileNotFoundException("OSS path does not exist: " + path);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        ObjectLocation location = location(path);
        try {
            if (location.key().isEmpty() == false
                    && requireClient().doesObjectExist(location.bucket(), location.key())) {
                return new FileStatus[] {
                    fileStatus(
                            path,
                            requireClient()
                                    .getObjectMetadata(location.bucket(), location.key()))
                };
            }

            String prefix = directoryPrefix(location.key());
            List<FileStatus> statuses = new ArrayList<>();
            String marker = null;
            do {
                ListObjectsRequest request =
                        new ListObjectsRequest(location.bucket())
                                .withPrefix(prefix)
                                .withDelimiter("/")
                                .withMaxKeys(1000)
                                .withMarker(marker);
                ObjectListing listing = requireClient().listObjects(request);
                for (OSSObjectSummary object : listing.getObjectSummaries()) {
                    if (object.getKey().equals(prefix)) {
                        continue;
                    }
                    statuses.add(
                            new OssFileStatus(
                                    object.getSize(),
                                    false,
                                    objectPath(location.bucket(), object.getKey()),
                                    time(object.getLastModified())));
                }
                for (String directory : listing.getCommonPrefixes()) {
                    statuses.add(
                            new OssFileStatus(
                                    0L,
                                    true,
                                    objectPath(location.bucket(), stripTrailingSlash(directory)),
                                    0L));
                }
                marker = listing.isTruncated() ? listing.getNextMarker() : null;
            } while (marker != null);

            if (statuses.isEmpty() && location.key().isEmpty() == false) {
                throw new FileNotFoundException("OSS directory does not exist: " + path);
            }
            statuses.sort(Comparator.comparing(status -> status.getPath().toString()));
            return statuses.toArray(FileStatus[]::new);
        } catch (FileNotFoundException e) {
            throw e;
        } catch (RuntimeException e) {
            throw ossFailure("list", path, e);
        }
    }

    @Override
    public boolean exists(Path path) throws IOException {
        ObjectLocation location = location(path);
        if (location.key().isEmpty()) {
            return true;
        }
        try {
            return requireClient().doesObjectExist(location.bucket(), location.key())
                    || directoryExists(location);
        } catch (RuntimeException e) {
            throw ossFailure("check", path, e);
        }
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        throw readOnly("delete", path);
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        throw readOnly("create directory", path);
    }

    @Override
    public boolean rename(Path source, Path destination) throws IOException {
        throw new IOException(
                "Mounted Paimon OSS FileIO is read-only; cannot rename "
                        + source
                        + " to "
                        + destination);
    }

    @Override
    public void close() {
        closeClient();
    }

    private boolean directoryExists(ObjectLocation location) {
        ObjectListing listing =
                requireClient()
                        .listObjects(
                                new ListObjectsRequest(location.bucket())
                                        .withPrefix(directoryPrefix(location.key()))
                                        .withDelimiter("/")
                                        .withMaxKeys(1));
        return listing.getObjectSummaries().isEmpty() == false
                || listing.getCommonPrefixes().isEmpty() == false;
    }

    private ObjectMetadata metadata(ObjectLocation location, Path path) throws IOException {
        try {
            if (requireClient().doesObjectExist(location.bucket(), location.key()) == false) {
                throw new FileNotFoundException("OSS object does not exist: " + path);
            }
            return requireClient().getObjectMetadata(location.bucket(), location.key());
        } catch (FileNotFoundException e) {
            throw e;
        } catch (RuntimeException e) {
            throw ossFailure("stat", path, e);
        }
    }

    private OSS requireClient() {
        OSS current = client;
        if (current == null) {
            throw new IllegalStateException("Paimon OSS FileIO is not configured or is closed");
        }
        return current;
    }

    private void closeClient() {
        OSS current = client;
        client = null;
        if (current != null) {
            current.shutdown();
        }
    }

    private static String requireOption(Options options, String key) {
        String value = options.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required Paimon OSS option " + key);
        }
        return value;
    }

    private static ObjectLocation location(Path path) throws IOException {
        URI uri = path.toUri();
        if ("oss".equalsIgnoreCase(uri.getScheme()) == false
                || uri.getAuthority() == null
                || uri.getAuthority().isBlank()) {
            throw new IOException("Expected an oss://bucket/key path, got " + path);
        }
        String key = uri.getPath() == null ? "" : uri.getPath();
        while (key.startsWith("/")) {
            key = key.substring(1);
        }
        return new ObjectLocation(uri.getAuthority(), key);
    }

    private static String directoryPrefix(String key) {
        return key.isEmpty() || key.endsWith("/") ? key : key + "/";
    }

    private static String stripTrailingSlash(String value) {
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }

    private static Path objectPath(String bucket, String key) {
        return new Path("oss", bucket, "/" + key);
    }

    private static FileStatus fileStatus(Path path, ObjectMetadata metadata) {
        return new OssFileStatus(
                metadata.getContentLength(), false, path, time(metadata.getLastModified()));
    }

    private static long time(Date date) {
        return date == null ? 0L : date.getTime();
    }

    private static IOException readOnly(String action, Path path) {
        return new IOException(
                "Mounted Paimon OSS FileIO is read-only; cannot " + action + " " + path);
    }

    private static IOException ossFailure(String action, Path path, RuntimeException cause) {
        return new IOException("Failed to " + action + " OSS path " + path, cause);
    }

    private record ObjectLocation(String bucket, String key) {}

    private record OssFileStatus(long getLen, boolean isDir, Path getPath, long getModificationTime)
            implements FileStatus {}

    private static final class OssSeekableInputStream extends SeekableInputStream {

        private final OSS client;
        private final String bucket;
        private final String key;
        private final long length;
        private long position;
        private long bufferStart = -1L;
        private byte[] buffer = new byte[0];
        private boolean closed;

        private OssSeekableInputStream(OSS client, String bucket, String key, long length) {
            this.client = Objects.requireNonNull(client, "client");
            this.bucket = bucket;
            this.key = key;
            this.length = length;
        }

        @Override
        public void seek(long desired) throws IOException {
            ensureOpen();
            if (desired < 0L) {
                throw new IOException("Negative seek position for oss://" + bucket + "/" + key);
            }
            position = desired;
        }

        @Override
        public long getPos() throws IOException {
            ensureOpen();
            return position;
        }

        @Override
        public int read() throws IOException {
            byte[] single = new byte[1];
            return read(single, 0, 1) < 0 ? -1 : single[0] & 0xff;
        }

        @Override
        public int read(byte[] target, int offset, int requested) throws IOException {
            ensureOpen();
            Objects.checkFromIndexSize(offset, requested, target.length);
            if (requested == 0) {
                return 0;
            }
            if (position >= length) {
                return -1;
            }

            int remaining = (int) Math.min((long) requested, length - position);
            int total = remaining;
            while (remaining > 0) {
                ensureBuffered();
                int bufferOffset = Math.toIntExact(position - bufferStart);
                int copied = Math.min(remaining, buffer.length - bufferOffset);
                System.arraycopy(buffer, bufferOffset, target, offset, copied);
                offset += copied;
                remaining -= copied;
                position += copied;
            }
            return total;
        }

        @Override
        public long skip(long count) throws IOException {
            ensureOpen();
            if (count <= 0L) {
                return 0L;
            }
            long skipped = Math.min(count, Math.max(0L, length - position));
            position += skipped;
            return skipped;
        }

        @Override
        public int available() throws IOException {
            ensureOpen();
            return (int) Math.min(Integer.MAX_VALUE, Math.max(0L, length - position));
        }

        @Override
        public void close() {
            closed = true;
            buffer = new byte[0];
        }

        private void ensureBuffered() throws IOException {
            if (position >= bufferStart && position < bufferStart + buffer.length) {
                return;
            }
            int requested = (int) Math.min(READ_BUFFER_SIZE, length - position);
            GetObjectRequest request = new GetObjectRequest(bucket, key);
            request.setRange(position, position + requested - 1L);
            byte[] bytes = new byte[requested];
            try (OSSObject object = client.getObject(request);
                    InputStream input = object.getObjectContent()) {
                int read = 0;
                while (read < requested) {
                    int count = input.read(bytes, read, requested - read);
                    if (count < 0) {
                        throw new EOFException(
                                "Short OSS range response for oss://"
                                        + bucket
                                        + "/"
                                        + key
                                        + ": expected "
                                        + requested
                                        + " bytes, got "
                                        + read);
                    }
                    read += count;
                }
            } catch (RuntimeException e) {
                throw new IOException(
                        "Failed to read OSS range oss://"
                                + bucket
                                + "/"
                                + key
                                + " at "
                                + position
                                + " length "
                                + requested,
                        e);
            }
            bufferStart = position;
            buffer = bytes;
        }

        private void ensureOpen() throws IOException {
            if (closed) {
                throw new IOException("OSS input stream is closed: oss://" + bucket + "/" + key);
            }
        }
    }
}
