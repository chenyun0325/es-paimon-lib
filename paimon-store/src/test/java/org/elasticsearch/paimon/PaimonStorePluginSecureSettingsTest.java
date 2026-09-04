/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V.
 * Licensed under the Elastic License 2.0.
 */
package org.elasticsearch.paimon;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.OSSObject;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.options.Options;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonStorePluginSecureSettingsTest {

    @Test
    void exposesShardDescriptorsButKeepsOssCredentialsFiltered() {
        assertFalse(PaimonStorePlugin.INDEX_SHARDS.isFiltered());
        assertFalse(PaimonStorePlugin.INDEX_SOURCE_ENABLED.isFiltered());
        assertFalse(PaimonStorePlugin.INDEX_RETURN_FIELDS.isFiltered());
        assertTrue(PaimonStorePlugin.OSS_ACCESS_KEY_ID.isFiltered());
    }

    @Test
    void discoversParquetFormatRequiredByTableSchemaValidation() {
        assertEquals(
                "parquet",
                FileFormat.fromIdentifier("parquet", new Options()).getFormatIdentifier());
    }

    @Test
    void createsOssFileIOWithoutPaimonComponentClassLoaderOrHadoop() throws Exception {
        Options options = new Options();
        options.set("path", "oss://test-bucket/warehouse/default.db/table");
        options.set("fs.oss.endpoint", "https://oss-cn-shanghai.aliyuncs.com");
        options.set("fs.oss.accessKeyId", "test-id");
        options.set("fs.oss.accessKeySecret", "test-secret");

        try (FileIO fileIO =
                PaimonSnapshotPlanner.createStaticFileIO(
                        options, "oss://test-bucket/warehouse/default.db/table")) {
            assertEquals(PaimonOssFileIO.class, fileIO.getClass());
        }
        assertThrows(
                ClassNotFoundException.class,
                () ->
                        Class.forName(
                                "org.apache.hadoop.security.UserGroupInformation",
                                false,
                                PaimonOssFileIO.class.getClassLoader()));
    }

    @Test
    void readsSeekableOssRangesAndRejectsWrites() throws Exception {
        byte[] content = "0123456789abcdef".getBytes(StandardCharsets.UTF_8);
        AtomicInteger rangeRequests = new AtomicInteger();
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(content.length);

        OSS client =
                (OSS)
                        Proxy.newProxyInstance(
                                getClass().getClassLoader(),
                                new Class<?>[] {OSS.class},
                                (proxy, method, arguments) -> {
                                    return switch (method.getName()) {
                                        case "doesObjectExist" -> true;
                                        case "getObjectMetadata" -> metadata;
                                        case "getObject" -> {
                                            GetObjectRequest request =
                                                    (GetObjectRequest) arguments[0];
                                            long[] range = request.getRange();
                                            rangeRequests.incrementAndGet();
                                            OSSObject object = new OSSObject();
                                            object.setObjectContent(
                                                    new ByteArrayInputStream(
                                                            Arrays.copyOfRange(
                                                                    content,
                                                                    Math.toIntExact(range[0]),
                                                                    Math.toIntExact(range[1] + 1L))));
                                            yield object;
                                        }
                                        case "shutdown" -> null;
                                        default ->
                                                throw new AssertionError(
                                                        "Unexpected OSS call " + method.getName());
                                    };
                                });

        Path path = new Path("oss://test-bucket/table/snapshot-1");
        try (PaimonOssFileIO fileIO = new PaimonOssFileIO(client);
                SeekableInputStream input = fileIO.newInputStream(path)) {
            byte[] first = new byte[4];
            assertEquals(4, input.read(first));
            assertArrayEquals("0123".getBytes(StandardCharsets.UTF_8), first);

            input.seek(10L);
            byte[] second = new byte[3];
            assertEquals(3, input.read(second));
            assertArrayEquals("abc".getBytes(StandardCharsets.UTF_8), second);
            assertEquals(1, rangeRequests.get());

            assertThrows(IOException.class, () -> fileIO.newOutputStream(path, true));
        }
    }

    @Test
    void copiedSecretSurvivesNodeKeystoreClose() throws Exception {
        TestSecureSettings secureSettings = new TestSecureSettings("secret-value");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();

        SecureString copied = PaimonStorePlugin.copyOssAccessKeySecret(settings);
        secureSettings.close();

        assertThrows(
                IllegalStateException.class,
                () -> PaimonStorePlugin.OSS_ACCESS_KEY_SECRET.get(settings));
        assertEquals("secret-value", copied.toString());

        copied.close();
        assertThrows(IllegalStateException.class, copied::length);
    }

    private static final class TestSecureSettings implements SecureSettings {
        private final String value;
        private boolean loaded = true;

        private TestSecureSettings(String value) {
            this.value = value;
        }

        @Override
        public boolean isLoaded() {
            return loaded;
        }

        @Override
        public Set<String> getSettingNames() {
            ensureOpen();
            return Set.of(PaimonStorePlugin.OSS_ACCESS_KEY_SECRET.getKey());
        }

        @Override
        public SecureString getString(String setting) {
            ensureOpen();
            if (PaimonStorePlugin.OSS_ACCESS_KEY_SECRET.getKey().equals(setting) == false) {
                throw new IllegalArgumentException("Unknown secure setting " + setting);
            }
            return new SecureString(value);
        }

        @Override
        public InputStream getFile(String setting) throws GeneralSecurityException {
            throw new GeneralSecurityException("No secure files");
        }

        @Override
        public byte[] getSHA256Digest(String setting) throws GeneralSecurityException {
            throw new GeneralSecurityException("No digest in test settings");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new IOException("Test secure settings are not serializable");
        }

        @Override
        public void close() {
            loaded = false;
        }

        private void ensureOpen() {
            if (loaded == false) {
                throw new IllegalStateException("Keystore is closed");
            }
        }
    }
}
