/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V.
 * Licensed under the Elastic License 2.0.
 */
package org.elasticsearch.paimon;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SwitchableMountDirectoryTest {

    @Test
    void exposesRemoteSegmentsWithBootstrapCommitMetadata() throws Exception {
        Directory local = new ByteBuffersDirectory();
        Directory lake = new ByteBuffersDirectory();
        Map<String, String> bootstrapData =
                Map.of(
                        "local_checkpoint", "-1",
                        "max_seq_no", "-1",
                        "history_uuid", "history-1",
                        "translog_uuid", "translog-1",
                        "es_version", "9004000");
        writeIndex(local, 0, bootstrapData);
        writeIndex(lake, 3, Map.of());

        try (SwitchableMountDirectory mounted = new SwitchableMountDirectory(local, lake)) {
            try (DirectoryReader reader = DirectoryReader.open(mounted)) {
                assertEquals(0, reader.numDocs());
            }
            mounted.activateLakeIndex();
            assertTrue(mounted.isLakeIndexActive());
            try (DirectoryReader reader = DirectoryReader.open(mounted)) {
                assertEquals(3, reader.numDocs());
            }
            assertEquals(
                    bootstrapData,
                    SegmentInfos.readLatestCommit(mounted).getUserData());
            assertThrows(
                    IOException.class,
                    () -> mounted.createOutput("forbidden", IOContext.DEFAULT));
        }
    }

    @Test
    void canReactivateAfterNodeRestartWithoutCommitNameCollision() throws Exception {
        Directory local = new ByteBuffersDirectory();
        Directory lake = new ByteBuffersDirectory();
        try {
            writeIndex(local, 0, Map.of("history_uuid", "history-restart"));
            writeIndex(lake, 2, Map.of());
            mountAndAssertDocumentCount(local, lake, 2);
            mountAndAssertDocumentCount(local, lake, 2);
        } finally {
            local.close();
            lake.close();
        }
    }

    private static void mountAndAssertDocumentCount(
            Directory local, Directory lake, int expectedDocuments) throws Exception {
        try (SwitchableMountDirectory mounted =
                new SwitchableMountDirectory(
                        new NonClosingDirectory(local), new NonClosingDirectory(lake))) {
            mounted.activateLakeIndex();
            try (DirectoryReader reader = DirectoryReader.open(mounted)) {
                assertEquals(expectedDocuments, reader.numDocs());
            }
        }
    }

    private static final class NonClosingDirectory extends FilterDirectory {
        private NonClosingDirectory(Directory delegate) {
            super(delegate);
        }

        @Override
        public void close() {}
    }

    private static void writeIndex(
            Directory directory, int documentCount, Map<String, String> commitData)
            throws IOException {
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        config.setCommitOnClose(false);
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 0; i < documentCount; i++) {
                Document document = new Document();
                document.add(new StringField("value", "v" + i, Field.Store.NO));
                writer.addDocument(document);
            }
            writer.setLiveCommitData(commitData.entrySet());
            writer.commit();
        }
    }
}
