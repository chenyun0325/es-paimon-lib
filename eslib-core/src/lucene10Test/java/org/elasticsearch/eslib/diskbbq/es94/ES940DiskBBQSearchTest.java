/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V.
 * Licensed under the Elastic License 2.0.
 */
package org.elasticsearch.eslib.diskbbq.es94;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.VectorUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ES940DiskBBQSearchTest {

    @Test
    void approximateSearchEntryPointScansRawVectors() throws Exception {
        float[][] vectors = {
            { 0.1f, 0.2f, 0.3f, 0.4f },
            { 0.5f, 0.6f, 0.7f, 0.8f },
            { 0.9f, 0.1f, 0.2f, 0.3f },
            { 0.2f, 0.3f, 0.4f, 0.5f },
            { 0.6f, 0.7f, 0.8f, 0.9f }
        };

        try (Directory directory = new ByteBuffersDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig(new KeywordAnalyzer());
            config.setCodec(new ES940TestCodec());
            config.setUseCompoundFile(false);
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                for (float[] vector : vectors) {
                    Document document = new Document();
                    document.add(
                            new KnnFloatVectorField(
                                    "embedding",
                                    VectorUtil.l2normalize(vector.clone()),
                                    VectorSimilarityFunction.DOT_PRODUCT));
                    writer.addDocument(document);
                }
                writer.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                TopKnnCollector collector = new TopKnnCollector(3, Integer.MAX_VALUE);
                leaf.searchNearestVectors(
                        "embedding",
                        VectorUtil.l2normalize(vectors[1].clone()),
                        collector,
                        AcceptDocs.fromLiveDocs(leaf.getLiveDocs(), leaf.maxDoc()));

                TopDocs hits = collector.topDocs();
                assertEquals(5, hits.totalHits.value());
                assertEquals(3, hits.scoreDocs.length);
                assertEquals(
                        1,
                        hits.scoreDocs[0].doc,
                        "the exact query vector must rank its source document first");
                assertEquals(1.0f, hits.scoreDocs[0].score, 0.001f);
            }
        }
    }

    @Test
    void sparseVectorOrdinalsRespectAcceptedDocumentIds() throws Exception {
        float[] first = VectorUtil.l2normalize(new float[] { 1.0f, 2.0f, 3.0f, 4.0f });
        float[] target = VectorUtil.l2normalize(new float[] { 5.0f, 6.0f, 7.0f, 8.0f });

        try (Directory directory = new ByteBuffersDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig(new KeywordAnalyzer());
            config.setCodec(new ES940TestCodec());
            config.setUseCompoundFile(false);
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                writer.addDocument(new Document()); // doc 0: no vector
                writer.addDocument(vectorDocument(first)); // vector ord 0 -> doc 1
                writer.addDocument(new Document()); // doc 2: no vector
                writer.addDocument(vectorDocument(target)); // vector ord 1 -> doc 3
                writer.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                FixedBitSet acceptedDocs = new FixedBitSet(leaf.maxDoc());
                acceptedDocs.set(3);
                TopKnnCollector collector = new TopKnnCollector(2, Integer.MAX_VALUE);
                leaf.searchNearestVectors(
                        "embedding",
                        target,
                        collector,
                        AcceptDocs.fromLiveDocs(acceptedDocs, leaf.maxDoc()));

                TopDocs hits = collector.topDocs();
                assertEquals(1, hits.totalHits.value());
                assertEquals(1, hits.scoreDocs.length);
                assertEquals(3, hits.scoreDocs[0].doc);
            }
        }
    }

    private static Document vectorDocument(float[] vector) {
        Document document = new Document();
        document.add(
                new KnnFloatVectorField(
                        "embedding", vector, VectorSimilarityFunction.DOT_PRODUCT));
        return document;
    }

    /**
     * Writes a segment whose outer codec name is a registered production SPI name while forcing
     * the per-field format under test to be ES940. Reopening then exercises the same SPI read path
     * as a Lucene 9 lake segment mounted by Elasticsearch/Lucene 10.
     */
    private static final class ES940TestCodec extends FilterCodec {

        private final KnnVectorsFormat knnFormat =
                new PerFieldKnnVectorsFormat() {
                    @Override
                    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                        return new ES940DiskBBQVectorsFormat(64, 16);
                    }
                };

        private ES940TestCodec() {
            super("PaimonLucene10", new Lucene104Codec());
        }

        @Override
        public KnnVectorsFormat knnVectorsFormat() {
            return knnFormat;
        }
    }
}
