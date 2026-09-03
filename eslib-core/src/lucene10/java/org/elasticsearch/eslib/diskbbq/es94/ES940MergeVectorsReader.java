/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V.
 * Licensed under the Elastic License 2.0.
 */
package org.elasticsearch.eslib.diskbbq.es94;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

import java.io.IOException;

/**
 * Reader for the standalone ES940 DiskBBQ format.
 *
 * <p>Lucene opens every source segment's {@link KnnVectorsReader} while merging. ES940's writer
 * ({@link IVFVectorsWriter#mergeOneFieldIVF}) rebuilds the IVF index from the raw vectors, so raw
 * vector access is delegated to the underlying flat reader.
 *
 * <p>The standalone library does not yet contain Elasticsearch's optimized DiskBBQ search
 * implementation. Mounted indexes must nevertheless remain searchable, so searches exhaustively
 * score the raw vectors exposed by the flat reader. This keeps the on-disk format and merge
 * behavior compatible while providing correct results; only query performance differs from the
 * optimized IVF search.
 */
final class ES940MergeVectorsReader extends KnnVectorsReader {

    private static final int SCORE_BATCH_SIZE = 64;

    private final FlatVectorsReader rawVectorsReader;

    ES940MergeVectorsReader(FlatVectorsReader rawVectorsReader) {
        this.rawVectorsReader = rawVectorsReader;
    }

    @Override
    public void checkIntegrity() throws IOException {
        rawVectorsReader.checkIntegrity();
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
        return rawVectorsReader.getFloatVectorValues(field);
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
        return rawVectorsReader.getByteVectorValues(field);
    }

    @Override
    public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
            throws IOException {
        exactSearch(rawVectorsReader.getRandomVectorScorer(field, target), knnCollector, acceptDocs);
    }

    @Override
    public void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
            throws IOException {
        exactSearch(rawVectorsReader.getRandomVectorScorer(field, target), knnCollector, acceptDocs);
    }

    private static void exactSearch(
            RandomVectorScorer scorer, KnnCollector knnCollector, AcceptDocs acceptDocs)
            throws IOException {
        if (scorer == null) {
            return;
        }

        Bits acceptedOrds =
                scorer.getAcceptOrds(acceptDocs == null ? null : acceptDocs.bits());
        int[] ords = new int[SCORE_BATCH_SIZE];
        float[] scores = new float[SCORE_BATCH_SIZE];
        int batchSize = 0;
        for (int ord = 0; ord < scorer.maxOrd(); ord++) {
            if (acceptedOrds != null && acceptedOrds.get(ord) == false) {
                continue;
            }
            if (knnCollector.earlyTerminated()) {
                break;
            }
            ords[batchSize++] = ord;
            if (batchSize == SCORE_BATCH_SIZE) {
                collectBatch(scorer, knnCollector, ords, scores, batchSize);
                batchSize = 0;
            }
        }
        if (batchSize > 0) {
            collectBatch(scorer, knnCollector, ords, scores, batchSize);
        }
    }

    private static void collectBatch(
            RandomVectorScorer scorer,
            KnnCollector knnCollector,
            int[] ords,
            float[] scores,
            int batchSize)
            throws IOException {
        knnCollector.incVisitedCount(batchSize);
        if (scorer.bulkScore(ords, scores, batchSize)
                > knnCollector.minCompetitiveSimilarity()) {
            for (int i = 0; i < batchSize; i++) {
                knnCollector.collect(scorer.ordToDoc(ords[i]), scores[i]);
            }
        }
    }

    @Override
    public void close() throws IOException {
        rawVectorsReader.close();
    }
}
