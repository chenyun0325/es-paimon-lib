/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package org.elasticsearch.paimon;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PaimonRowLoaderTest {

    @Test
    void readsOneRowFromThePinnedSnapshotByGlobalRowId(@TempDir Path tempDir)
            throws Exception {
        Path tableDir = tempDir.resolve("table");
        Schema schema =
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.INT())
                        .column("content", org.apache.paimon.types.DataTypes.STRING())
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        // Avro keeps this focused on Row-ID/snapshot hydration. The production
                        // bundle's Parquet reader is covered by Paimon's own format tests.
                        .option(CoreOptions.FILE_FORMAT.key(), "avro")
                        .build();

        Options options = new Options();
        options.set("path", tableDir.toString());
        try (FileIO fileIO = LocalFileIO.create()) {
            fileIO.configure(CatalogContext.create(options));
            new SchemaManager(fileIO, new org.apache.paimon.fs.Path(tableDir.toString()))
                    .createTable(schema);
            FileStoreTable table = FileStoreTableFactory.create(fileIO, options);
            write(
                    table,
                    GenericRow.of(1, BinaryString.fromString("first")),
                    GenericRow.of(2, BinaryString.fromString("second")));
        }

        Settings settings =
                Settings.builder()
                        .put("path.home", tempDir.resolve("es-home"))
                        .putList("path.repo", tempDir.toString())
                        .build();
        try (PaimonRowLoader loader =
                new PaimonRowLoader(settings, null, new Environment(settings, null))) {
            assertEquals(
                    Map.of("id", 2, "content", "second"),
                    loader.load(tableDir.toString(), 1L, 1L, List.of()));
            assertEquals(
                    Map.of("content", "first"),
                    loader.load(tableDir.toString(), 1L, 0L, List.of("content")));
            assertThrows(
                    java.io.IOException.class,
                    () -> loader.load(tableDir.toString(), 1L, 2L, List.of()));
        }
    }

    private static void write(FileStoreTable table, GenericRow... rows) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (GenericRow row : rows) {
                write.write(row);
            }
            commit.commit(write.prepareCommit());
        }
    }
}
