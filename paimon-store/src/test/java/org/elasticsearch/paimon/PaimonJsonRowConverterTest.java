/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package org.elasticsearch.paimon;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PaimonJsonRowConverterTest {

    @Test
    void convertsCompleteRowToDetachedJsonValues() {
        RowType nestedType =
                DataTypes.ROW(
                        DataTypes.FIELD(20, "active", DataTypes.BOOLEAN()),
                        DataTypes.FIELD(21, "score", DataTypes.DOUBLE()));
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(1, "content", DataTypes.STRING()),
                        DataTypes.FIELD(2, "embedding", DataTypes.VECTOR(3, DataTypes.FLOAT())),
                        DataTypes.FIELD(3, "tags", DataTypes.ARRAY(DataTypes.STRING())),
                        DataTypes.FIELD(
                                4,
                                "attributes",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                        DataTypes.FIELD(5, "nested", nestedType),
                        DataTypes.FIELD(6, "created", DataTypes.TIMESTAMP(6)),
                        DataTypes.FIELD(7, "amount", DataTypes.DECIMAL(8, 2)),
                        DataTypes.FIELD(8, "payload", DataTypes.BYTES()));

        Map<BinaryString, Integer> attributes = new LinkedHashMap<>();
        attributes.put(BinaryString.fromString("rank"), 9);
        byte[] payload = new byte[] {1, 2, 3};
        GenericRow row =
                GenericRow.of(
                        7,
                        BinaryString.fromString("hello"),
                        BinaryVector.fromPrimitiveArray(new float[] {1.0f, 2.5f, 3.0f}),
                        new GenericArray(
                                new Object[] {
                                    BinaryString.fromString("red"),
                                    BinaryString.fromString("blue")
                                }),
                        new GenericMap(attributes),
                        GenericRow.of(true, 4.5d),
                        Timestamp.fromLocalDateTime(
                                LocalDateTime.of(2026, 9, 4, 12, 30, 15, 123_000_000)),
                        Decimal.fromBigDecimal(new BigDecimal("12.30"), 8, 2),
                        payload);

        Map<String, Object> converted =
                PaimonJsonRowConverter.convert(row, rowType, List.of());

        assertEquals(7, converted.get("id"));
        assertEquals("hello", converted.get("content"));
        assertEquals(List.of(1.0f, 2.5f, 3.0f), converted.get("embedding"));
        assertEquals(List.of("red", "blue"), converted.get("tags"));
        assertEquals(Map.of("rank", 9), converted.get("attributes"));
        assertEquals(Map.of("active", true, "score", 4.5d), converted.get("nested"));
        assertEquals("2026-09-04T12:30:15.123", converted.get("created"));
        assertEquals(new BigDecimal("12.30"), converted.get("amount"));
        assertArrayEquals(payload, (byte[]) converted.get("payload"));

        payload[0] = 99;
        assertArrayEquals(new byte[] {1, 2, 3}, (byte[]) converted.get("payload"));
    }

    @Test
    void returnsOnlyConfiguredTopLevelFields() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(1, "content", DataTypes.STRING()));
        GenericRow row = GenericRow.of(7, BinaryString.fromString("hello"));

        assertEquals(
                Map.of("content", "hello"),
                PaimonJsonRowConverter.convert(row, rowType, List.of("content")));
    }
}
