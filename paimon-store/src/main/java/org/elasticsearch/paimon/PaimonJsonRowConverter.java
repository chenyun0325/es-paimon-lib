/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package org.elasticsearch.paimon;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.JsonSerdeUtil;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Converts reusable Paimon internal values into detached JSON-compatible Java values. */
final class PaimonJsonRowConverter {

    private PaimonJsonRowConverter() {}

    static Map<String, Object> convert(
            InternalRow row, RowType rowType, List<String> returnFields) {
        Set<String> selected = returnFields.isEmpty() ? null : Set.copyOf(returnFields);
        Map<String, Object> result = new LinkedHashMap<>();
        List<DataField> fields = rowType.getFields();
        for (int position = 0; position < fields.size(); position++) {
            DataField field = fields.get(position);
            if (selected != null && selected.contains(field.name()) == false) {
                continue;
            }
            Object value = InternalRow.createFieldGetter(field.type(), position).getFieldOrNull(row);
            result.put(field.name(), convertValue(value, field.type()));
        }
        return result;
    }

    private static Object convertValue(Object value, DataType type) {
        if (value == null) {
            return null;
        }
        return switch (type.getTypeRoot()) {
            case CHAR, VARCHAR -> ((BinaryString) value).toString();
            case BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE -> value;
            case BINARY, VARBINARY -> {
                byte[] bytes = (byte[]) value;
                yield Arrays.copyOf(bytes, bytes.length);
            }
            case DECIMAL -> ((Decimal) value).toBigDecimal();
            case DATE -> LocalDate.ofEpochDay(((Number) value).longValue()).toString();
            case TIME_WITHOUT_TIME_ZONE ->
                    LocalTime.ofNanoOfDay(((Number) value).longValue() * 1_000_000L).toString();
            case TIMESTAMP_WITHOUT_TIME_ZONE -> ((Timestamp) value).toLocalDateTime().toString();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE -> {
                Timestamp timestamp = (Timestamp) value;
                yield Instant.ofEpochMilli(timestamp.getMillisecond())
                        .plusNanos(timestamp.getNanoOfMillisecond())
                        .toString();
            }
            case ARRAY -> convertArray((InternalArray) value, ((ArrayType) type).getElementType());
            case VECTOR ->
                    convertVector((InternalVector) value, ((VectorType) type).getElementType());
            case MAP -> convertMap((InternalMap) value, (MapType) type);
            case MULTISET -> convertMultiset((InternalMap) value, (MultisetType) type);
            case ROW -> convert((InternalRow) value, (RowType) type, List.of());
            case VARIANT -> convertVariant((Variant) value);
            case BLOB -> {
                byte[] bytes = ((Blob) value).toData();
                yield Arrays.copyOf(bytes, bytes.length);
            }
        };
    }

    private static List<Object> convertArray(InternalArray array, DataType elementType) {
        InternalArray.ElementGetter getter = InternalArray.createElementGetter(elementType);
        List<Object> result = new ArrayList<>(array.size());
        for (int position = 0; position < array.size(); position++) {
            result.add(convertValue(getter.getElementOrNull(array, position), elementType));
        }
        return result;
    }

    private static List<Object> convertVector(InternalVector vector, DataType elementType) {
        InternalArray.ElementGetter getter = InternalVector.createElementGetter(elementType);
        List<Object> result = new ArrayList<>(vector.size());
        for (int position = 0; position < vector.size(); position++) {
            result.add(convertValue(getter.getElementOrNull(vector, position), elementType));
        }
        return result;
    }

    private static Map<String, Object> convertMap(InternalMap map, MapType type) {
        return convertMap(
                map,
                type.getKeyType(),
                type.getValueType());
    }

    private static Map<String, Object> convertMultiset(InternalMap map, MultisetType type) {
        return convertMap(map, type.getElementType(), org.apache.paimon.types.DataTypes.INT());
    }

    private static Map<String, Object> convertMap(
            InternalMap map, DataType keyType, DataType valueType) {
        InternalArray keys = map.keyArray();
        InternalArray values = map.valueArray();
        InternalArray.ElementGetter keyGetter = InternalArray.createElementGetter(keyType);
        InternalArray.ElementGetter valueGetter = InternalArray.createElementGetter(valueType);
        Map<String, Object> result = new LinkedHashMap<>();
        for (int position = 0; position < map.size(); position++) {
            Object key = convertValue(keyGetter.getElementOrNull(keys, position), keyType);
            Object converted =
                    convertValue(valueGetter.getElementOrNull(values, position), valueType);
            result.put(String.valueOf(key), converted);
        }
        return result;
    }

    private static Object convertVariant(Variant variant) {
        return JsonSerdeUtil.fromJson(variant.toJson(), Object.class);
    }
}
