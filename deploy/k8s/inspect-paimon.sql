-- Paimon table directory: default.db/my_table_3.
-- Spark catalog namespace uses default (the .db suffix belongs only to the warehouse path).
-- Copy the exact table Location from DESCRIBE TABLE EXTENDED into configmap.yaml mount.json.

SHOW NAMESPACES IN oss;
SHOW TABLES IN oss.default;

SHOW CREATE TABLE oss.default.my_table_3;
DESCRIBE TABLE EXTENDED oss.default.my_table_3;

-- Mount latest by omitting snapshot_id, or copy one of these snapshot IDs into mount.json.
SELECT *
FROM oss.default.`my_table_3$snapshots`
ORDER BY snapshot_id DESC
LIMIT 20;

-- Confirms schema evolution and the table options used by the ESLib writer.
SELECT *
FROM oss.default.`my_table_3$schemas`;

SELECT *
FROM oss.default.`my_table_3$options`
WHERE key LIKE '%index%'
   OR key LIKE '%row%'
   OR key LIKE '%bucket%';

-- This is the decisive mount pre-check. Each live es-index row becomes one ES primary shard.
-- index_field_name can be copied to mount.json.vector_field_name when selection is needed.
SELECT index_type,
       index_field_name,
       file_name,
       file_size,
       row_count,
       row_range_start,
       row_range_end
FROM oss.default.`my_table_3$table_indexes`
WHERE index_type = 'es-index'
ORDER BY row_range_start;
