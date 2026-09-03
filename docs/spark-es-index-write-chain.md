# Spark 写入 Paimon 全局索引 es-index：完整链路、元数据与存储格式

## 1. 文档范围

本文说明以下完整链路：

1. Spark 向 Paimon 表写入原始数据。
2. Paimon 为记录分配全局 Row-ID 并提交数据快照。
3. Spark 调用 create_global_index 分布式构建 es-index。
4. executor 将 Lucene 文件封装为湖上 archive。
5. driver 提交 IndexManifest 和新 Snapshot。
6. Elasticsearch paimon-store 插件读取元数据，将每个 archive 映射为一个只读 primary shard。

Paimon Core 和 Spark 调用链按 Apache Paimon 2.0.0 核对；es-index archive 与 ESM1 协议按 paimon-eslib writer 及本项目 reader 核对。

参考：

- [Apache Paimon 2.0 Global Index](https://paimon.apache.org/docs/2.0/multimodal-table/global-index/)
- [Apache Paimon paimon-eslib README](https://github.com/apache/paimon/blob/master/paimon-eslib/README.md)
- [ESIndexGlobalIndexWriter](https://github.com/apache/paimon/blob/master/paimon-eslib/src/main/java/org/apache/paimon/eslib/index/ESIndexGlobalIndexWriter.java)
- [GlobalIndexMeta](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/index/GlobalIndexMeta.java)
- [IndexManifestEntry](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/manifest/IndexManifestEntry.java)

## 2. 核心结论

Spark 不会通过 Elasticsearch Bulk API 写入文档。实际链路是：

~~~text
Spark INSERT
    ↓
Paimon 数据文件（默认 Parquet）+ 全局 _ROW_ID
    ↓
Paimon Snapshot S
    ↓
CALL sys.create_global_index(... index_type => 'es-index')
    ↓
Spark 分布式构建 Lucene 索引
    ↓
每个 Row-ID 区间生成一个 es-index archive
    ↓
提交 IndexManifest + 新 Snapshot S+1
    ↓
ES paimon-store 插件读取 Snapshot 和 IndexManifest
    ↓
一个 archive 映射成一个 Elasticsearch primary shard
~~~

> es-index 是存放在湖上的不可变 Lucene archive，不是普通 Elasticsearch 索引。ES mount 将 archive 以只读方式映射为 Elasticsearch shard。

## 3. 表配置要求

全局索引面向启用了 Row Tracking 和 Data Evolution 的 append table：

~~~sql
CREATE TABLE my_table_3 (
    id INT,
    embedding ARRAY<FLOAT>,
    content STRING,
    category STRING,
    price INT
) TBLPROPERTIES (
    'bucket' = '-1',
    'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true',
    'global-index.enabled' = 'true'
);
~~~

- row-tracking.enabled 为每条记录提供稳定的全局 <code>_ROW_ID</code>。
- data-evolution.enabled 是 Global Index 依赖的表模式。
- bucket=-1 是当前 Global Index 的表要求。
- global-index.enabled 控制查询侧是否启用 Global Index。

## 4. 阶段一：Spark INSERT

示例：

~~~sql
INSERT INTO my_table_3 VALUES
(1, array(0.1, 0.2, 0.3, 0.4), '高性能机械键盘', '电子产品', 599),
(2, array(0.5, 0.6, 0.7, 0.8), '无线蓝牙鼠标', '电脑配件', 129),
(3, array(0.9, 0.1, 0.2, 0.3), '4K高清显示器', '显示设备', 2499),
(4, array(0.2, 0.3, 0.4, 0.5), '人体工学办公椅', '办公家具', 899),
(5, array(0.6, 0.7, 0.8, 0.9), 'Type-C 扩展坞', '数码配件', 259);
~~~

这一步只执行 Paimon 数据写入：

1. Spark 将记录交给 Paimon writer。
2. Paimon 为每条记录分配表内全局唯一的 <code>_ROW_ID</code>。
3. 默认写入 Parquet 数据文件。
4. 数据文件元数据记录该文件覆盖的 Row-ID 范围。
5. 提交 data manifest、manifest list 和 snapshot。
6. Snapshot 中的 <code>nextRowId</code> 推进。

核查 Row-ID：

~~~sql
SELECT *, _ROW_ID
FROM oss.default.`my_table_3$row_tracking`
ORDER BY _ROW_ID;
~~~

普通 INSERT 不生成 <code>es-index-global-index-*.index</code>。若表已有旧 es-index，新增记录不会自动进入旧 archive，必须再次执行 create_global_index 补齐新的 Row-ID 范围。

## 5. 阶段二：create_global_index

~~~sql
CALL sys.create_global_index(
    table => 'default.my_table_3',
    index_column => 'embedding,content,category,price',
    index_type => 'es-index',
    options => '
      global-index.es-index.fields.embedding.algorithm=diskbbq,
      global-index.es-index.fields.embedding.dimension=4,
      global-index.es-index.fields.embedding.metric=cosine
    '
);
~~~

字段顺序有明确语义：

- 第一个字段 embedding 是 primary index field。
- content、category、price 是 companion fields。
- Paimon IndexManifest 使用稳定的 schema field ID 标识字段。
- ESM1 同时保存字段名称、类型和实际索引配置。

### 5.1 Spark 调用链

~~~text
CreateGlobalIndexProcedure
  └─ GlobalIndexTopologyBuilderUtils.createTopoBuilder("es-index")
      └─ DefaultGlobalIndexTopoBuilder.buildIndex(...)
          ├─ GlobalIndexBuilderUtils.unindexedRowRanges(...)
          ├─ GlobalIndexBuilderUtils.createShardIndexedSplits(...)
          ├─ SparkContext.parallelize(tasks)
          └─ DefaultGlobalIndexBuilder.buildIndex(...)
              ├─ 读取选定字段 + 隐藏 _ROW_ID
              ├─ 通过 GlobalIndexer SPI 查找 es-index
              ├─ ESIndexGlobalIndexWriter.write(...)
              └─ ESIndexGlobalIndexWriter.finish()
~~~

### 5.2 固定构建快照

driver 固定一个 latest snapshot，随后：

1. 扫描该 snapshot 的数据文件及 Row-ID 范围。
2. 读取相同 index type、primary field ID 和 extra field IDs 的现有 es-index。
3. 以 <code>[0, snapshot.nextRowId - 1]</code> 为候选范围。
4. 减去已经被现有 archive 覆盖的范围。
5. 将剩余区间作为本次待构建范围。

因此，重复执行 create_global_index 通常是增量补齐未覆盖范围，而不是自动重建全部旧 archive。

### 5.3 IndexedSplit 与 shard 规划

规划器综合以下信息：

- partition；
- 源数据文件 bucket；
- 数据文件 Row-ID 范围；
- global-index.row-count-per-shard；
- 已有索引的覆盖范围。

global-index.row-count-per-shard 默认目标为 100,000，目标区间是两端包含的闭区间：

~~~text
[0, 99999]
[100000, 199999]
[200000, 299999]
~~~

目标区间再与真实数据范围和未索引范围求交。最终：

> 一个 IndexedSplit 对应一个 Row-ID 闭区间，一个 Spark task 构建一个 archive。

## 6. 阶段三：executor 构建 Lucene

task 读取索引字段和隐藏字段 <code>_ROW_ID</code>，并计算：

~~~text
relativeRowId = absolutePaimonRowId - rowRangeStart
~~~

relativeRowId 被用作 archive 内部的 Lucene 文档位置。例如 archive 覆盖 <code>[100000, 199999]</code>：

~~~text
Paimon Row-ID 100000 → Lucene docID 0
Paimon Row-ID 100001 → Lucene docID 1
...
Paimon Row-ID 199999 → Lucene docID 99999
~~~

当前 builder：

- 将 <code>_ROW_ID</code> 写成 SortedNumericDocValuesField；
- 使用 <code>_ROW_ID</code> 作为 Lucene index sort；
- 禁用 compound file；
- 最后执行 forceMerge(1)；
- 索引字段全为 null 时仍写空文档占位；
- 强制 Row-ID 按非递减顺序写入。

相关代码：

- [DefaultESIndexBuilder.java](../eslib-core/src/main/java/org/elasticsearch/eslib/builder/DefaultESIndexBuilder.java)
- [ScalarFieldHandler.java](../eslib-core/src/main/java/org/elasticsearch/eslib/scalar/ScalarFieldHandler.java)

### 6.1 Row-ID 不变量

~~~text
rowRangeStart 和 rowRangeEnd 均包含

rowCount = rowRangeEnd - rowRangeStart + 1

globalPaimonRowId = rowRangeStart + luceneDocId
~~~

空字段占位文档保证 Lucene docID 不会因 null 值而发生位移。

## 7. 字段的 Lucene 存储

| Paimon 字段 | es-index 类型 | Lucene 存储 |
|---|---|---|
| embedding | VECTOR / DISKBBQ | KnnFloatVectorField 和 DiskBBQ 向量文件 |
| content | FULLTEXT | TextField，另生成 content.keyword |
| category | KEYWORD | StringField + DocValues，另生成 category.fulltext |
| price | SCALAR INT | Point 索引 + Numeric DocValues |
| _ROW_ID | 内部字段 | SortedNumeric DocValues + index sort |

注意：

- TextField 使用 Store.NO，只保存倒排结构。
- Keyword、数值和日期字段可以保存 DocValues。
- archive 不包含 Elasticsearch <code>_source</code>。
- 原始记录的权威存储仍是 Paimon 数据文件。
- 返回完整原始行需要按全局 Row-ID 回查 Paimon；当前 mount 插件没有实现自动 source hydration。

## 8. 阶段四：生成 archive

ESIndexGlobalIndexWriter 完成 Lucene 构建后：

1. 调用 builder.build。
2. 固定一次 Lucene directory 文件列表。
3. 生成 ESM1 v2 元数据。
4. 申请 archive 文件名。
5. 将全部 Lucene logical files 封装为一个文件。
6. 返回 ResultEntry。

文件名：

~~~text
es-index-global-index-<UUID>.index
~~~

ResultEntry：

~~~text
ResultEntry {
    fileName
    rowCount
    meta        // ESM1 v2
}
~~~

Writer 代码：

- [ESIndexGlobalIndexWriter.java](../build/codex-paimon-repo/paimon-eslib/src/main/java/org/apache/paimon/eslib/index/ESIndexGlobalIndexWriter.java)

## 9. archive 二进制格式

archive 使用 Java DataOutputStream，所有整数均为大端：

~~~text
int32 fileCount

repeat fileCount:
    int32 nameByteLength
    byte[nameByteLength] logicalLuceneFileNameUtf8
    int64 dataLength
    byte[dataLength] rawLuceneFileBytes
~~~

示意：

~~~text
+-------------------------+
| fileCount               | 4 bytes
+-------------------------+
| nameLength              | 4 bytes
| "segments_1"            |
| dataLength              | 8 bytes
| raw segments_1 bytes    |
+-------------------------+
| nameLength              |
| "_0.si"                 |
| dataLength              |
| raw _0.si bytes         |
+-------------------------+
| ...                     |
~~~

特点：

- archive wrapper 没有 magic、version 和 CRC；
- Lucene 文件原样保留自己的 codec header、footer 和 checksum；
- logical file 排列顺序没有业务语义；
- writer 使用 64 KiB buffer 流式复制；
- 当前 builder 禁用 compound file；
- 实际文件集合由 Lucene codec 和字段配置决定，不能硬编码扩展名。

## 10. ESM1 v2：GlobalIndexMeta._INDEX_META

archive 是顺序容器。为了让 Elasticsearch 能直接对 OSS 发起 Range GET，而不必从头扫描 archive，writer 还会生成文件 offset table，保存于 <code>GlobalIndexMeta._INDEX_META</code>。

格式同样为大端：

~~~text
int32 magic   = 0x45534D31          // ASCII "ESM1"
int32 version = 2

int32 indexedFieldCount
repeat:
    string indexedFieldName
    string indexedFieldNullableSqlType

int32 fieldConfigCount
repeat:
    string fieldName
    string indexTypeName
    nullable-string vectorAlgorithmName
    int32 dimension
    nullable-string metric
    nullable-string analyzerName
    nullable-string scalarTypeName

    int32 algorithmParamCount
    repeat:
        string parameterName
        string parameterValue

int32 logicalFileCount
repeat:
    string logicalLuceneFileName
    int64 payloadOffset
    int64 payloadLength
~~~

字符串：

~~~text
int32 utf8ByteLength
byte[utf8ByteLength] utf8Content
~~~

nullable 值：

~~~text
boolean present       // 1 byte
value                 // present=true 时存在
~~~

payloadOffset 直接指向 raw Lucene file bytes 的第一个字节，而不是 archive entry header。

当前 reader 兼容：

- legacy v0：只有文件 offset 和 length；
- v1：ESM1、字段配置和 offset；
- v2：增加有序原始字段名和 SQL 类型。

对应代码：

- [ESIndexFileMeta.java](../build/codex-paimon-repo/paimon-eslib/src/main/java/org/apache/paimon/eslib/index/ESIndexFileMeta.java)
- [ESIndexArchiveMetadata.java](../eslib-core/src/main/java/org/elasticsearch/eslib/mount/ESIndexArchiveMetadata.java)

## 11. 湖上目录布局

对表 <code>oss://cy-test2/spark/default.db/my_table_3</code>：

~~~text
my_table_3/
├─ schema/
│  └─ schema-<schemaId>
│
├─ snapshot/
│  ├─ snapshot-1
│  └─ snapshot-2
│
├─ manifest/
│  ├─ manifest-list-*
│  ├─ manifest-*
│  └─ index-manifest-<writerUUID>-<counter>
│
├─ <Paimon 数据目录>/
│  └─ data-*.parquet
│
└─ index/
   ├─ es-index-global-index-<UUID-1>.index
   ├─ es-index-global-index-<UUID-2>.index
   └─ ...
~~~

如果配置：

~~~properties
global-index.external-path=oss://another-bucket/path
~~~

则 archive 写到 external path，index-manifest 仍保存在表的 manifest 目录，IndexFileMeta._EXTERNAL_PATH 保存 archive 的完整 URI。

## 12. 元数据分层与物理格式

| 层级 | 文件或对象 | 默认物理格式 | 作用 |
|---|---|---|---|
| Table schema | schema/schema-N | JSON | 字段、稳定 field ID、table options |
| Snapshot | snapshot/snapshot-N | UTF-8 JSON | 指向 data manifests 和 index manifest，保存 nextRowId |
| Data manifest | manifest-* | Paimon FileFormat，默认 Avro + Zstd | 数据文件、统计和 Row-ID 范围 |
| Index manifest | index-manifest-* | Paimon FileFormat，默认 Avro + Zstd | 当前 snapshot 可见的 Global Index 集合 |
| es-index archive | *.index | 自定义大端二进制 | 封装 Lucene directory |
| _INDEX_META | manifest 内 BYTES | ESM1 v2 | 字段配置和 logical file offset 表 |
| Lucene files | archive payload | Lucene codec 格式 | postings、docvalues、vectors、segments |

Avro 和 Zstd 是 Paimon 2.0.0 默认值。如果表覆盖 manifest.format 或 manifest.compression，实际格式随配置变化。

## 13. IndexManifestEntry

逻辑记录结构：

~~~text
_VERSION INT = 1
_KIND TINYINT                       // ADD=0，DELETE=1
_PARTITION BYTES
_BUCKET INT
_INDEX_TYPE STRING                  // "es-index"
_FILE_NAME STRING                   // es-index-global-index-UUID.index
_FILE_SIZE BIGINT                   // 完整 archive 大小
_ROW_COUNT BIGINT                   // Lucene 文档数
_DELETIONS_VECTORS_RANGES ARRAY     // es-index 通常为 null
_EXTERNAL_PATH STRING               // 未外置时为 null

_GLOBAL_INDEX ROW {
    _ROW_RANGE_START BIGINT
    _ROW_RANGE_END BIGINT
    _INDEX_FIELD_ID INT
    _EXTRA_FIELD_IDS ARRAY<INT>
    _INDEX_META BYTES
    _SOURCE_META BYTES
}
~~~

当前 es-index：

- _INDEX_META 为 ESM1 v2；
- _SOURCE_META 为 null；
- _INDEX_FIELD_ID 是第一个索引字段的稳定 DataField ID；
- _EXTRA_FIELD_IDS 保持其余字段的调用顺序；
- _ROW_RANGE_START 和 _ROW_RANGE_END 均包含；
- _ROW_COUNT 等于区间长度；
- _FILE_SIZE 是整个 archive wrapper 的字节数。

虽然 schema 支持 ADD 和 DELETE，但 snapshot 指向的 index manifest 是当前有效索引集合的物化清单。正常已提交 manifest 只保留 ADD；DELETE 是提交合并阶段移除旧 entry 的输入语义。

## 14. 阶段五：driver 提交元数据

executor 结果转换链：

~~~text
ResultEntry
    ↓
GlobalIndexMeta
    ↓
IndexFileMeta
    ↓
DataIncrement.indexIncrement(...)
    ↓
CommitMessageImpl
~~~

Paimon 2.0.0 的 Global Index index-only commit 将 bucket 字段固定为 0。它是 Paimon commit 元数据，不是 Elasticsearch shard ID。

driver 收集结果后调用 TableCommitImpl.commit：

1. executor 已将 archive 写入最终 UUID 路径。
2. driver 合并前一 snapshot 的 live entries 和本轮变更。
3. 写新的 index-manifest。
4. 写新的 snapshot。
5. snapshot.indexManifest 指向新 manifest。
6. snapshot commit 成功后，整批 archive 同时可见。

archive 在 metadata commit 前已经写入。若任务写 archive 成功而最终 snapshot commit 失败，该文件不会被 snapshot 引用，属于 orphan file，需要后续清理。

## 15. 阶段六：Elasticsearch mount

~~~text
POST /_paimon/mount
    ↓
PaimonSnapshotPlanner
    ├─ 打开 Paimon table
    ├─ 选择 snapshot
    ├─ 读取 snapshot.indexManifest
    ├─ scan(snapshot, "es-index")
    ├─ 解析 GlobalIndexMeta
    └─ 解析 ESM1
    ↓
PaimonMountPlan
    ├─ 按 rowRangeStart 排序
    ├─ 校验范围不重叠
    ├─ 校验字段布局一致
    └─ 每个 archive 分配一个 shard ordinal
    ↓
ArchiveDirectory
    └─ logical Lucene file → archive offset/length
    ↓
OSS Range GET 或本地 positional read
    ↓
PaimonReadOnlyEngineFactory
~~~

代码：

- [PaimonSnapshotPlanner.java](../paimon-store/src/main/java/org/elasticsearch/paimon/PaimonSnapshotPlanner.java)
- [PaimonMountPlan.java](../eslib-core/src/main/java/org/elasticsearch/eslib/mount/PaimonMountPlan.java)
- [ArchiveDirectory.java](../eslib-core/src/main/java/org/elasticsearch/eslib/io/ArchiveDirectory.java)
- [RestPaimonMountAction.java](../paimon-store/src/main/java/org/elasticsearch/paimon/RestPaimonMountAction.java)
- [PaimonReadOnlyEngineFactory.java](../paimon-store/src/main/java/org/elasticsearch/paimon/PaimonReadOnlyEngineFactory.java)

映射关系：

~~~text
一个 live es-index archive
        =
一个 Elasticsearch primary shard
~~~

因此 ES shard 数量等于目标 snapshot 中有效 archive 数量，而不是 Paimon bucket 数量。

物理索引名：

~~~text
<alias>_<snapshotId>
~~~

例如 alias 为 my_table_3、snapshot 为 2，则物理索引为 my_table_3_2。

## 16. my_table_3 示例

假设五条记录首次写入后的 Row-ID 为：

~~~text
0, 1, 2, 3, 4
~~~

通常只产生一个 archive：

~~~text
file:
  es-index-global-index-<UUID>.index

row range:
  [0, 4]

row count:
  5

Lucene docID:
  0, 1, 2, 3, 4
~~~

如果初始 schema 顺序为：

~~~text
id, embedding, content, category, price
~~~

且没有发生字段删除后重新添加等 schema evolution，则通常：

~~~text
_INDEX_FIELD_ID  = 1
_EXTRA_FIELD_IDS = [2, 3, 4]
~~~

生产代码必须使用 Paimon stable field ID，不能将当前字段位置直接当作 ID。

## 17. 核查 SQL

### 17.1 原始行与 Row-ID

~~~sql
SELECT *, _ROW_ID
FROM oss.default.`my_table_3$row_tracking`
ORDER BY _ROW_ID;
~~~

### 17.2 Snapshot 链

~~~sql
SELECT *
FROM oss.default.`my_table_3$snapshots`
ORDER BY snapshot_id;
~~~

### 17.3 es-index 文件和范围

~~~sql
SELECT
    index_type,
    index_field_name,
    file_name,
    file_size,
    row_count,
    row_range_start,
    row_range_end
FROM oss.default.`my_table_3$table_indexes`
WHERE index_type = 'es-index'
ORDER BY row_range_start;
~~~

已有核查脚本：

- [inspect-paimon.sql](../deploy/k8s/inspect-paimon.sql)

### 17.4 Elasticsearch

~~~bash
curl 'http://127.0.0.1:9200/_cat/aliases?v'
curl 'http://127.0.0.1:9200/_cat/shards/my_table_3_2?v'
curl 'http://127.0.0.1:9200/my_table_3/_mapping?pretty'
~~~

## 18. 校验规则

每条 entry：

~~~text
row_count = row_range_end - row_range_start + 1
~~~

不同 archive：

~~~text
允许 Row-ID range 存在 gap
禁止 Row-ID range overlap
禁止 archive URI 重复
要求索引字段布局一致
~~~

挂载后：

~~~text
ES primary shard 数量 = live es-index entry 数量
~~~

ESM1 reader 还会检查：

- count、字符串长度和元数据总长度；
- logical file 名称是否重复；
- offset 和 length 是否为负或溢出；
- logical file 末尾是否超过 archive 长度；
- 是否存在未解析的 trailing bytes。

## 19. 生命周期与边界

### 19.1 INSERT 不自动刷新

新增记录后：

- 旧 archive 保持不可变；
- 旧 IndexManifest 只覆盖原 Row-ID；
- 新记录在重新构建前不可通过旧 es-index 搜索；
- 应再次执行 create_global_index 补齐未覆盖区间。

### 19.2 Snapshot 决定可见性

archive 物理存在不等于可见。只有被目标 snapshot 的 indexManifest 引用，才是该 snapshot 的有效 Global Index。

### 19.3 删除和重建

drop_global_index 会从新 IndexManifest 移除 entry，但旧 snapshot 在过期前仍可能引用旧 archive，因此物理文件不能立即删除。

删除 Elasticsearch 物理索引只删除 ES cluster state 中的挂载结果，不会删除 Paimon 湖上的 archive。

### 19.4 Source 与索引分离

es-index 保存：

- postings；
- terms；
- points；
- doc values；
- vector index。

它不保存完整 Paimon 行，也不提供 Elasticsearch <code>_source</code>。完整记录仍应从 Paimon 数据文件读取。

### 19.5 校验和边界

- archive wrapper：没有 CRC。
- ESM1：没有 CRC。
- raw Lucene files：保留 Lucene 自身 checksum。
- ES mount 后生成的 PMS1 ShardMountSpec：带 CRC32，但属于 ES cluster state 派生配置，不属于 Spark/Paimon 湖上格式。

### 19.6 Lucene 版本兼容

当前链路由 Spark 侧 Lucene 9 writer 生成 archive，再由 Elasticsearch 9.4/Lucene 10 插件读取。升级时必须保证：

- Lucene 文件格式可由目标 reader 识别；
- 自定义 DiskBBQ codec 的 writer/reader 完全一致；
- archive wrapper 和 ESM1 version 兼容；
- 执行真实的 Lucene 9 archive → OSS → Elasticsearch 9.4 mount → REST 查询回归。

## 20. 总体设计摘要

系统分为四层：

~~~text
Paimon 数据层
  Parquet + 全局 Row-ID

Paimon 元数据层
  Schema + Snapshot + Data Manifest + Index Manifest

es-index 搜索层
  ESM1 metadata + Lucene archive

Elasticsearch 挂载层
  Snapshot → archive → read-only primary shard
~~~

最关键的映射关系：

~~~text
Paimon global Row-ID
    =
archive.rowRangeStart + Lucene docID
~~~

该关系使 Elasticsearch 能直接搜索湖上的 Lucene archive，同时仍可将命中位置稳定映射回 Paimon 原始数据。
