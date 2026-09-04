# Paimon es-index 全局索引详细设计

## 1. 范围与定位

本文描述当前项目中 Paimon <code>es-index</code> Global Index 的详细设计，覆盖：

- Spark/Paimon 构建端；
- Paimon Snapshot 和 IndexManifest；
- Lucene archive 与 ESM1 元数据；
- Elasticsearch 9.4 mount；
- OSS Range GET 查询；
- Row-ID、Lucene docID 和 ES shard 的映射；
- 增量追加、失败处理和性能边界；
- 基于 my_table_3 的完整实例。

Paimon Core 与 Spark 调用链按 Apache Paimon 2.0.0 核对；archive 协议按 paimon-eslib writer 和本项目 reader 核对。

## 2. Paimon 索引体系中的位置

Paimon 中存在几类生命周期和寻址方式不同的索引，不能混为一谈。

~~~mermaid
flowchart TB
    ROOT["Paimon 索引体系"]

    ROOT --> FI["File Index<br/>每个数据文件关联索引"]
    ROOT --> PK["Primary-Key Index<br/>随 bucket / compaction 生命周期维护"]
    ROOT --> GI["Data Evolution Global Index<br/>按表级全局 Row-ID 寻址"]

    FI --> FI1["数据跳过、Bloom、Bitmap 等"]
    PK --> PK1["source-backed<br/>物理文件位置"]
    GI --> GI1["BTree / Bitmap / FM"]
    GI --> GI2["Vector / Full Text"]
    GI --> ESI["es-index<br/>多字段 Lucene archive"]

    ESI --> MOUNT["本项目：archive → ES primary shard"]
~~~

当前项目只挂载 Data Evolution append table 上的 <code>es-index</code>：

- 它按表级全局 <code>_ROW_ID</code> 寻址；
- 它独立于普通 INSERT 构建；
- 它不随 Elasticsearch 写入生命周期维护；
- 它与 Primary-Key 表随 compaction 维护的 source-backed index 不同。

官方说明：

- [Paimon 2.0 Global Index](https://paimon.apache.org/docs/2.0/multimodal-table/global-index/)
- [Paimon Primary-Key Indexes](https://paimon.apache.org/docs/2.0/primary-key-table/global-index/)
- [Paimon File Index](https://paimon.apache.org/docs/master/concepts/spec/fileindex/)

## 3. 设计目标

### 3.1 目标

1. **湖上单一事实来源**
   - 原始记录由 Paimon Snapshot 和数据文件管理。
   - Lucene 只保存搜索结构。

2. **不复制索引到 Elasticsearch**
   - archive 保留在 OSS 或共享文件系统。
   - ES 通过随机范围读取直接打开 Lucene 文件。

3. **Snapshot 级一致性**
   - 只有被 snapshot.indexManifest 引用的 archive 才可见。
   - 数据与索引均按不可变对象发布。

4. **稳定的行级映射**
   - 每个 archive 覆盖一个 Paimon Row-ID 闭区间。
   - Lucene docID 可确定性映射回 Paimon global Row-ID。

5. **ES 原生查询兼容**
   - 支持 dense_vector、text、keyword、数值、日期和 geo_point mapping。
   - 让 ES 查询直接作用于湖上的 Lucene segment。

6. **对象存储友好**
   - 将 Lucene directory 的多个文件封装成单个 archive，减少对象数量。
   - ESM1 保存 offset table，避免挂载时扫描整个 archive。

### 3.2 当前非目标

- 不允许向 mounted index 写入、更新或删除文档；
- 不提供 Elasticsearch <code>_source</code>；
- 不自动从 Paimon 回填完整原始行；
- 普通 INSERT 不自动刷新旧 es-index；
- mounted index 当前固定零副本；
- 当前 DiskBBQ reader 优先保证正确性，尚未实现完整 IVF 查询性能。

## 4. 总体架构

~~~mermaid
flowchart LR
    subgraph BUILD["构建平面：Spark / JDK 11 / Lucene 9"]
        SQL["Spark SQL<br/>INSERT / create_global_index"]
        PROC["Paimon Spark Procedure"]
        SPLIT["IndexedSplit Planner"]
        TASK["Spark Executor Tasks"]
        SPI["GlobalIndexer SPI<br/>es-index"]
        L9["ESLib Lucene 9 Builder"]

        SQL --> PROC
        PROC --> SPLIT
        SPLIT --> TASK
        TASK --> SPI
        SPI --> L9
    end

    subgraph LAKE["湖上持久化平面"]
        DATA["Paimon data files"]
        SNAP["snapshot-N"]
        IM["index-manifest-*"]
        ARC["es-index-global-index-UUID.index"]

        DATA --> SNAP
        SNAP --> IM
        IM --> ARC
    end

    subgraph SERVE["服务平面：Elasticsearch 9.4 / JDK 21 / Lucene 10"]
        REST["POST /_paimon/mount"]
        PLAN["PaimonSnapshotPlanner"]
        MAP["Mapping + ShardMountSpec"]
        STORE["ArchiveDirectory"]
        ENGINE["ReadOnlyEngine"]
        QUERY["ES Search / KNN"]

        REST --> PLAN
        PLAN --> MAP
        MAP --> STORE
        STORE --> ENGINE
        QUERY --> ENGINE
    end

    SQL --> DATA
    L9 --> ARC
    PROC --> SNAP
    PLAN --> SNAP
    STORE -->|"OSS Range GET"| ARC
~~~

架构有两个 Lucene 运行时：

| 运行侧 | 构建配置 | 职责 |
|---|---|---|
| Spark/Paimon | Lucene 9.12、JDK 11 | 构建 Lucene segment 和 archive |
| Elasticsearch 9.4 | Lucene 10.4、JDK 21 | 直接读取 archive 并执行查询 |

因此自定义 codec、DiskBBQ 文件结构、量化算法和版本常量必须保持跨版本字节兼容。

## 5. 模块职责

| 模块 | 关键组件 | 职责 |
|---|---|---|
| paimon-spark | CreateGlobalIndexProcedure | 接收 SQL 调用并启动全局索引构建 |
| paimon-core | GlobalIndexBuilderUtils | 计算未覆盖 Row-ID、规划 IndexedSplit、生成 IndexFileMeta |
| paimon-eslib | ESIndexGlobalIndexer / Writer | 将 Paimon 行转换为 Lucene 文档并生成 archive |
| eslib-core Lucene 9 | DefaultESIndexBuilder | 构建全文、标量、向量和 DocValues |
| Paimon metadata | Snapshot / IndexManifest | 控制 archive 的快照可见性和生命周期 |
| paimon-store | RestPaimonMountAction | 创建快照专属 ES 物理索引并切换 alias |
| paimon-store | PaimonSnapshotPlanner | 读取 Paimon Snapshot 和 live es-index entries |
| eslib-core | ESIndexArchiveMetadata | 解析 ESM1 并校验 logical file ranges |
| eslib-core | PaimonMountPlan | archive 排序、范围校验、分配 ES shard ID |
| paimon-store | ShardMountSpec | 将 archive 描述安全保存到 ES index settings |
| eslib-core | ArchiveDirectory | 将 archive 内 byte range 暴露为 Lucene Directory |
| paimon-store | SwitchableMountDirectory | 合并本地 bootstrap commit 与湖上 segment |
| paimon-store | PaimonReadOnlyEngineFactory | 激活 lake directory 并创建 ReadOnlyEngine |

## 6. 核心元数据关系

~~~mermaid
flowchart LR
    S["snapshot-N<br/>JSON"]
    IM["index-manifest-X<br/>默认 Avro + Zstd"]
    IE["IndexManifestEntry"]
    IFM["IndexFileMeta"]
    GIM["GlobalIndexMeta"]
    ESM["ESM1 v2"]
    A["es-index archive"]
    LF["Lucene logical files"]

    S -->|"indexManifest 文件名"| IM
    IM -->|"当前 live ADD 集合"| IE
    IE --> IFM
    IFM -->|"fileName / size / rowCount"| A
    IFM --> GIM
    GIM --> RID["row range + stable field IDs"]
    GIM -->|"_INDEX_META bytes"| ESM
    ESM -->|"name → offset,length"| LF
    A -->|"封装"| LF
~~~

这套结构分为两层：

- Paimon 元数据负责“哪个 snapshot 可以看见哪些 archive”；
- ESM1 负责“一个 archive 内有哪些 Lucene 文件以及它们的位置”。

## 7. 表与 Row Tracking

Global Index 表通常需要：

~~~sql
TBLPROPERTIES (
    'bucket' = '-1',
    'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true',
    'global-index.enabled' = 'true'
)
~~~

Row Tracking 为每条记录分配表内全局唯一的 <code>_ROW_ID</code>：

~~~sql
SELECT *, _ROW_ID, _SEQUENCE_NUMBER
FROM oss.default.`my_table_3$row_tracking`
ORDER BY _ROW_ID;
~~~

Snapshot 维护 <code>nextRowId</code>，构建器以：

~~~text
[0, nextRowId - 1]
~~~

作为理论候选空间，再减去已有索引覆盖范围。

## 8. 构建流程

### 8.1 构建时序

~~~mermaid
sequenceDiagram
    autonumber
    actor User
    participant SQL as Spark SQL
    participant Proc as CreateGlobalIndexProcedure
    participant Meta as Paimon Snapshot/Manifest
    participant Planner as GlobalIndexBuilderUtils
    participant Exec as Spark Executors
    participant Writer as ESIndexGlobalIndexWriter
    participant OSS as OSS / File System
    participant Commit as TableCommitImpl

    User->>SQL: CALL sys.create_global_index
    SQL->>Proc: table, fields, index_type=es-index, options
    Proc->>Meta: 固定 latest snapshot S
    Meta-->>Proc: data files、nextRowId、旧 index entries
    Proc->>Planner: 计算 unindexedRowRanges
    Planner->>Planner: 按 partition/bucket/range 规划 IndexedSplit
    Planner-->>Exec: 每个 split 一个构建任务

    loop 每个 IndexedSplit
        Exec->>Meta: 读取索引字段 + _ROW_ID
        Exec->>Writer: write(relativeRowId, row)
        Writer->>Writer: Lucene build + forceMerge(1)
        Writer->>OSS: 写 es-index archive
        Writer-->>Exec: ResultEntry(fileName,rowCount,ESM1)
    end

    Exec-->>Proc: 收集 CommitMessage
    Proc->>Commit: commit(indexResults)
    Commit->>OSS: 写新 index-manifest
    Commit->>OSS: 提交新 snapshot S+1
    Commit-->>User: 新 archive 数量
~~~

### 8.2 调用链

~~~text
CreateGlobalIndexProcedure
  └─ GlobalIndexTopologyBuilderUtils.createTopoBuilder("es-index")
      └─ DefaultGlobalIndexTopoBuilder.buildIndex(...)
          ├─ GlobalIndexBuilderUtils.unindexedRowRanges(...)
          ├─ GlobalIndexBuilderUtils.createShardIndexedSplits(...)
          ├─ SparkContext.parallelize(...)
          └─ DefaultGlobalIndexBuilder.buildIndex(...)
              ├─ GlobalIndexer SPI → es-index
              ├─ ESIndexGlobalIndexWriter.write(...)
              └─ ESIndexGlobalIndexWriter.finish()
~~~

### 8.3 未覆盖范围算法

~~~text
candidate = [0, snapshot.nextRowId - 1]

covered = 所有满足以下条件的 live index ranges：
  indexType == "es-index"
  primaryFieldId 相同
  extraFieldIds 顺序和内容相同
  partition filter 相同

uncovered = candidate - merge(covered)
~~~

这意味着：

- 对同一字段布局再次构建时，只补齐空缺；
- 字段名相同但 stable field ID 不同，不会被视为同一索引；
- extra fields 的顺序也是索引身份的一部分；
- Row-ID range 允许有 gap，但不允许 overlap。

### 8.4 shard 切分

非排序型 Global Index 使用：

~~~text
global-index.row-count-per-shard = 100000
~~~

作为目标 archive 行数。对任意 Row-ID：

~~~text
alignedStart = floor(rowId / shardSize) * shardSize
alignedEnd   = alignedStart + shardSize - 1
~~~

规划器再与数据文件实际范围、连续文件组、uncovered ranges 以及 partition/bucket 求交，得到最终 IndexedSplit。

| 配置 | 默认值 | 作用 |
|---|---:|---|
| global-index.row-count-per-shard | 100000 | 每个非排序索引 archive 的目标行数 |
| global-index.build.max-shard | 32 | 一次构建偏好的最大 shard 数 |
| global-index.build.max-parallelism | 4096 | Spark/Flink 构建最大并行度 |

## 9. Row-ID 与 Lucene docID

executor 读取绝对 Paimon Row-ID，写入前转换为：

~~~text
relativeRowId = absoluteRowId - rowRangeStart
~~~

~~~mermaid
flowchart LR
    P0["Paimon Row-ID 100000"] --> D0["Lucene docID 0"]
    P1["Paimon Row-ID 100001"] --> D1["Lucene docID 1"]
    P2["Paimon Row-ID 100002"] --> D2["Lucene docID 2"]
    PN["Paimon Row-ID 199999"] --> DN["Lucene docID 99999"]

    R["archive rangeStart = 100000"] --> F["globalRowId = rangeStart + docID"]
~~~

强不变量：

~~~text
rowRangeStart 和 rowRangeEnd 均包含

rowCount = rowRangeEnd - rowRangeStart + 1

0 <= luceneDocId < rowCount

globalRowId = rowRangeStart + luceneDocId
~~~

索引字段为 null 时仍写空占位文档，否则后续文档会左移，Row-ID 映射失效。

## 10. Lucene 文档设计

### 10.1 内部字段

每个文档包含：

~~~text
_ROW_ID = relativeRowId
~~~

它使用 SortedNumericDocValuesField，并作为 Lucene index sort。构建结束执行 forceMerge(1)，保证最终 segment 的 docID 顺序与相对 Row-ID 一致。

### 10.2 业务字段

| 类型 | Lucene 表示 | 查询能力 | 原始值 |
|---|---|---|---|
| VECTOR | KnnFloatVectorField | KNN / ANN | 向量索引所需数据 |
| FULLTEXT | TextField(Store.NO) | match、phrase、bool | 不保存 |
| KEYWORD | StringField + Sorted DocValues | term、聚合、返回 docvalue | DocValues |
| INT/LONG | Point + Numeric DocValues | 精确、范围、排序 | DocValues |
| FLOAT/DOUBLE | Point + Numeric DocValues | 精确、范围、排序 | DocValues |
| DATE | long/point/docvalue | 日期范围 | DocValues |
| GEO_POINT | LatLonPoint + DocValues | 地理查询 | DocValues |

String 字段可选择 FULLTEXT 或 KEYWORD，并可生成：

~~~text
<field>.keyword
<field>.fulltext
<field>.__paimon_array_present
~~~

### 10.3 my_table_3 映射

| 字段 | 配置 | ES mapping |
|---|---|---|
| embedding | VECTOR、DISKBBQ、dim=4、cosine | dense_vector + bbq_disk |
| content | FULLTEXT、standard | text + content.keyword |
| category | KEYWORD | keyword + category.fulltext |
| price | SCALAR INT | integer |

Mount 端根据 ESM1 重建 mapping，并强制：

~~~json
{
  "dynamic": "strict",
  "_source": {
    "enabled": false
  }
}
~~~

ES mapping 只承担查询解析所需的逻辑类型声明。向量 mapping 只写
bbq_disk、int8_hnsw 或 hnsw 等算法类型，不会完整复制 ESM1 中的
vectors_per_cluster、centroids_per_parent_cluster、m、ef_construction
等构建参数。完整物理配置仍以 ESM1 和 Lucene segment codec 为准。

当前索引列没有包含业务字段 id。如果需要从 ES 结果直接返回 id，应在重新构建时把它作为 companion field，例如：

~~~sql
index_column => 'embedding,id,content,category,price'
~~~

## 11. 湖上文件布局

对于：

~~~text
oss://cy-test2/spark/default.db/my_table_3
~~~

典型布局：

~~~text
my_table_3/
├─ schema/
│  └─ schema-<schemaId>
├─ snapshot/
│  ├─ snapshot-1
│  ├─ snapshot-2
│  └─ ...
├─ manifest/
│  ├─ manifest-list-*
│  ├─ manifest-*
│  └─ index-manifest-<writerUUID>-<counter>
├─ <Paimon 数据目录>/
│  └─ data-*.parquet
└─ index/
   ├─ es-index-global-index-<UUID-A>.index
   ├─ es-index-global-index-<UUID-B>.index
   └─ ...
~~~

如果设置 global-index.external-path：

- archive 写到 external root；
- index-manifest 仍在表的 manifest 目录；
- IndexFileMeta.externalPath 保存完整 archive URI。

## 12. IndexManifest 数据模型

~~~text
_VERSION INT = 1
_KIND TINYINT                       // ADD=0, DELETE=1
_PARTITION BYTES
_BUCKET INT
_INDEX_TYPE STRING                  // "es-index"
_FILE_NAME STRING
_FILE_SIZE BIGINT
_ROW_COUNT BIGINT
_DELETIONS_VECTORS_RANGES ARRAY
_EXTERNAL_PATH STRING

_GLOBAL_INDEX ROW {
    _ROW_RANGE_START BIGINT
    _ROW_RANGE_END BIGINT
    _INDEX_FIELD_ID INT
    _EXTRA_FIELD_IDS ARRAY<INT>
    _INDEX_META BYTES
    _SOURCE_META BYTES
}
~~~

es-index 当前约定：

- _INDEX_META：ESM1 v2；
- _SOURCE_META：null；
- DV ranges：通常为 null；
- _INDEX_FIELD_ID：index_column 中第一个字段的 stable DataField ID；
- _EXTRA_FIELD_IDS：其余字段 ID，顺序不变；
- _FILE_SIZE：整个 archive wrapper 的物理长度；
- _ROW_COUNT：Lucene 文档数。

Paimon 2.0.0 默认使用 Avro + Zstd 写 index-manifest，但可由 manifest.format 和 manifest.compression 覆盖。

已提交 snapshot 指向的是“当前 live index 集合”的物化清单。DELETE 是 commit 合并输入，正常最终 manifest 只保留 live ADD entries。

## 13. archive 二进制格式

文件名：

~~~text
es-index-global-index-<UUID>.index
~~~

使用 Java DataOutputStream，大端：

~~~text
int32 fileCount

repeat fileCount:
    int32 nameByteLength
    byte[nameByteLength] logicalLuceneFileNameUtf8
    int64 dataLength
    byte[dataLength] rawLuceneFileBytes
~~~

~~~mermaid
flowchart LR
    H["fileCount<br/>4 bytes"]
    E1["nameLen + name<br/>dataLen + raw bytes"]
    E2["nameLen + name<br/>dataLen + raw bytes"]
    EN["..."]

    H --> E1 --> E2 --> EN
~~~

特征：

- wrapper 无 magic、无 version、无 CRC；
- Lucene logical file 保留原始 codec checksum；
- entry 顺序无业务语义；
- writer 使用 64 KiB buffer 流式封装；
- ESM1 offset table 与实际 entry 顺序一致。

## 14. ESM1 v2

ESM1 存于 GlobalIndexMeta._INDEX_META，用于随机定位 archive 内 logical files：

~~~text
int32 magic   = 0x45534D31           // "ESM1"
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

字符串采用：

~~~text
int32 UTF8 byte length + UTF8 bytes
~~~

nullable 值采用：

~~~text
boolean present + value when present
~~~

payloadOffset 指向 raw Lucene file bytes 的首字节，不是 archive entry header。

Reader 兼容：

- v0：legacy offset-only；
- v1：field configs + offsets；
- v2：ordered fields/types + configs + offsets。

Mount 的字段布局比较包括有序字段名/类型和完整字段配置，但不比较各
archive 的物理 offset。由于 v1 没有显式字段类型，v1 与 v2 archive
不能作为相同布局混挂。

完整字节级说明：

- [spark-es-index-write-chain.md](spark-es-index-write-chain.md)
- [ESIndexArchiveMetadata.java](../eslib-core/src/main/java/org/elasticsearch/eslib/mount/ESIndexArchiveMetadata.java)

## 15. Snapshot 提交与一致性

~~~mermaid
stateDiagram-v2
    [*] --> DataCommitted: INSERT 提交 Snapshot S
    DataCommitted --> Uncovered: 新 Row-ID 尚无索引
    Uncovered --> Building: create_global_index
    Building --> PayloadWritten: executor 写 archive
    PayloadWritten --> Visible: driver 提交 IndexManifest + Snapshot S+1
    PayloadWritten --> Orphan: metadata commit 失败
    Visible --> Mounted: ES mount S+1
    Mounted --> Superseded: mount 更新 snapshot / alias
    Visible --> Expired: snapshot 过期且无引用
    Orphan --> Cleaned: orphan cleanup
    Superseded --> Expired: 旧 snapshot 清理
    Expired --> [*]
    Cleaned --> [*]
~~~

一致性语义：

1. executor 先写不可变 archive。
2. driver 收集 ResultEntry 并生成 IndexFileMeta。
3. TableCommitImpl 写新的 index-manifest。
4. 新 snapshot 指向该 manifest。
5. snapshot 成功提交后，整批 archive 才可见。

因此：

- 不会出现半批 archive 被同一 snapshot 看见；
- metadata commit 失败可能遗留不可见 orphan；
- 旧 snapshot 仍可引用旧 manifest 和旧 archive；
- 物理删除必须遵守 snapshot、tag 和 branch 的引用生命周期。

## 16. Mount 规划

### 16.1 REST 请求

~~~json
{
  "table_path": "oss://cy-test2/spark/default.db/my_table_3",
  "index_name": "my_table_3",
  "storage_mode": "remote",
  "source_enabled": false,
  "auth_type": "node"
}
~~~

snapshot_id 可选；省略时选择 latest snapshot。vector_field_name 可筛选包含指定字段的 es-index。

storage_mode 当前只校验 remote 或 mmap 的请求值，并不决定真实 provider；
实际使用 OSS Range GET 还是本地 positional read，由 archive URI 的
oss 或 file scheme 决定。如果同一 snapshot 中存在多套包含同名字段的
es-index，仅按 vector_field_name 过滤仍可能混入不同布局，生产环境应保持
主向量字段唯一，或扩展 API 使用 field IDs/layout hash 精确选择。

### 16.2 Mount 时序

~~~mermaid
sequenceDiagram
    autonumber
    actor Client
    participant REST as RestPaimonMountAction
    participant Planner as PaimonSnapshotPlanner
    participant Paimon as Paimon Snapshot/Manifest
    participant Plan as PaimonMountPlan
    participant ES as Elasticsearch Cluster
    participant Store as PaimonStorePlugin
    participant Lake as OSS Archive

    Client->>REST: POST /_paimon/mount
    REST->>Planner: tablePath, snapshotId, selectedField
    Planner->>Paimon: 打开 table，选择 snapshot
    Planner->>Paimon: scan(snapshot, "es-index")
    Paimon-->>Planner: IndexManifestEntry 列表
    Planner->>Planner: 解析 ESM1、构造 LakeShardDescriptor
    Planner->>Plan: 排序并校验 archive
    Plan-->>REST: snapshotId、field layout、shards
    REST->>REST: 生成 mapping 和 PMS1 shard settings
    REST->>ES: 创建 alias_snapshotId 物理索引
    ES->>Store: 每个 shard 创建 Directory
    Store->>Lake: 打开 archive provider
    Store->>Store: bootstrap local commit + activate lake
    Store-->>ES: ReadOnlyEngine
    REST->>ES: 原子切换 alias
    ES-->>Client: alias、physical index、snapshot、shards
~~~

### 16.3 Mount 前校验

Planner 和 MountPlan 拒绝：

- snapshot 没有 indexManifest；
- es-index 没有 GlobalIndexMeta 或 _INDEX_META；
- ESM1 version、长度、offset 非法；
- logical file 重名；
- archive URI 重复；
- Row-ID 范围重叠；
- rowCount 与闭区间长度不一致；
- archive 之间字段布局不一致；
- 单个 Lucene shard 超过 Integer.MAX_VALUE 行。

archive range 允许存在 gap；gap 代表该 snapshot 的部分 Row-ID 未被所选 es-index 覆盖。

## 17. ES 物理索引设计

物理索引名：

~~~text
<alias>_<snapshotId>
~~~

核心 settings：

~~~properties
index.store.type=paimon
index.number_of_shards=<live archive count>
index.number_of_replicas=0
index.blocks.write=true
index.paimon.table_path=<table path>
index.paimon.snapshot_id=<snapshot id>
index.paimon.shards=<Base64 PMS1 list>
~~~

每个 archive 按 rowRangeStart、archive URI 排序后分配 shard ID：

~~~text
archive 0 → ES shard 0
archive 1 → ES shard 1
...
~~~

> ES shard 数量等于所选 snapshot 中的 live es-index archive 数量，不等于 Paimon bucket 数量。

## 18. PMS1 ShardMountSpec

Mount 将每个 shard 的必要描述写入 index settings：

~~~text
magic       = 0x504D5331             // "PMS1"
version     = 1
archive URI
archive length
rowRangeStart
rowRangeEnd
rowCount
logical file count
repeat:
    file name
    offset
    length
CRC32 written as 8-byte long
~~~

结构最终使用 Base64 编码。

PMS1：

- 不包含 OSS access key 或 secret；
- 带 version 和 CRC32；
- 是 ES cluster-state 派生配置；
- 不是 Spark 写入的湖上 Paimon 元数据。

代码：[ShardMountSpec.java](../paimon-store/src/main/java/org/elasticsearch/paimon/ShardMountSpec.java)

## 19. Directory 与 ReadOnlyEngine

~~~mermaid
flowchart TB
    ES["Elasticsearch shard recovery"]
    LOCAL["本地 FSDirectory<br/>bootstrap commit"]
    LAKE["ArchiveDirectory<br/>湖上 Lucene files"]
    SWITCH["SwitchableMountDirectory"]
    OVERLAY["本地小型 segments_N overlay"]
    ENGINE["ReadOnlyEngine"]

    ES --> LOCAL
    LOCAL --> SWITCH
    LAKE --> SWITCH
    SWITCH -->|"复制 bootstrap userData<br/>引用 lake segments"| OVERLAY
    OVERLAY --> ENGINE
    LAKE --> ENGINE
~~~

Elasticsearch 先完成正常空 store recovery，然后在 engine open 时：

1. 读取本地 bootstrap SegmentInfos。
2. 读取湖上 archive 的 SegmentInfos。
3. 将 bootstrap commit userData 写入湖上 SegmentInfos 副本。
4. 仅在本地写一个很小的 synthetic <code>segments_N</code>。
5. segment data 仍保留在 archive。
6. 激活 ReadOnlyEngine。

写入由三层共同阻止：

- index.blocks.write；
- ReadOnlyEngine；
- SwitchableMountDirectory 激活后的写方法拒绝。

代码：

- [SwitchableMountDirectory.java](../paimon-store/src/main/java/org/elasticsearch/paimon/SwitchableMountDirectory.java)
- [PaimonReadOnlyEngineFactory.java](../paimon-store/src/main/java/org/elasticsearch/paimon/PaimonReadOnlyEngineFactory.java)

## 20. 查询 I/O 路径

~~~mermaid
flowchart LR
    Q["ES match / term / range / knn"]
    SH["目标 primary shards"]
    RO["ReadOnlyEngine"]
    DIR["ArchiveDirectory"]
    OFF["ESM1 / PMS1<br/>offset,length"]
    GET["OSS Range GET"]
    ARC["archive object"]
    RES["Shard Top-K / hits"]
    MERGE["ES coordinating node merge"]

    Q --> SH
    SH --> RO
    RO --> DIR
    OFF --> DIR
    DIR --> GET
    GET --> ARC
    ARC --> RES
    RES --> MERGE
~~~

ArchiveDirectory 将每个 logical Lucene file 映射为：

~~~text
archiveBaseOffset + fileOffset + requestedPosition
~~~

ArchiveIndexInput 支持：

- seek；
- slice；
- clone；
- 64 KiB 顺序读取 buffer；
- 较大读取按最大 8 MiB 块拆分；
- provider fork，使并发 Lucene inputs 共享对象访问能力。

## 21. 凭据与安全边界

OSS 凭据来自 ES node settings 和 keystore：

~~~yaml
paimon.oss.endpoint: https://oss-cn-shanghai.aliyuncs.com
paimon.oss.access_key_id: <PAIMON_OSS_ACCESS_KEY_ID>
~~~

~~~shell
bin/elasticsearch-keystore add paimon.oss.access_key_secret
~~~

设计原则：

- REST 请求不接受明文 secret；
- cluster state/PMS1 不保存 secret；
- 每个节点从自身 keystore 打开 OSS archive；
- secret 在插件构造阶段复制，因为 ES 随后会关闭 SecureSettings；
- keystore 轮换后需要重启节点。

本地路径只允许位于 Elasticsearch path.repo 下。

## 22. 实例一：my_table_3 默认单 archive

### 22.1 数据

~~~sql
INSERT INTO my_table_3 VALUES
(1, array(0.1, 0.2, 0.3, 0.4), '高性能机械键盘', '电子产品', 599),
(2, array(0.5, 0.6, 0.7, 0.8), '无线蓝牙鼠标', '电脑配件', 129),
(3, array(0.9, 0.1, 0.2, 0.3), '4K高清显示器', '显示设备', 2499),
(4, array(0.2, 0.3, 0.4, 0.5), '人体工学办公椅', '办公家具', 899),
(5, array(0.6, 0.7, 0.8, 0.9), 'Type-C 扩展坞', '数码配件', 259);
~~~

假设 Row-ID：

| id | Paimon _ROW_ID | Lucene docID |
|---:|---:|---:|
| 1 | 0 | 0 |
| 2 | 1 | 1 |
| 3 | 2 | 2 |
| 4 | 3 | 3 |
| 5 | 4 | 4 |

默认 shardSize=100000：

~~~text
archive A
  range     = [0,4]
  rowCount  = 5
  ES shard  = 0
~~~

### 22.2 IndexManifest 逻辑视图

以下表示字段语义，不是实际 JSON 存储：

~~~yaml
kind: ADD
indexType: es-index
fileName: es-index-global-index-<UUID-A>.index
fileSize: <archive bytes>
rowCount: 5
externalPath: null
globalIndex:
  rowRangeStart: 0
  rowRangeEnd: 4
  indexFieldId: 1
  extraFieldIds: [2, 3, 4]
  indexMeta: <ESM1 v2 bytes>
  sourceMeta: null
~~~

如果初始 schema 顺序为：

~~~text
id, embedding, content, category, price
~~~

且没有字段删除后重建，则 field IDs 通常是：

~~~text
id=0, embedding=1, content=2, category=3, price=4
~~~

实际系统必须使用 stable DataField ID。

### 22.3 Mount 结果

若索引构建产生 snapshot 2：

~~~text
alias          = my_table_3
physical index = my_table_3_2
primary shards = 1
shard 0        = archive A, range [0,4]
~~~

## 23. 实例二：多 archive 映射

为直观看到分片，假设测试环境设置：

~~~properties
global-index.row-count-per-shard=3
~~~

五行规划为：

~~~text
archive A: [0,2], rowCount=3
archive B: [3,4], rowCount=2
~~~

~~~mermaid
flowchart TB
    subgraph A["Archive A → ES shard 0"]
        A0["docID 0 → Row-ID 0 → id=1"]
        A1["docID 1 → Row-ID 1 → id=2"]
        A2["docID 2 → Row-ID 2 → id=3"]
    end

    subgraph B["Archive B → ES shard 1"]
        B0["docID 0 → Row-ID 3 → id=4"]
        B1["docID 1 → Row-ID 4 → id=5"]
    end
~~~

例如 shard 1 命中 docID 1：

~~~text
globalRowId = shard.rangeStart + docID
            = 3 + 1
            = 4
~~~

对应 Paimon 中 id=5 的行。

## 24. 实例三：增量追加

初始索引：

~~~text
archive A: range [0,4]
~~~

随后 INSERT 两行，产生 Row-ID 5、6。普通 INSERT 后：

~~~text
data snapshot contains Row-ID [0,6]
es-index still covers only [0,4]
uncovered range = [5,6]
~~~

再次执行 create_global_index：

~~~text
old archive A: [0,4]
new archive B: [5,6]
new index manifest: A + B
~~~

重新 mount：

~~~text
ES shard 0 → archive A → [0,4]
ES shard 1 → archive B → [5,6]
~~~

最危险的状态是 INSERT 已提交、但尚未重建索引：mount 和查询都可能成功，但新增行不会出现在结果中。这是覆盖不完整，不是运行时异常。

另一个容易误判的例子：

~~~text
已有 archive A: [0,99999]
缺失 range:       [100000,199999]
已有 archive C: [200000,249999]
~~~

Mount 允许这个 gap，并按现有 archive 排序编号：

~~~text
ES shard 0 → archive A
ES shard 1 → archive C
~~~

archive C 不会保持“逻辑 shard 2”；ES shard ID 是每次 mount 规划产生的
排序 ordinal。缺失区间内的行不会自动回退到 Paimon 数据文件。

## 25. 查询实例

### 25.1 全文查询

~~~shell
curl -X POST 'http://127.0.0.1:9200/my_table_3/_search?pretty' \
  -H 'Content-Type: application/json' \
  -d '{
    "size": 10,
    "_source": false,
    "query": {
      "match": {
        "content": "鼠标"
      }
    },
    "docvalue_fields": [
      "content.keyword",
      "category",
      "price"
    ]
  }'
~~~

### 25.2 Keyword 和数值过滤

~~~shell
curl -X POST 'http://127.0.0.1:9200/my_table_3/_search?pretty' \
  -H 'Content-Type: application/json' \
  -d '{
    "size": 10,
    "_source": false,
    "query": {
      "bool": {
        "filter": [
          { "term": { "category": "电脑配件" } },
          { "range": { "price": { "lte": 500 } } }
        ]
      }
    },
    "docvalue_fields": [
      "content.keyword",
      "category",
      "price"
    ]
  }'
~~~

### 25.3 KNN 查询

~~~shell
curl -X POST 'http://127.0.0.1:9200/my_table_3/_search?pretty' \
  -H 'Content-Type: application/json' \
  -d '{
    "size": 3,
    "_source": false,
    "knn": {
      "field": "embedding",
      "query_vector": [0.5, 0.6, 0.7, 0.8],
      "k": 3,
      "num_candidates": 5
    },
    "docvalue_fields": [
      "content.keyword",
      "category",
      "price"
    ]
  }'
~~~

当前 Lucene 10 mount reader 对 ES940 DiskBBQ 执行正确性优先的 shard 内精确扫描。该向量按 cosine 的理论前三名：

| 排名 | Row-ID | 内容 | cosine |
|---:|---:|---|---:|
| 1 | 1 | 无线蓝牙鼠标 | 1.000000 |
| 2 | 4 | Type-C 扩展坞 | 约 0.999750 |
| 3 | 3 | 人体工学办公椅 | 约 0.990375 |

查询会 fan-out 到所有 mounted primary shards，再由 ES coordinating node 合并全局 Top-K。当前 exact fallback 下，num_candidates 不会带来完整 DiskBBQ IVF 的候选裁剪收益。

## 26. 核查

### 26.1 Paimon Row-ID

~~~sql
SELECT id, embedding, content, category, price, _ROW_ID
FROM oss.default.`my_table_3$row_tracking`
ORDER BY _ROW_ID;
~~~

### 26.2 Snapshot

~~~sql
SELECT *
FROM oss.default.`my_table_3$snapshots`
ORDER BY snapshot_id DESC;
~~~

### 26.3 Index files

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

每行必须满足：

~~~text
row_count = row_range_end - row_range_start + 1
~~~

### 26.4 Elasticsearch

~~~shell
curl 'http://127.0.0.1:9200/_cat/plugins?v'
curl 'http://127.0.0.1:9200/_cat/aliases?v'
curl 'http://127.0.0.1:9200/_cat/shards?v'
curl 'http://127.0.0.1:9200/my_table_3/_mapping?pretty'
~~~

## 27. 失败边界

| 场景 | 当前行为 | 设计含义 |
|---|---|---|
| INSERT 后未重建 index | 新行不在旧 archive 中 | ES mount 不回退扫描 Paimon 数据 |
| Paimon update/delete 或 deletion vector 改变可见行 | 当前 mount 不应用 Paimon live-row/DV 过滤 | 当前方案应限定到 append-only、不可变的已索引快照 |
| 更改 analyzer/algorithm/dimension 后直接增量构建 | 覆盖判定仍可能复用旧 range，新增 archive 布局不同 | 应先 drop 再按统一配置全量 rebuild |
| executor 写 archive 后 commit 失败 | archive 成为 orphan | 不可见，但需清理 |
| index ranges overlap | mount 失败 | 防止一个 Row-ID 映射到多个 shard |
| field layout 不一致 | mount 失败 | 一个 ES index 必须只有一个 mapping |
| ESM1 offset 越界 | mount 失败 | 防止读取非法 archive 区域 |
| 本地 archive 长度变化 | shard open 失败 | 防止打开被替换对象 |
| OSS archive 被同长度覆盖 | 当前难以提前检测 | 建议增加 ETag/VersionId 固定 |
| source_enabled=true | REST 拒绝 | 当前没有行回填能力 |
| ES data node 不可用 | 零副本 shard 不可用 | 当前不是高可用形态 |
| alias 切换时 shard 尚未 STARTED | 查询可能短暂 503 | mount Job 应等待 cluster health |
| DiskBBQ 大 shard | exact scan 延迟升高 | 正确性优先，需补全 IVF reader |
| OSS 凭据轮换 | 旧进程使用启动时 secret | 更新 keystore 后重启节点 |

## 28. 性能模型

### 28.1 构建成本

~~~text
读取 Paimon 投影字段
+ Lucene 文档写入
+ HNSW/DiskBBQ 构建
+ forceMerge(1)
+ archive 上传
~~~

主要调优维度：

- row-count-per-shard；
- Spark task 并行度；
- embedding 维度；
- vector algorithm；
- HNSW m、ef_construction；
- DiskBBQ cluster 参数；
- executor 内存和本地临时盘；
- OSS 上传带宽。

### 28.2 查询成本

~~~text
总延迟
≈ coordinating node 开销
+ shard fan-out
+ Lucene 查询
+ OSS Range GET 次数 × RTT
+ 结果合并
~~~

archive 太大时，单 shard CPU、内存和远程读取增大；archive 太小时，ES shard 数、cluster state 和查询 fan-out 增大。

当前 DiskBBQ exact scan 的主要复杂度接近：

~~~text
O(rows × dimensions)
~~~

shardSize 应根据维度、OSS RTT、并发和目标延迟压测确定，不能只依赖默认值。

## 29. 已知限制与演进建议

### 29.1 当前限制

- _source 关闭；
- return_fields 尚未驱动 source hydration；
- 当前 ES mount 不读取 Paimon deletion vector，也没有 raw-data fallback；
- 已有覆盖身份主要由 index type 和 stable field IDs 决定，不包含完整配置 hash；
- replicas=0；
- 重挂同一 alias 和相同 snapshot 时物理索引已存在会失败；
- 旧 ES 物理索引不会自动删除；
- OSS 没有固定 ETag/VersionId；
- DiskBBQ 当前为 raw-vector 精确回退；
- mount 创建索引后立即切 alias，没有显式等待 primary STARTED。

### 29.2 建议顺序

1. mount 后等待 primary shard STARTED，再切 alias；
2. 实现 OSS HEAD 校验并固定 ETag/VersionId；
3. 完整实现 ES940 DiskBBQ IVF reader；
4. 增加 archive cache 和查询 I/O 指标；
5. 暴露 global Row-ID，并实现 Paimon source hydration；
6. 增加旧物理索引和过期 snapshot 的清理；
7. 设计高可用多节点挂载或 archive-aware recovery；
8. 建立 Lucene 9 writer → OSS → Lucene 10 reader 的真实跨版本回归。

## 30. 验收测试矩阵

| 类别 | 测试 |
|---|---|
| Row-ID | 单行、多行、null 行、range gap、range overlap |
| archive | 单文件、多文件、截断、长度变化、checksum 错误 |
| metadata | ESM1 v0/v1/v2、未知 version、重复 filename、offset overflow |
| field | vector、text、keyword、numeric、date、geo、generated subfields |
| shard | 单 archive、多 archive、排序稳定性、节点重启 |
| Snapshot | latest、显式 snapshot、旧 snapshot、无 indexManifest |
| 增量 | INSERT 后部分覆盖、补建、drop/rebuild |
| OSS | 正确凭据、错误签名、网络超时、Range GET 短读 |
| ES | mapping、read-only、alias 切换、primary readiness |
| 兼容 | Lucene 9 构建、Lucene 10 mount、DiskBBQ 查询 |

## 31. 代码导航

构建与 archive：

- [DefaultESIndexBuilder.java](../eslib-core/src/main/java/org/elasticsearch/eslib/builder/DefaultESIndexBuilder.java)
- [ScalarFieldHandler.java](../eslib-core/src/main/java/org/elasticsearch/eslib/scalar/ScalarFieldHandler.java)
- [ESIndexGlobalIndexWriter.java](../build/codex-paimon-repo/paimon-eslib/src/main/java/org/apache/paimon/eslib/index/ESIndexGlobalIndexWriter.java)
- [ESIndexFileMeta.java](../build/codex-paimon-repo/paimon-eslib/src/main/java/org/apache/paimon/eslib/index/ESIndexFileMeta.java)

Mount 与查询：

- [PaimonSnapshotPlanner.java](../paimon-store/src/main/java/org/elasticsearch/paimon/PaimonSnapshotPlanner.java)
- [PaimonMountPlan.java](../eslib-core/src/main/java/org/elasticsearch/eslib/mount/PaimonMountPlan.java)
- [LakeShardDescriptor.java](../eslib-core/src/main/java/org/elasticsearch/eslib/mount/LakeShardDescriptor.java)
- [ESIndexArchiveMetadata.java](../eslib-core/src/main/java/org/elasticsearch/eslib/mount/ESIndexArchiveMetadata.java)
- [RestPaimonMountAction.java](../paimon-store/src/main/java/org/elasticsearch/paimon/RestPaimonMountAction.java)
- [ElasticsearchMappingBuilder.java](../paimon-store/src/main/java/org/elasticsearch/paimon/ElasticsearchMappingBuilder.java)
- [ShardMountSpec.java](../paimon-store/src/main/java/org/elasticsearch/paimon/ShardMountSpec.java)
- [PaimonStorePlugin.java](../paimon-store/src/main/java/org/elasticsearch/paimon/PaimonStorePlugin.java)
- [SwitchableMountDirectory.java](../paimon-store/src/main/java/org/elasticsearch/paimon/SwitchableMountDirectory.java)
- [PaimonReadOnlyEngineFactory.java](../paimon-store/src/main/java/org/elasticsearch/paimon/PaimonReadOnlyEngineFactory.java)
- [ArchiveDirectory.java](../eslib-core/src/main/java/org/elasticsearch/eslib/io/ArchiveDirectory.java)
- [ArchiveIndexInput.java](../eslib-core/src/main/java/org/elasticsearch/eslib/io/ArchiveIndexInput.java)
- [OSSArchiveDataProvider.java](../eslib-core/src/main/java/org/elasticsearch/eslib/io/OSSArchiveDataProvider.java)

部署与核查：

- [paimon-store README](../paimon-store/README.md)
- [Kubernetes ConfigMap](../deploy/k8s/configmap.yaml)
- [Paimon inspection SQL](../deploy/k8s/inspect-paimon.sql)
- [Spark 写入链路文档](spark-es-index-write-chain.md)

## 32. 设计总结

~~~text
Paimon 管理数据、Row-ID、Snapshot 和 archive 生命周期

Lucene archive 提供搜索结构

Elasticsearch 将 archive 解释为只读 shard
~~~

核心不变量：

~~~text
global Paimon Row-ID
    =
archive.rowRangeStart + Lucene docID
~~~

一致性边界：

~~~text
archive 物理写入
    ≠
archive 对查询可见

只有 snapshot.indexManifest 引用后才可见
~~~

服务化映射：

~~~text
一个 live es-index archive
    =
一个 ES primary shard
~~~
