# ES-Paimon Mount 总体设计与迭代复盘

> 文档状态：2026-09-03  
> 目标版本：Elasticsearch 9.4.0 / Lucene 10.4.0 / Paimon 2.0.0 / JDK 21  
> 写入侧：Spark 3.5.1 / Lucene 9.12.0  
> 当前插件版本：`paimon-store 1.0.7`  
> 当前 K8s 测试镜像：`es-paimon:9.4.0-1.0.7-r9`

## 1. 项目结论

当前项目已经完成核心功能闭环，可以作为“Elasticsearch 直接挂载并查询 Paimon 湖上 Lucene 索引”的单节点功能验证版本：

- 不重新导入、不复制 Lucene segment。
- 一个 Paimon ESLib archive 映射为一个 Elasticsearch primary shard。
- Spark 写入侧使用 Lucene 9.12，Elasticsearch 9.4 挂载侧使用 Lucene 10.4/JDK 21。
- 支持 OSS Range GET、普通全文/标量查询和 ES940 DiskBBQ 向量查询。
- 物理索引按 snapshot 隔离，通过稳定 alias 切换。
- 插件运行依赖经过裁剪，当前产物未发现插件内部或插件与 Elasticsearch 运行时的重复类。
- 当前向量查询优先保证正确性，采用逐 shard 精确扫描，尚未实现 DiskBBQ IVF 加速。

当前定位是“可运行的功能验证版”，还不是生产化完成版。生产化重点将从兼容性打通转向端到端自动化、远端向量查询性能、索引生命周期和容灾治理。

## 2. 总体设计方案

### 2.1 端到端数据流

```text
Spark / Paimon / Lucene 9.12
        │
        ├─ Paimon snapshot、schema、index manifest
        └─ ESLib archive
             ├─ Lucene logical file → [offset, length)
             ├─ rowRangeStart / rowRangeEnd
             └─ 字段及向量格式元数据
                          │
                    POST /_paimon/mount
                          │
              PaimonSnapshotPlanner
                          │
          校验、排序 live es-index archives
                          │
        archive 0 → ES primary shard 0
        archive 1 → ES primary shard 1
        archive N → ES primary shard N
                          │
      <alias>_<snapshotId> 只读物理索引
                          │
 ArchiveDirectory → OSS Range GET → ReadOnlyEngine
                          │
              标准 Elasticsearch Query DSL
```

核心思想不是“把湖上索引导入 Elasticsearch”，而是“让 Elasticsearch 的 Lucene reader 直接读取湖上 segment”。

### 2.2 写入层

Spark/Paimon 调用 `DefaultESIndexBuilder` 生成 Lucene 9.12 索引，包括：

- text、keyword、scalar、vector 字段。
- `_ROW_ID` 排序键。
- 稠密 Lucene docId。
- 空值位置补空文档，以保持行号映射稳定。
- 最终 `forceMerge(1)`，形成稳定的 segment 布局。

将 Lucene 文件串接成 archive、写入 Paimon Global Index 元数据的部分由 Spark 侧引入的 `paimon-eslib-2.0.0.jar` 完成，不在当前仓库中。archive metadata 至少包含：

- 字段布局和索引类型。
- logical Lucene filename 到 archive `[offset, length)` 的映射。
- `rowRangeStart`、`rowRangeEnd` 和 `rowCount`。
- archive URI 和预期长度。

### 2.3 挂载控制面

`PaimonSnapshotPlanner` 负责：

- 打开指定 Paimon 表和 snapshot。
- 默认选择最新 snapshot，也可以显式指定 `snapshot_id`。
- 扫描 snapshot 中 live `es-index` ADD 记录。
- 解析 archive URI、长度、row range、字段布局和 Lucene 文件偏移。
- 可按 `vector_field_name` 选择同一 snapshot 中的目标 ESLib 索引。

`PaimonMountPlan` 负责：

- 按 `rowRangeStart` 排序 archive。
- 拒绝重复 archive URI。
- 拒绝全局 row range 重叠。
- 拒绝不同 archive 的字段布局不一致。
- 将排序后数组位置稳定映射为 Elasticsearch shard id。
- 保持 `globalRowId = rowRangeStart + luceneDocId`。

`RestPaimonMountAction` 负责创建：

- 物理索引：`<alias>_<snapshotId>`。
- `index.store.type=paimon`。
- `index.blocks.write=true`。
- `index.number_of_replicas=0`。
- 根据 archive metadata 生成的严格 mapping。
- 创建物理索引后，将稳定 alias 原子切换到新 snapshot。

同一个 alias 和 snapshot 不会原地覆盖。若同名物理索引已存在，重复 mount 当前会失败，这是对不可变 snapshot 的保护行为。

### 2.4 Archive 到 shard 的映射

核心映射关系如下：

```text
一个 Paimon ESLib archive
       ↓
一个 Elasticsearch primary shard
       ↓
一个 ArchiveDirectory
       ↓
Lucene logical file
       ↓
archive 中的 [offset, length)
       ↓
本地 FileChannel 或 OSS HTTP Range GET
```

选择“一 archive 对应一 shard”的原因：

- 不需要重新打包、拆分或复制 Lucene 文件。
- 可以保持 `rowRangeStart + luceneDocId` 的全局行号关系。
- snapshot 的 archive 边界可以直接成为 Elasticsearch 的 shard 边界。

代价是 Elasticsearch shard 数由 Paimon archive 数量决定。archive 过多时，需要关注 shard 数量和 cluster-state 体积。

### 2.5 挂载数据面

每个 shard 的 URI、archive 长度、row range 和文件 offset 被编码进 `ShardMountSpec`：

- 带格式版本。
- 带 CRC32 校验。
- 使用 Base64 存储在 filtered index setting 中。
- 不包含 OSS AccessKey Secret。

数据读取流程：

1. `PaimonStorePlugin` 注册自定义 `IndexStorePlugin` 和 `EnginePlugin`。
2. `MountArchiveProviderFactory` 根据 URI 选择本地文件或 OSS provider。
3. 本地路径必须位于 Elasticsearch `path.repo` 下。
4. `ArchiveDirectory` 将 logical Lucene file 映射到 archive byte range。
5. `ArchiveIndexInput` 实现 seek、slice、clone、缓冲读取和 provider 复用。
6. `SwitchableMountDirectory` 首先允许 Elasticsearch 完成标准空 store recovery。
7. engine 打开时读取湖上 `SegmentInfos`，合成本地 bootstrap commit metadata。
8. 本地只写入一个很小的 overlay `segments_N`。
9. `PaimonReadOnlyEngineFactory` 打开 `ReadOnlyEngine`。
10. 后续 segment data 全部从湖上 archive 按需读取。

### 2.6 OSS 访问设计

最终没有使用 Paimon 自带的 `paimon-oss` 和 Hadoop，而是拆分成两条只读 OSS 访问链：

1. `PaimonOssFileIO`
   - 用于 mount planning。
   - 读取 Paimon schema、snapshot、manifest 和 global index metadata。
   - 直接基于 Aliyun OSS SDK 3.17.4。
   - 实现 exists、stat、list 和 seekable range read。
   - 所有写操作明确拒绝。

2. `OSSArchiveDataProvider`
   - 用于 shard 查询阶段。
   - 对 Lucene archive 执行 HTTP Range GET。
   - fork 后共享引用计数 OSS client，避免重复创建大量客户端。

这种设计同时避开：

- Paimon `ComponentClassLoader` 的 `create_class_loader` entitlement。
- Hadoop 3.3.4 在 JDK 21 上调用不受支持的 `Subject.getSubject`。
- Hadoop 运行时和 filesystem cache 带来的依赖体积与 Jar Hell 风险。

### 2.7 Mapping 与原始字段

Mapping 由 ESLib archive metadata 生成，采用：

- `dynamic: strict`
- `_source.enabled: false`
- text/keyword/scalar/date/geo_point/dense_vector 映射
- text 的 `.keyword` multifield
- keyword 的 `.fulltext` multifield
- DiskBBQ 映射为 Elasticsearch `dense_vector` 的 `bbq_disk`

当前没有完整 JSON `_source`，也没有按全局 row-id 回表 Paimon。因此：

- `source_enabled=true` 会被 mount API 明确拒绝。
- 原始值只能通过已有 DocValues 返回。
- 示例字段可以使用 `content.keyword`、`category` 和 `price`。
- 返回值位于 Elasticsearch hit 的 `fields`，不是 `_source`。

### 2.8 Lucene 9 写、Lucene 10 读

Spark 写入侧使用 Lucene 9.12，Elasticsearch 9.4 使用 Lucene 10.4。兼容策略为：

- 保持外层 codec 名称 `PaimonLucene9` 稳定。
- Lucene 10 插件注册对应 Codec 和 KnnVectorsFormat SPI。
- 非向量格式交给 `lucene-backward-codecs` 的 Lucene 9.12 codec 读取。
- 向量格式根据湖上 segment header 选择对应 ESLib reader。
- 不修改既有 archive 的格式名和 header，从而保持已有湖上索引可读。

### 2.9 ES940 DiskBBQ 查询

当前 Lucene 10 读取侧没有移植完整 ES940 IVF 查询实现，而是使用 archive 中同时保存的 raw vector 做精确查询：

- 通过 `RandomVectorScorer` 创建查询 scorer。
- 将 accepted document bits 映射成 vector ordinal。
- 每批 64 条执行 `bulkScore()`。
- 使用 `ordToDoc()` 处理稀疏 vector field。
- 维护 visited count、competitive similarity 和 early termination。
- 支持 float 和 byte 查询入口。

该实现保证格式兼容和结果正确，但查询复杂度约为 `O(N × dimensions)`/shard，当前 `num_candidates` 不会获得正常 DiskBBQ IVF 的候选裁剪收益。

## 3. 每轮迭代差异与决策记录

以下按可以独立验收的工程轮次整理。每轮均记录触发现象、工程判断、相对上一轮的修改以及结果。

### 3.1 首版：零拷贝 mount 架构

**触发现象/目标**

Paimon 表已经由 Spark 生成 ESLib Global Index，需要让 Elasticsearch 直接搜索湖上的 Lucene 文件。

**工程判断**

如果重新导入到普通 Elasticsearch 索引，会产生重复存储、长导入时间和 snapshot 一致性问题。应直接复用 archive 中的 Lucene segment。

**主要修改**

- 新增 `POST /_paimon/mount`。
- 扫描 Paimon snapshot 的 live `es-index` manifest。
- 增加 archive metadata、row range、字段布局校验。
- 一个 archive 映射一个 primary shard。
- 增加自定义 Directory、Store 和 ReadOnlyEngine。
- 新建 snapshot-specific 物理索引并切换 alias。

**结果**

建立了“控制面规划 + 数据面零拷贝读取”的完整架构骨架。

### 3.2 Gradle 编译和版本对齐

**触发现象**

- `:paimon-store:compileJava` 找不到 Lucene 类。
- 依赖图中出现 Paimon 1.x，而 Spark 侧实际使用 Paimon 2.0.0。
- Elasticsearch 9.4/Lucene 10/JDK 21 与原编译图不一致。

**工程判断**

需要分开处理“编译需要哪些 API”和“插件运行包应该携带哪些 JAR”，不能为了解决编译缺类而把 Elasticsearch/Lucene 运行时全部打进插件。

**主要修改**

- 对齐 Elasticsearch 9.4.0、Lucene 10.4.0、Paimon 2.0.0、Java 21。
- 保留完整 Elasticsearch `compileOnly` 依赖图。
- Elasticsearch/Lucene API 只参与编译，不进入插件 ZIP。
- 根项目支持 Lucene 9/10 双 profile。
- `eslib-core` 增加 Lucene 10 专用源码和测试集。

**结果**

插件编译环境与目标 Elasticsearch 节点 ABI 对齐，同时没有提前制造运行时 Jar Hell。

### 3.3 生成 Kubernetes 测试环境

**触发现象/目标**

需要基于已有 Spark/Paimon 表快速验证插件安装、mount、mapping、shard 和查询。

**工程判断**

部署模板必须使用具体表根目录，而不是只填写 warehouse；OSS 凭据必须通过 Kubernetes Secret 和 Elasticsearch keystore 注入。

**主要修改**

- 提取目标表：`oss://cy-test2/spark/default.db/my_table_3`。
- 配置 endpoint：`https://oss-cn-shanghai.aliyuncs.com`。
- 增加 Namespace、Service、StatefulSet、PVC、ConfigMap 和 mount Job。
- 增加 Secret 示例，但不把 Secret 加入 Kustomize resources。
- 增加 plugin、alias、mapping、shard 和查询验证命令。

**结果**

形成了可重复执行的单节点 K8s 功能验证环境。

### 3.4 Docker 插件安装 Jar Hell

**触发现象**

Elasticsearch 插件安装失败：

```text
jar1: Elasticsearch/lib/lz4-java-1.10.1.jar
jar2: plugins/paimon-store/lz4-java-1.10.4.jar
class: net.jpountz.lz4.LZ4BlockInputStream$1
```

进一步扫描还发现：

- `paimon-api` 已嵌入 shade 类，继续携带独立 `paimon-shade-*` 会产生大量重复类。
- `eslib-simdvec` 与 Elasticsearch 自带 simdvec 模块重复。
- Elasticsearch、Lucene、Log4j 等 node-provided JAR 不能重复打包。

**工程判断**

依赖冲突应该按“运行时所有权”解决，而不是简单切换版本：

- Elasticsearch 提供 Lucene、LZ4、simdvec 和日志系统。
- Paimon API 已提供其内嵌 shade 类。
- 插件只携带目标节点确实没有的运行类。

**主要修改**

`bundlePlugin` 排除：

- `lz4-java-*`
- `eslib-simdvec-*`
- `paimon-shade-*`
- `paimon-oss-*`
- Hadoop
- Elasticsearch/Lucene/Log4j
- JaCoCo agent

同时增加 bundle 后置检查，禁用 JAR 出现时直接让构建失败。

**结果**

- 初始包内部重复类曾达到 4,086。
- 清理后插件内部重复类为 0。
- 插件与 Elasticsearch 9.4 运行时重复类为 0。
- 用户后续 `_cat/plugins` 已显示 `paimon-store 1.0.7`，证明安装阶段通过。

### 3.5 所有模块无条件 include 与 IDE 同步

**触发现象**

用户要求项目始终包含：

- `eslib-core`
- `eslib-simdvec`
- `paimon-store`

原 `paimon-store/build.gradle` 在项目求值阶段直接检查 `-Plucene=10`，导致普通 IntelliJ Gradle sync 也失败。

**工程判断**

“项目始终可导入”和“插件产物必须使用 Lucene 10”是两个不同约束，应该分别处理。

**主要修改**

- `settings.gradle` 无条件 include 三个模块。
- 将 profile 检查移动到 `verifyLucene10Profile` task。
- JavaCompile 和 `bundlePlugin` 依赖该校验 task。
- IDE 模型导入阶段不再抛异常。

**结果**

IntelliJ 可以正常同步整个仓库；真正编译或打包 `paimon-store` 时仍强制使用 `-Plucene=10`。

### 3.6 initContainer 的 `cp -a` 权限错误

**触发现象**

```text
cp: preserving times for '/mnt/config/.': Operation not permitted
```

**工程判断**

`cp -a` 会尝试保留源目录时间戳和属性。initContainer 以非 root 用户运行，对 EmptyDir 挂载根执行属性保留会触发 EPERM。这里没有必要提升容器为 root。

**主要修改**

- `cp -a` 改为 `cp -R --no-preserve=all`。
- 单文件复制也使用 `--no-preserve=all`。
- 明确 `runAsUser`、`runAsGroup` 和 `fsGroup`。
- 在 Pod EmptyDir 中创建全新的 Elasticsearch keystore。
- 增加配置、Secret 非空和 keystore entry 检查。

**结果**

Pod 成功跨过 initContainer，Elasticsearch 主容器可以启动。

### 3.7 curl mount Job 无法创建容器及错误正文缺失

**触发现象**

```text
container has runAsNonRoot and image has non-numeric user (curl_user)
```

后续 mount 失败时，普通 `curl --fail` 只显示 `curl: (22)`，没有 Elasticsearch JSON 错误正文。

**工程判断**

应使用明确数字 UID 消除 Kubernetes 对命名用户的歧义，同时保留非 root 安全策略。可观测性也属于修复的一部分，否则无法定位后续 500 根因。

**主要修改**

- Job 设置 `runAsUser: 1000`、`runAsGroup: 1000`。
- 保留 `runAsNonRoot`、只读根文件系统和 drop ALL capabilities。
- curl 改为 `--fail-with-body`。
- 输出 HTTP status、插件、alias 和 shard 状态。

**结果**

Job 可以启动，后续 mount 500 的真实异常能够直接从 Job 日志中看到。

### 3.8 `Keystore is closed`

**触发现象**

```text
IllegalStateException: Keystore is closed
```

**工程判断**

Elasticsearch 在插件构造完成后会关闭底层 `SecureSettings`。原实现在 REST 或 shard 延迟执行阶段才读取 Secret，违反了其生命周期约束。

**主要修改**

- 插件构造期间读取并 clone `SecureString`。
- 将插件生命周期内的副本传给 planner 和 archive provider。
- 插件关闭时显式关闭 Secret 副本。
- Secret 仍不进入 index settings 或 cluster state。

**结果**

延迟执行的 mount 和 shard 创建可以安全取得 OSS Secret，错误继续推进到 Paimon FileIO 层。

### 3.9 `create_class_loader` entitlement

**触发现象**

```text
NotEntitledException:
component [paimon-store]
class [org.apache.paimon.plugin.ComponentClassLoader]
entitlement [create_class_loader]
```

**工程判断**

Paimon 默认通过组件发现机制动态加载 `paimon-oss`，但 mount planner 实际只需要只读元数据访问。没有必要扩大插件的 classloader 权限。

**中间修改**

- 将 `paimon-oss` 制作为可静态加载的 JAR。
- planner 直接装载 OSS FileIO，暂时绕过 `ComponentClassLoader`。

**结果**

成功跨过 `create_class_loader`，但继续暴露 Hadoop/JDK 21 的兼容问题。该中间方案随后被完全替换。

### 3.10 `getSubject is not supported`

**触发现象**

```text
UnsupportedOperationException: getSubject is not supported
```

**工程判断**

静态加载 `paimon-oss` 仍会委托 Hadoop 3.3.4。Hadoop 在 Elasticsearch JDK 21 环境调用了已经不受支持的 JAAS `Subject.getSubject`。继续修补 Hadoop 会带来更大的运行时和 entitlement 风险。

**主要修改**

- 放弃 `paimon-oss-static` 中间方案。
- 新增直接基于 Aliyun OSS SDK 的只读 `PaimonOssFileIO`。
- `oss://` 表使用 `PaimonOssFileIO`。
- local/file 表使用 Paimon `LocalFileIO`。
- 最终插件完全排除 `paimon-oss` 和 Hadoop。

**结果**

同时解决动态 classloader、JDK API 和依赖体积问题。下一次执行已真正请求 OSS，证明该层通过。

### 3.11 OSS `SignatureDoesNotMatch`

**触发现象**

OSS 返回签名不匹配。检查 Kubernetes Secret 后发现：

- `access-key-id` Base64 解码后是一段 shell 命令文本。
- `access-key-secret` 为空。

**工程判断**

应先验证实际输入 SDK 的 Secret，而不是在没有证据时修改签名算法。

**主要修改**

- 无核心 Java 修改。
- 通过 `kubectl create secret generic --from-literal=...` 重建 Secret。
- endpoint 明确为 `https://oss-cn-shanghai.aliyuncs.com`。
- initContainer 增加 Secret 非空检查。
- Secret 更新后重启 Elasticsearch，使插件重新读取凭据。

**结果**

错误继续推进到 Paimon schema 的 Parquet factory，证明 OSS 鉴权通过。

### 3.12 缺少 Parquet `FileFormatFactory`

**触发现象**

```text
Could not find any factory for identifier 'parquet'
that implements FileFormatFactory in the classpath
```

**工程判断**

即使 mount 只读取 metadata/global index，Paimon 在打开表时仍会验证 schema 中声明的数据格式。必须提供 Parquet SPI，但不能把已经移除的 Hadoop 和重复依赖重新带回来。

**第一版尝试**

直接加入原始 `paimon-format-2.0.0.jar`。

**防回归发现**

原始 format JAR 与 `paimon-common`、`zstd-jni` 存在 44 个重复类，会重新触发 Jar Hell。

**最终修改**

- `paimon-format` 使用 `transitive=false`。
- 新增 `preparePaimonFormatJar`。
- 生成 `paimon-format-es-2.0.0.jar`。
- 保留 `FileFormatFactory` SPI 和 `ParquetFileFormatFactory`。
- 删除重复 Zstd、Aircompressor 和 LZO 类。
- bundle 禁止原始 `paimon-format-*`，只允许 cleaned format。

**结果**

Parquet factory 可发现，同时插件重复类仍保持为 0。执行路径继续进入 index manifest 的 Zstd 解压。

### 3.13 Zstd native entitlement 和进程保护

**触发现象**

Paimon Avro index manifest 解压时触发：

```text
NotEntitledException: load_native_libraries
ExceptionInInitializerError
```

静态初始化错误还可能从 Elasticsearch generic thread 逃逸。

**工程判断**

Zstd native load 是读取真实 manifest 必需的能力，应精确授权。另一方面，依赖初始化错误应该转换为可诊断 REST 失败，不能让错误逃逸到 Elasticsearch 线程边界。

**主要修改**

- entitlement 增加 `load_native_libraries`。
- 只保留一份 `zstd-jni-1.5.5-11.jar`。
- cleaned `paimon-format-es` 继续删除重复 Zstd 类。
- `RestPaimonMountAction` 捕获 `LinkageError` 并包装为 `IOException`。
- bundle 校验 entitlement 必须包含全部必要权限。

**结果**

- Zstd native round-trip 通过。
- Linux amd64 `.so` 已存在于 zstd JAR。
- Jar Hell 仍为 0。
- mount 执行继续进入 Lucene shard 和查询阶段。

日志中的 `SLF4J StaticLoggerBinder` 是非致命 NOP logger warning。没有为消除 warning 再加入 binder，以免与 Elasticsearch 日志系统制造新的依赖冲突。

### 3.14 `_source` 与原始字段返回

**触发现象**

Mapping 中 `_source.enabled=false`，用户需要返回 `content`、`category` 和 `price`。

**工程判断**

当前 archive 中没有可直接作为 Elasticsearch `_source` 返回的 JSON，插件也没有通过 `rowRangeStart + luceneDocId` 回表 Paimon 做 row hydration。不能把“字段可搜索”误认为“完整源文档可返回”。

**主要处理**

- 保持 `_source=false`。
- `source_enabled=true` 在 mount API 中明确拒绝。
- 使用 `docvalue_fields` 返回：
  - `content.keyword`
  - `category`
  - `price`
- 结果位于 hit 的 `fields`。

**结果**

现有索引可以返回已配置 keyword/scalar DocValues 的字段，但完整源文档返回留待后续设计。

### 3.15 DiskBBQ 查询抛 `build-only`：r8

**触发现象**

```text
ES940DiskBBQVectorsFormat search is build-only in the standalone lib
```

**工程判断**

Lucene 10 的 `ES940MergeVectorsReader` 原本只服务离线构建/merge，查询方法直接抛异常。不能直接替换为 Elasticsearch 内部 DiskBBQ reader，因为湖上 SPI/header 名称和内部格式不完全相同，强行替换可能破坏已有 segment 兼容性。

**r8 修改**

- 保持磁盘格式、SPI 和依赖不变。
- 将查询委托给 raw `FlatVectorsReader.search()`。

**结果**

原来的 500 消失，shard 查询成功，但返回 0 hits。说明 r8 只消除了异常，尚未真正完成结果收集。

### 3.16 DiskBBQ 查询 0 hits：r9

**触发现象**

```text
_shards.successful = 1
hits.total.value = 0
```

**工程判断**

Lucene 10.4 的 `FlatVectorsReader.search()` 默认是空实现。它不会报错，也不会收集结果，因此 r8 是不完整修复。

**r9 修改**

- 不再委托默认 no-op search。
- 使用 `getRandomVectorScorer()` 显式实现 raw-vector exhaustive search。
- float/byte 查询入口分别处理。
- 使用 `getAcceptOrds()` 映射 accepted docs。
- 每批 64 条执行 `bulkScore()`。
- 使用 `ordToDoc()` 支持稀疏 vector field。
- 维护 visited count、competitive similarity 和 early termination。

**测试改进**

- 不再只依赖可能走其他查询分支的高层测试。
- 直接调用 `LeafReader.searchNearestVectors()`，确保进入真实 ES940 reader。
- 用例 1：5 个向量取 top 3，查询自身向量必须排第一。
- 用例 2：稀疏 vector field 只接受指定 doc，验证 AcceptDocs 和 ordinal/doc 映射。

**结果**

- DiskBBQ 定向测试 2/2 通过。
- `paimon-store` 回归测试 10/10 通过。
- Jar Hell 保持为 0。
- 当前 K8s 清单和文档使用 r9。

当前代价是按 shard 精确扫描，而不是完整 IVF/DiskBBQ ANN 查询。

### 3.17 `NoShardAvailableActionException` 与删除重挂

**触发现象**

普通 `match_all + docvalue_fields` 返回：

```text
NoShardAvailableActionException
HTTP 503
```

**工程判断**

这是物理 shard 未分配或旧挂载不可用，不是 `_source`、`docvalue_fields` 或查询 DSL 的语法问题。

**主要处理**

- 精确删除旧物理索引 `my_table_3_2`。
- 保留 StatefulSet 和 PVC，不删除整个 Elasticsearch 数据目录。
- 删除并重建 `paimon-mount` Job。
- 由当前插件镜像重新创建物理索引和 alias。

**结果与设计暴露点**

本轮没有核心源码变化，但说明 mount API 当前只完成物理索引创建和 alias 切换，并不保证 primary shard 已进入 `STARTED`。后续应在 mount Job 中增加 cluster health 等待，并检查 create-index 的 shard acknowledgement。

## 4. 为什么错误是逐层暴露，而不是前一轮修坏后一轮

实际执行路径是：

```text
Gradle 编译
 → 插件打包
 → Docker 安装
 → Pod init
 → mount Job
 → keystore
 → Paimon FileIO
 → OSS 鉴权
 → schema/Parquet 校验
 → manifest/Zstd 解压
 → Lucene shard 打开
 → DiskBBQ 查询
```

只有前一层成功，下一层代码才会真正执行。因此：

1. 编译图修正后，才能生成插件 ZIP。
2. Jar Hell 清理后，插件才能安装。
3. init/curl 安全上下文修正后，Pod 和 Job 才能运行。
4. keystore 生命周期修正后，REST 才能取得凭据。
5. 绕过动态 classloader 后，才暴露 Hadoop/JDK 不兼容。
6. 直连 OSS SDK 后，才真正验证 Kubernetes Secret。
7. Secret 正确后，才进入 Paimon schema validation。
8. Parquet factory 可发现后，才开始读取压缩 index manifest。
9. Zstd entitlement 正确后，才完成 mount 并进入 Lucene query。
10. DiskBBQ 异常移除后，才观察到 `FlatVectorsReader.search()` 的 no-op 行为。

其中有两次属于“中间方案解决当前层，但仍包含下一层问题”，最终均被替换：

- 静态 `paimon-oss` 虽绕过 classloader，但没有移除 Hadoop，最终被自研只读 OSS FileIO 替代。
- r8 虽消除 DiskBBQ `build-only` 异常，但委托到 no-op search，最终由 r9 的真实精确打分替代。

Parquet、Zstd 等修复均在重新打包前执行重复类扫描，避免解决一个问题后重新引入 Jar Hell。

## 5. 构建与依赖设计

### 5.1 项目模块

`settings.gradle` 始终无条件包含：

```groovy
include 'eslib-core'
include 'eslib-simdvec'
include 'paimon-store'
```

### 5.2 构建命令

```shell
./gradlew :paimon-store:bundlePlugin -Plucene=10 --rerun-tasks
```

`paimon-store` 始终存在于项目模型中，但 compile 和 bundle task 在执行时强制 Lucene 10 profile。

### 5.3 插件瘦包原则

插件不重复携带 Elasticsearch 节点已经提供的类。最终禁止进入 ZIP 的主要依赖包括：

- Elasticsearch server/runtime JAR
- Lucene JAR
- Log4j JAR
- `lz4-java`
- `eslib-simdvec`
- `paimon-oss`
- Hadoop
- 独立 `paimon-shade-*`

插件保留：

- `paimon-store`
- `eslib-core`
- Paimon 2.0 必需 API/core 类
- 清洗后的 `paimon-format-es-2.0.0.jar`
- Aliyun OSS SDK
- 唯一一份 `zstd-jni-1.5.5-11.jar`

### 5.4 Entitlement

当前插件 entitlement 包含：

- `manage_threads`
- `set_https_connection_properties`
- `outbound_network`
- `load_native_libraries`
- Elasticsearch `path.repo` 只读访问

OSS Secret 只存在于：

- Kubernetes Secret
- Elasticsearch keystore
- 插件生命周期内的 `SecureString` 副本

不会进入 mount REST body、index settings 或 cluster state。

## 6. 最终验证状态

当前构建产物：

```text
paimon-store/build/distributions/paimon-store-1.0.7.zip
```

本次终检结果：

- 文件大小：63,953,548 字节。
- JAR 数量：42。
- SHA-256：`7BDD2CE5B75AFF495DB047AF5596EEF6E5A029DE3C285F6DC45E1F104452E503`。
- 插件内部重复类：0。
- 插件与 Elasticsearch 9.4 运行时重复类：0。
- `PluginJarHellCheck`：`Plugin/server duplicate classes: 0`。
- `jdeps --missing-deps`：`eslib-core` 和 `paimon-store` 均无缺失输出。
- `paimon-store` 回归测试：10/10 通过。
- DiskBBQ r9 定向查询测试：2/2 通过。
- K8s YAML：全部能够解析。
- Secret 未进入 Kustomize resources。
- 当前 K8s 清单、README 和镜像引用均为 r9。
- `zstd-jni` 中存在 Linux amd64 native library。

需要保持审慎的一点：当前还没有保存“r9 镜像在真实 OSS/K8s 环境返回非空 kNN hits”的最终验收日志。本地 reader 正确性已经验证，但真实跨版本端到端闭环仍应作为最终验收项。

## 7. 当前设计权衡

### 7.1 零拷贝

**收益**

- 挂载速度快。
- Elasticsearch 本地磁盘占用很小。
- 不需要重新索引已有湖上数据。
- snapshot 与物理索引一一对应。

**代价**

- 查询延迟和可用性依赖 OSS。
- archive 必须在 mount 生命周期内保持可用且不可变。
- 精确向量扫描可能读取大量远端 raw vector 数据。

### 7.2 一 archive 一 shard

**收益**

- 映射简单稳定。
- 不复制、不拆分 archive。
- 保持全局 row-id 计算关系。

**代价**

- shard 数受 archive 数量控制。
- archive 或 segment 文件很多时，`ShardMountSpec` 和 cluster-state 体积可能增大。

### 7.3 `number_of_replicas=0`

**收益**

- 避免 peer recovery 把远端 segment 复制到本地。
- 保持 mount 的零拷贝语义。

**代价**

- 当前没有 Elasticsearch replica 容灾。
- 单节点或节点故障时 shard 会直接不可用。

### 7.4 Snapshot 物理索引与 alias

**收益**

- 新 snapshot 新建物理索引。
- alias 切换清晰、一致性强。
- 旧 snapshot 不会被原地修改。

**代价**

- 当前不支持原地 refresh。
- 重挂同一 snapshot 会因物理索引已存在而失败。
- 旧物理索引需要额外生命周期清理。

### 7.5 `_source=false`

**收益**

- 无需重复存储完整源文档。
- 不需要在第一阶段实现复杂的 Paimon 行回源。

**代价**

- 只能返回已经建立 DocValues 的字段。
- 无法直接返回完整原始行。
- 当前 row range 尚未注入 Elasticsearch hit 响应。

### 7.6 DiskBBQ 精确回退

**收益**

- 保持已有湖上文件格式不变。
- 查询结果正确。
- 不依赖 Elasticsearch 内部私有 DiskBBQ 实现。

**代价**

- 每个 shard 需要扫描全部 raw vector。
- `num_candidates` 当前不能降低主要扫描成本。
- 大 shard、高维向量和高并发时会产生明显的 OSS I/O 与 CPU 压力。

## 8. 已知限制和技术债

1. 当前 ES940 DiskBBQ 查询是精确扫描，不是完整 IVF 查询。
2. 当前测试不是完整的“Lucene 9.12 写 archive → OSS → ES 9.4 REST kNN”自动化闭环。
3. `bundlePlugin` 当前不自动依赖全部测试和官方 Elasticsearch Docker 安装测试。
4. mount 返回成功不等于 primary shard 已经进入 `STARTED`。
5. `_cat/plugins` 只能看到 `1.0.7`，不能区分 r7/r8/r9。
6. `imagePullPolicy: IfNotPresent` 配合同标签重建可能继续使用旧镜像缓存。
7. OSS shard-open 尚未通过 HEAD 校验 Content-Length、ETag 或 VersionId。
8. `_source` 始终关闭，尚未实现 Paimon row hydration。
9. `return_fields` 目前列为允许请求字段，但尚未解析和使用。
10. `storage_mode` 当前只做兼容性校验，实际 provider 仍由 archive URI 决定。
11. 旧 snapshot 物理索引不会自动删除。
12. OSS 凭据轮换后需要重启 Elasticsearch 节点。
13. K8s 示例关闭了 Elasticsearch Security，只适用于测试。
14. `ES940DiskBBQVectorsFormat` 部分旧注释仍称 reader 不能查询，与 r9 当前实现不一致。
15. SLF4J 1.7 当前没有 binding，第三方库会输出 NOP logger warning，功能不受影响但其日志不可见。

## 9. 生产化建议与优先级

### P0：最终功能验收

1. 增加真实跨版本端到端测试：
   - Lucene 9.12/Spark 写入。
   - Paimon snapshot 和 ESLib archive 写入 OSS。
   - Elasticsearch 9.4 安装当前 ZIP。
   - REST mount。
   - 验证全文、标量和非空 kNN hits。
2. mount 后等待物理索引 primary shard `STARTED`。
3. 检查 `CreateIndexResponse.isAcknowledged()` 和 `isShardsAcknowledged()`。
4. 将最终 K8s 日志、mapping、alias、shard 和查询响应作为验收记录保存。

### P1：CI 与交付可靠性

1. CI 固定执行 Lucene 9 全测试。
2. CI 固定执行 Lucene 10 全测试。
3. 构建插件 ZIP。
4. 对官方 Elasticsearch 9.4 distribution 执行插件安装/Jar Hell 检查。
5. 执行 Testcontainers 或真实 Elasticsearch REST smoke test。
6. 使用不可变镜像 digest 或唯一 CI build tag。
7. 在插件 manifest 或 OCI label 中写入源码 revision。

### P1：查询性能

1. 移植兼容 `PaimonES940DiskBBQVectorsFormat` 的 Lucene 10 IVF reader。
2. 利用 centroid/cluster 文件执行候选裁剪。
3. 增加 OSS 批量 Range GET、prefetch 和 ByteBudget。
4. 建立向量数、维度、候选数、Range 请求数、读取字节数和 P95/P99 延迟基准。

### P2：数据一致性与生命周期

1. OSS shard-open 增加 Content-Length、ETag 或 VersionId 校验。
2. 将 Paimon snapshot retention 与 Elasticsearch mount 生命周期关联。
3. 增加旧 snapshot 物理索引清理策略。
4. 评估 archive 数量到 shard 数量的治理方式。
5. 评估可保持零拷贝语义的容灾或 replica 方案。

### P2：结果返回能力

可以选择以下一种方案：

1. 写入端把 stored fields 或 source payload 一起写入 archive。
2. 查询端通过 `globalRowId = rowRangeStart + luceneDocId` 回表 Paimon，并拼装 synthetic source。

第二种方案节省索引存储，但需要设计批量回源、snapshot 一致性、超时和失败降级。

## 10. 关键文件索引

### 构建与版本

- `settings.gradle`
- `build.gradle`
- `eslib-core/build.gradle`
- `paimon-store/build.gradle`
- `paimon-store/src/main/resources/plugin-descriptor.properties`

### 写入与元数据协议

- `eslib-core/src/main/java/org/elasticsearch/eslib/builder/DefaultESIndexBuilder.java`
- `eslib-core/src/main/java/org/elasticsearch/eslib/api/ESIndexBuilder.java`
- `eslib-core/src/main/java/org/elasticsearch/eslib/mount/ESIndexArchiveMetadata.java`
- `eslib-core/src/main/java/org/elasticsearch/eslib/mount/LakeShardDescriptor.java`
- `eslib-core/src/main/java/org/elasticsearch/eslib/mount/PaimonMountPlan.java`

### Mount 控制面

- `paimon-store/src/main/java/org/elasticsearch/paimon/RestPaimonMountAction.java`
- `paimon-store/src/main/java/org/elasticsearch/paimon/PaimonSnapshotPlanner.java`
- `paimon-store/src/main/java/org/elasticsearch/paimon/ShardMountSpec.java`
- `paimon-store/src/main/java/org/elasticsearch/paimon/ElasticsearchMappingBuilder.java`

### Store、Engine 与 OSS

- `paimon-store/src/main/java/org/elasticsearch/paimon/PaimonStorePlugin.java`
- `paimon-store/src/main/java/org/elasticsearch/paimon/MountArchiveProviderFactory.java`
- `paimon-store/src/main/java/org/elasticsearch/paimon/SwitchableMountDirectory.java`
- `paimon-store/src/main/java/org/elasticsearch/paimon/PaimonReadOnlyEngineFactory.java`
- `paimon-store/src/main/java/org/elasticsearch/paimon/PaimonOssFileIO.java`
- `eslib-core/src/main/java/org/elasticsearch/eslib/io/ArchiveDirectory.java`
- `eslib-core/src/main/java/org/elasticsearch/eslib/io/ArchiveIndexInput.java`
- `eslib-core/src/main/java/org/elasticsearch/eslib/io/OSSArchiveDataProvider.java`

### Lucene 兼容与 DiskBBQ

- `eslib-core/src/lucene10/java/org/elasticsearch/eslib/adapter/lucene10/PaimonLucene9Codec.java`
- `eslib-core/src/lucene10/java/org/elasticsearch/eslib/diskbbq/es94/ES940MergeVectorsReader.java`
- `eslib-core/src/lucene10Test/java/org/elasticsearch/eslib/diskbbq/es94/ES940DiskBBQSearchTest.java`

### 权限与部署

- `paimon-store/src/main/plugin-metadata/entitlement-policy.yaml`
- `deploy/k8s/Dockerfile`
- `deploy/k8s/statefulset.yaml`
- `deploy/k8s/configmap.yaml`
- `deploy/k8s/mount-job.yaml`
- `deploy/k8s/kustomization.yaml`
- `deploy/k8s/README.md`

## 11. 总体评价

本项目已经完成从“概念验证”到“可运行功能验证”的关键跨越：

- Paimon snapshot 能够转化为 Elasticsearch 的只读 shard 拓扑。
- Lucene segment 保持在数据湖中，Elasticsearch 只保存必要的本地 commit overlay。
- Lucene 9 写、Lucene 10 读的兼容路径已经建立。
- OSS、Elasticsearch entitlement、JDK 21、Paimon SPI 和插件 Jar Hell 等主要集成障碍已被逐层解决。
- DiskBBQ 已从完全不可查询推进到结果正确的精确查询。

下一阶段不应再以“继续增加依赖或扩大权限”为主，而应集中处理：

1. 自动化跨版本端到端验收。
2. mount 后 shard readiness 保证。
3. DiskBBQ IVF 远端查询性能。
4. source/row hydration。
5. snapshot、archive、shard 和镜像的生产生命周期治理。
