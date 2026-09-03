# Elasticsearch Paimon mount：Kubernetes 单节点测试

这套文件把 Spark 写到 OSS 的 Paimon 2.0.0 ESLib Global Index，按 archive 的全局行号范围映射成 Elasticsearch shard。ES 只在本地保存少量引导元数据；Lucene segment 仍留在 OSS，并通过 HTTP Range GET 读取。

已从 Spark 启动参数提取并写入模板：

- Paimon：2.0.0
- OSS warehouse：`oss://cy-test2/spark`
- OSS endpoint：`https://oss-cn-shanghai.aliyuncs.com`
- 写入端 Lucene：9.12.0
- 挂载端 Elasticsearch：9.4.0 / Lucene 10.4.0 / JDK 21

## 1. 提取表路径、快照和字段

目标表已经配置为 `oss.default.my_table_3`，对应 warehouse 路径
`oss://cy-test2/spark/default.db/my_table_3`。在现有 Spark 3.5.1 环境执行：

```shell
bin/spark-sql \
  --jars /opt/spark/libext/aliyun-sdk-oss-3.17.4.jar,/opt/spark/libext/eslib-core-lucene9-1.0.7.jar,/opt/spark/libext/eslib-simdvec-lucene9-1.0.7.jar,/opt/spark/libext/lucene-analysis-common-9.12.0.jar,/opt/spark/libext/lucene-codecs-9.12.0.jar,/opt/spark/libext/lucene-core-9.12.0.jar,/opt/spark/libext/lucene-queries-9.12.0.jar,/opt/spark/libext/lucene-queryparser-9.12.0.jar,/opt/spark/libext/paimon-eslib-2.0.0.jar,/opt/spark/libext/paimon-oss-2.0.0.jar,/opt/spark/libext/paimon-spark-3.5_2.12-2.0.0.jar \
  --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
  --conf spark.sql.catalog.oss=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.oss.warehouse=oss://cy-test2/spark \
  --conf spark.sql.catalog.oss.fs.oss.endpoint=oss-cn-shanghai.aliyuncs.com \
  --conf spark.sql.catalog.oss.fs.oss.accessKeyId="$OSS_ACCESS_KEY_ID" \
  --conf spark.sql.catalog.oss.fs.oss.accessKeySecret="$OSS_ACCESS_KEY_SECRET" \
  -f deploy/k8s/inspect-paimon.sql
```

把 `DESCRIBE TABLE EXTENDED` 输出的完整 `Location` 填入 `configmap.yaml` 的 `mount.json.table_path`。不要只填 warehouse；这里必须是具体表根目录。最后一条 `$table_indexes` 查询应返回 `index_type = es-index` 的记录；这些记录的 `row_range_start/end` 就是后续 shard 的行号边界。如果没有记录，需要先检查 Spark 写表时 ESLib Global Index 是否已成功提交。

默认挂载最新快照。如果要固定快照或同一快照内有多个 ESLib 索引，可在 `mount.json` 增加：

```json
"snapshot_id": 42,
"vector_field_name": "embedding"
```

## 2. 构建插件镜像

在仓库根目录执行：

```shell
./gradlew :paimon-store:bundlePlugin -Plucene=10 --rerun-tasks
jar tf paimon-store/build/distributions/paimon-store-1.0.7.zip \
  | grep -E 'paimon-format|paimon-oss|hadoop|lz4-java|eslib-simdvec|paimon-shade'
docker build --no-cache -f deploy/k8s/Dockerfile -t es-paimon:9.4.0-1.0.7-r9 .
```

`Dockerfile` 会把 `paimon-store-1.0.7.zip` 安装到官方 Elasticsearch 9.4.0 镜像。远端集群请把镜像推到仓库，并修改 `kustomization.yaml` 的 `newName`；kind 或 minikube 可直接把本地镜像加载到集群。

部署前可直接检查镜像内是否为新版格式包：

```shell
docker run --rm --entrypoint sh es-paimon:9.4.0-1.0.7-r9 \
  -c 'ls -l /usr/share/elasticsearch/plugins/paimon-store/paimon-format-es-2.0.0.jar && \
      grep -A8 "ALL-UNNAMED" /usr/share/elasticsearch/plugins/paimon-store/entitlement-policy.yaml'
```

不要复用已经加载到节点的旧镜像标签。清单使用 `imagePullPolicy: IfNotPresent` 以支持本地
测试镜像，因此同标签重建后 Kubernetes 仍可能继续使用节点缓存。

上面的 ZIP 检查应只输出 `paimon-format-es-2.0.0.jar`。插件不能携带 `paimon-oss`、Hadoop、`lz4-java`、
`eslib-simdvec` 或独立的 `paimon-shade-*` JAR。挂载规划通过阿里云 OSS SDK 的只读
Paimon `FileIO` 直接读取元数据，避免 Paimon 动态类加载器和 Hadoop 3.3.4 在新版 JDK
上调用已失效的 `Subject.getSubject`。其余重复类由 Elasticsearch 或 Paimon 自身提供，
重复打包会在安装阶段触发 `jar hell`。若检查出现匹配项，说明仍在使用旧插件包；请
确认 Gradle 命令成功完成，并保持 Docker 构建使用 `--no-cache`。

Paimon 的 Avro index manifest 可能使用 Zstandard 压缩。插件保留独立的
`zstd-jni-1.5.5-11.jar`，并在 `entitlement-policy.yaml` 中申请
`load_native_libraries`。不要把 `paimon-format` 内重复的 Zstd 类恢复回来，否则安装阶段
会重新触发 Jar Hell。Elasticsearch 默认已授予插件临时目录读写权限，Zstd 解包本地库
不需要额外扩大文件访问范围。

## 3. 创建 OSS Secret 并部署 ES

Secret 不在 Kustomize 清单内，避免把访问密钥提交到仓库。以下命令只把值写入 Kubernetes Secret：

```shell
kubectl apply -f deploy/k8s/namespace.yaml
kubectl -n es-paimon-test create secret generic paimon-oss-credentials \
  --from-literal=access-key-id="$OSS_ACCESS_KEY_ID" \
  --from-literal=access-key-secret="$OSS_ACCESS_KEY_SECRET" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -k deploy/k8s
kubectl -n es-paimon-test rollout status statefulset/es-paimon --timeout=10m
kubectl -n es-paimon-test logs statefulset/es-paimon -c elasticsearch
```

如果 Pod 停在 initContainer，先查看真正的失败输出（`describe` 只显示退出码）：

```shell
kubectl -n es-paimon-test logs pod/es-paimon-0 \
  -c prepare-config-and-keystore --previous
```

若集群没有默认 StorageClass，PVC 会保持 Pending；请在 `statefulset.yaml` 的 `volumeClaimTemplates` 中补充 `storageClassName`。Kubernetes 节点还应满足 Elasticsearch 的 `vm.max_map_count` 要求。

## 4. 执行 mount 并验证 shard

`configmap.yaml` 的 `mount.json` 已配置为 `default.db/my_table_3`，执行：

```shell
kubectl apply -f deploy/k8s/configmap.yaml
kubectl -n es-paimon-test delete job paimon-mount --ignore-not-found
kubectl apply -f deploy/k8s/mount-job.yaml
kubectl -n es-paimon-test logs -f job/paimon-mount
```

Mount 请求使用 `--fail-with-body`，因此 Elasticsearch 返回 4xx/5xx 时，Job 日志会保留
完整 JSON 错误正文和 HTTP 状态，而不只是显示 `curl: (22)`。

成功响应会包含物理索引、快照和 shard 数，例如：

```json
{"acknowledged":true,"alias":"my_table_3","index":"my_table_3_42","snapshot_id":42,"shards":8}
```

每个 live `es-index` archive 对应一个 ES primary shard。索引为只读、`number_of_replicas=0`，稳定 alias 指向带快照号的物理索引。

本地访问：

```shell
kubectl -n es-paimon-test port-forward service/es-paimon 9200:9200
curl 'http://127.0.0.1:9200/_cat/plugins?v'
curl 'http://127.0.0.1:9200/_cat/aliases?v'
curl 'http://127.0.0.1:9200/_cat/shards/my_table_3?v'
curl 'http://127.0.0.1:9200/my_table_3/_mapping?pretty'
curl 'http://127.0.0.1:9200/my_table_3/_search?size=0&pretty'
```

同一 alias 挂载更新快照时，修改 `snapshot_id`（或省略以取最新），重新应用 ConfigMap，再删除并重建 Job。重复挂载同一个快照会因为同名物理索引已存在而失败，这是当前实现的保护行为。

这是测试配置，显式关闭了 Elasticsearch Security。不要原样用于生产环境。
