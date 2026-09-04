# ES-Paimon-Lib

ES-Paimon-Lib is a standalone Lucene-based indexing library used by the optional
[Apache Paimon](https://paimon.apache.org/) ESLib global-index integration. It supports vector,
full-text, and scalar indexes without depending on Elasticsearch server classes.

This public repository contains the source corresponding to the externally published Maven
artifacts. It is maintained independently of the Apache Software Foundation and is not an ASF
release repository.

## Modules

| Module | Purpose |
|---|---|
| `eslib-core` | Public APIs, index builders/searchers, archive IO, HNSW, DiskBBQ, full-text, and scalar indexes |
| `eslib-simdvec` | Standalone fallback implementation for Elasticsearch SIMD-vector APIs used by DiskBBQ |
| `paimon-store` | Elasticsearch 9.4 plugin that mounts Paimon ESLib archives as read-only shards (Lucene 10 profile only) |

The former `eslib-api` module has been merged into `eslib-core`.

## Build

The `lucene` Gradle property selects the Lucene source set and Java target:

```shell
# Release line used by Apache Paimon: Lucene 9.12, JDK 11+
./gradlew clean test -Plucene=9

# Lucene 10.4 compatibility line: JDK 21+
./gradlew clean test -Plucene=10
```

## Mount a Paimon index in Elasticsearch

The `paimon-store` plugin implements a zero-copy, point-in-time mount. It reads the selected
Paimon snapshot's live `es-index` Global Index entries, orders them by inclusive row-id range, and
maps every archive to exactly one Elasticsearch primary shard. The archive's own Lucene files
stay on the lake; Elasticsearch reads their byte ranges through `ArchiveDirectory` and stores only
a small bootstrap commit in the node data path.

The target combination is Elasticsearch 9.4.0, Lucene 10.4.0, Apache Paimon 2.0.0, and JDK 21.
Build the installable plugin archive with:

```shell
./gradlew :paimon-store:bundlePlugin -Plucene=10
```

Install `paimon-store/build/distributions/paimon-store-1.0.7.zip` on every Elasticsearch data node
and restart the cluster. For an OSS table, configure the endpoint and access-key ID in
`elasticsearch.yml`, and keep the secret in the Elasticsearch keystore:

```yaml
paimon.oss.endpoint: https://oss-cn-hangzhou.aliyuncs.com
paimon.oss.access_key_id: ${PAIMON_OSS_ACCESS_KEY_ID}
```

```shell
bin/elasticsearch-keystore add paimon.oss.access_key_secret
```

For a local/shared-filesystem table, put its root in Elasticsearch's `path.repo`; the plugin's
entitlement policy and runtime checks both restrict local reads to those roots.

Mount the latest snapshot (or pass `snapshot_id` for a fixed snapshot):

```http
POST /_paimon/mount
{
  "table_path": "oss://example-bucket/warehouse/catalog.db/items",
  "index_name": "items_search",
  "vector_field_name": "embedding",
  "storage_mode": "remote",
  "source_enabled": true,
  "auth_type": "node"
}
```

The endpoint creates a numbered physical index such as `items_search_42`, then atomically points
the stable `items_search` alias at it. The index uses `index.store.type=paimon`, is write-blocked,
has one ES shard per live Paimon ESLib archive, and has no replicas so peer recovery cannot copy
the lake segments. Lucene document ID `d` in a mounted shard maps to Paimon's absolute row ID as
`rowRangeStart + d`.

When `source_enabled=true`, the search fetch phase converts each shard-local Lucene document ID
to its absolute Paimon Row-ID and reads the complete row from the same fixed snapshot. An optional
`return_fields` array limits the returned top-level fields. The plugin does not accept request-body
OSS credentials, refresh an existing mount in place, or create replicas. A new Paimon snapshot is
mounted as a new physical index and switched through the alias. See
[`paimon-store/README.md`](paimon-store/README.md) for the API, security model, and failure
semantics. A single-node OSS test deployment, image recipe, mount Job, and Spark metadata
inspection SQL are available in [`deploy/k8s/README.md`](deploy/k8s/README.md).

To write Maven artifacts to a local directory, publish each Lucene line separately. Their artifact
IDs include the Lucene major version, so both lines can coexist in the same Maven repository:

```shell
./gradlew publish -Plucene=9 -PreleaseRepositoryDir=/absolute/output/path
JAVA_HOME=/path/to/jdk-21 ./gradlew publish -Plucene=10 -PreleaseRepositoryDir=/absolute/output/path
```

## Maven artifacts

Each released version is built from its matching source tag (for example, `eslib-1.0.7`). Binary
artifacts and checksums are stored in the public
[es-paimon-lib-releases](https://github.com/CrownChu/es-paimon-lib-releases) repository.

| Profile | Core artifact | SIMD artifact | Runtime |
|---|---|---|---|
| Lucene 9 | `eslib-core-lucene9` | `eslib-simdvec-lucene9` | JDK 11+, Lucene 9.12.0 |
| Lucene 10 | `eslib-core-lucene10` | `eslib-simdvec-lucene10` | JDK 21+, Lucene 10.4.0 |

```xml
<repositories>
    <repository>
        <id>eslib-github</id>
        <url>https://raw.githubusercontent.com/CrownChu/es-paimon-lib-releases/eslib-1.0.7/repository</url>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>io.github.paimon.eslib</groupId>
        <artifactId>eslib-core-lucene9</artifactId>
        <version>1.0.7</version>
    </dependency>
</dependencies>
```

`eslib-core-lucene9` transitively depends on `eslib-simdvec-lucene9`. For a JDK 21 / Lucene 10
runtime, use `eslib-core-lucene10`; it transitively selects `eslib-simdvec-lucene10` and Lucene
10.4.0. Do not mix artifacts from the two Lucene lines in one runtime.

## Licensing

Elasticsearch-derived portions are redistributed under the Elastic License 2.0. Files derived
from Apache Lucene remain under the Apache License 2.0. See [LICENSE.txt](LICENSE.txt),
[NOTICE.txt](NOTICE.txt), and the complete texts in [licenses](licenses/).

Every published JAR includes the same license and notice files under `META-INF/`.
