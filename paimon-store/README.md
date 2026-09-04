# Paimon Store plugin

`paimon-store` is an Elasticsearch 9.4 store and engine plugin for mounting immutable Apache
Paimon ESLib Global Index files. It does not copy the archived Lucene segment files into the
Elasticsearch data directory.

## Data path

1. `POST /_paimon/mount` opens the requested Paimon table and selects the latest or explicit
   snapshot.
2. The planner scans live `es-index` manifest entries, parses each archive's ESLib metadata, and
   validates its fields, file ranges, row count, and non-overlapping global row-id range.
3. Archives are sorted by `rowRangeStart`; archive number `n` becomes ES shard `n`.
4. `index.store.type=paimon` constructs an `ArchiveDirectory` for that shard. Local files use
   positional file reads and `oss://` files use OSS range GETs.
5. Elasticsearch performs normal empty-store recovery once. At engine open, the plugin overlays
   that bootstrap commit metadata on the lake's `SegmentInfos`. The resulting small `segments_N`
   exists only in memory, leaving a self-contained bootstrap commit on disk for restart recovery;
   all referenced segment data remains in the archive.
6. A `ReadOnlyEngine` opens the composed directory. Index writes are blocked at both the cluster
   metadata and directory layers.

The shard descriptor stored in cluster state contains only the archive URI, length, row range,
and Lucene file offsets. It is versioned, checksummed, and never contains an OSS secret.

## Requirements

- Elasticsearch 9.4.0 and JDK 21 on every data node.
- A Paimon 2.0.0 table whose snapshot contains ESLib `es-index` Global Index entries.
- Row tracking and compatible ESLib archive metadata generated while writing the lake table.
- The plugin installed on every node that can allocate the mounted index.
- For OSS, identical `paimon.oss.*` node configuration on all eligible nodes.
- For local/shared-filesystem tables, a `path.repo` root containing the full table and all mounted
  archive files on every eligible node.

Build and install:

```shell
./gradlew :paimon-store:bundlePlugin -Plucene=10
bin/elasticsearch-plugin install file:///absolute/path/paimon-store-1.0.7.zip
```

`paimon-store` is always included in the Gradle project so IDE sync can import the whole
repository. Its compile and bundle tasks enforce `-Plucene=10` when they actually run; project
configuration and IDE model import do not fail when the root build uses the Lucene 9 profile.

The plugin archive deliberately excludes `lz4-java` and `eslib-simdvec`. Elasticsearch 9.4
provides those classes itself; bundling the Paimon or library copies would make installation fail
the Elasticsearch jar-hell check. It also excludes the standalone `paimon-shade-*` JARs because
Paimon 2.0 already embeds all of their classes in `paimon-api`.

Paimon's published `paimon-oss` JAR normally creates a `ComponentClassLoader` and delegates OSS
access to its embedded Hadoop 3.3.4 runtime. Elasticsearch does not grant the corresponding
`create_class_loader` entitlement to external plugins, and Hadoop 3.3.4 calls the obsolete JAAS
`Subject.getSubject` API which is unsupported by the Elasticsearch JDK. The plugin therefore uses
a read-only Paimon `FileIO` backed directly by Aliyun OSS SDK 3.17.4. The archive contains neither
`paimon-oss` nor Hadoop classes; `bundlePlugin` verifies that they remain absent.

Paimon validates the table's declared data format while opening its schema, even though mount
planning reads only metadata and ESLib index manifests. The plugin therefore includes Paimon's
an ES-cleaned `paimon-format-es-2.0.0.jar` so the `parquet` `FileFormatFactory` is discoverable, but
suppresses that artifact's transitive Hadoop graph and duplicate classes rejected by ES Jar Hell.

## Node settings

Local `file:` and native filesystem paths require no credentials, but are accepted only below an
Elasticsearch `path.repo` root. For `oss://bucket/key` paths:

```yaml
# elasticsearch.yml
paimon.oss.endpoint: https://oss-cn-hangzhou.aliyuncs.com
paimon.oss.access_key_id: ${PAIMON_OSS_ACCESS_KEY_ID}
```

```shell
bin/elasticsearch-keystore add paimon.oss.access_key_secret
```

The key requires read access to the Paimon table metadata and selected global-index archives.
During installation Elasticsearch displays the plugin's requested entitlements: read-only access
to `path.repo`, outbound network/HTTPS access, and thread management used by the OSS HTTP client.

## Mount API

```http
POST /_paimon/mount?master_timeout=30s&timeout=30s
Content-Type: application/json

{
  "table_path": "oss://example-bucket/warehouse/catalog.db/items",
  "index_name": "items_search",
  "snapshot_id": 42,
  "vector_field_name": "embedding",
  "storage_mode": "remote",
  "source_enabled": true,
  "return_fields": [],
  "auth_type": "node"
}
```

`table_path` and `index_name` are required. `snapshot_id` defaults to the latest snapshot.
`vector_field_name` selects archives containing that field when a snapshot carries several
ESLib indexes. `storage_mode` accepts `remote` or `mmap` for request compatibility; actual access
is selected from the archive URI. `source_enabled=true` enables Paimon row hydration for search
hits. An omitted or empty `return_fields` array returns every business field in the snapshot
schema; a non-empty array returns only those top-level fields. `return_fields` requires
`source_enabled=true`. `auth_type`, when supplied, must be `node`.

Successful response:

```json
{
  "acknowledged": true,
  "alias": "items_search",
  "index": "items_search_42",
  "snapshot_id": 42,
  "shards": 8,
  "source_enabled": true,
  "return_fields": []
}
```

The normal search API now returns the matching Paimon record in `_source`:

```shell
curl -sS 'http://127.0.0.1:9200/items_search/_search?pretty' \
  -H 'Content-Type: application/json' \
  -d '{"size":10,"query":{"match":{"content":"example"}}}'
```

Use `"_source": ["id", "content"]` in the search body for request-level field filtering.

The physical index is snapshot-specific. After it is created, the stable alias is switched from
older matching physical indexes to the new one. Remounting the same alias and snapshot currently
fails if that physical index already exists; it does not mutate the mounted index.

The Base64/CRC encoded shard descriptors contain no credentials and can be inspected through the
standard index settings API:

```shell
curl 'http://127.0.0.1:9200/items_search/_settings/index.paimon.shards*?flat_settings=true&pretty'
```

The response exposes one `index.paimon.shards.N` value per mounted primary shard. OSS access-key
settings remain filtered and are never included in index settings.

For decoded mount metadata, query the physical index or its alias:

```shell
curl 'http://127.0.0.1:9200/_paimon/mount/items_search?pretty'
```

The response contains the physical index, table path, snapshot ID, archive URI and length, Row-ID
range, and every Lucene file offset. Limit a large response to one shard, or omit file details:

```shell
curl 'http://127.0.0.1:9200/_paimon/mount/items_search?shard=0&include_files=true&pretty'
curl 'http://127.0.0.1:9200/_paimon/mount/items_search?include_files=false&pretty'
```

## Query behavior and limits

- Mounted indexes are immutable and have `index.number_of_replicas=0` by design.
- With `source_enabled=true`, an ordinary `_search` response contains a hydrated `_source` from
  the exact mounted Paimon snapshot. `_source: false` suppresses the lookup, and standard
  `_source.includes` / `_source.excludes` filters are applied after `return_fields`.
- Hydration is part of the search fetch phase: Lucene first finds a shard-local document ID, then
  the plugin computes `rowRangeStart + docId` and uses Paimon's `withRowRanges` random-access read.
  A missing or duplicate Row-ID fails the fetch instead of returning a mismatched record.
- Table handles and FileIO clients are cached per `(table_path, snapshot_id)` until node shutdown;
  each hit still creates a bounded Paimon reader. Keep normal Elasticsearch `size` limits for
  queries returning large vectors or blobs.
- Hydrated source currently applies to `_search`; source-dependent highlighting/script processing
  runs before the plugin fetch sub-phase and should use indexed fields/doc values instead.
- Fields are mapped from the ESLib archive metadata. Vector, text, keyword, numeric, date, and
  geo-point layouts are supported; incompatible layouts across archives fail the mount.
- Native/JNI vector indexes are rejected because Elasticsearch has no compatible `dense_vector`
  mapping. IK text fields require a compatible IK analysis plugin on every Elasticsearch node.
- ES940 DiskBBQ vectors are searchable through the raw flat-vector data stored in the same Lucene
  segment. The current Lucene 10 mount reader performs an exact per-shard scan, preserving result
  correctness and on-disk compatibility but not the optimized IVF query performance yet.
- Lucene doc IDs stay shard-local. The corresponding Paimon row ID is
  `rowRangeStart + luceneDocId`.
- `index.paimon.source_enabled`, `index.paimon.return_fields`, `index.paimon.table_path`,
  `index.paimon.snapshot_id`, and the shard descriptors are persisted in cluster-state settings.
  They are read again when a node restarts, so source hydration does not depend on an in-memory
  mount registration.
- If an archive changes length after planning, shard opening fails instead of reading a different
  object. Paimon snapshots and their global-index files must remain available for the lifetime of
  the mount.
- Direct credentials in the REST request are intentionally unsupported. The plugin copies the
  secure OSS value during node startup because Elasticsearch closes the backing keystore after
  plugin construction. After rotating credentials in the keystore, restart each Elasticsearch
  node so the plugin loads the new value.
