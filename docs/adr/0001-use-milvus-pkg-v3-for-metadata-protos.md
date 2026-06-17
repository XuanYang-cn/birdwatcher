# Use Milvus pkg/v3 for metadata protobufs

Birdwatcher will use `github.com/milvus-io/milvus/pkg/v3` for Milvus package protobufs when the process can safely load Milvus 3 generated packages, because Milvus 3 metadata types such as Compaction Target are not available through the current `pkg/v2` dependency. A local development checkout may use a `replace` to point `pkg/v3` at `~/Github/milvus/pkg`, but upstream birdwatcher changes should depend on a published `pkg/v3` version rather than a local path.

Implementation note: birdwatcher currently links Milvus v2 generated protos in the same binary. Directly importing Milvus v3 generated protos also registers duplicate descriptor names such as `common.proto`, so `show compaction-targets` uses a local wire-level model that matches the Milvus 3 `CompactionTarget` schema and keeps the dependency graph unchanged. Replace this shim with `pkg/v3` generated types when the broader Milvus dependency migration is safe for the whole binary.
