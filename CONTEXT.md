# Birdwatcher

Birdwatcher inspects and repairs Milvus operational metadata. This glossary names the Milvus metadata concepts the tool exposes to operators.

## Language

**Compaction Target**:
A desired compaction intent recorded for a collection. It describes what DataCoord should continue trying to compact, not a single execution attempt.
_Avoid_: Compaction job, compaction plan, compaction task

**Compaction Task**:
A concrete compaction execution record. It tracks an individual compaction attempt and its execution state.
_Avoid_: Compaction target
