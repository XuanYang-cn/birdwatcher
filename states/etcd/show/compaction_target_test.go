package show

import (
	"context"
	"encoding/json"
	"path"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
)

type compactionTargetShowKV struct {
	kv.MetaKV
	data map[string]string
}

func (c *compactionTargetShowKV) LoadWithPrefix(ctx context.Context, prefix string, opts ...kv.LoadOption) ([]string, []string, error) {
	keys := make([]string, 0)
	for key := range c.data {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	values := make([]string, 0, len(keys))
	for _, key := range keys {
		values = append(values, c.data[key])
	}
	return keys, values, nil
}

func TestCompactionTargetCommandFiltersTargets(t *testing.T) {
	basePath := "root"
	targets := map[string]string{
		path.Join(basePath, common.DCPrefix, common.CompactionTargetPrefix, "100"): mustShowCompactionTargetValue(t, models.CompactionTarget{
			TargetID:     100,
			CollectionID: 10,
			Intent:       models.TargetIntentRewrite,
			State:        models.TargetStateActive,
		}),
		path.Join(basePath, common.DCPrefix, common.CompactionTargetPrefix, "200"): mustShowCompactionTargetValue(t, models.CompactionTarget{
			TargetID:     200,
			CollectionID: 20,
			Intent:       models.TargetIntentSize,
			State:        models.TargetStateInactive,
		}),
	}
	component := NewComponent(&compactionTargetShowKV{data: targets}, &configs.Config{}, "root", "")

	result, err := component.CompactionTargetCommand(context.Background(), &CompactionTargetParam{
		CollectionID: 10,
		TargetID:     100,
		Intent:       "intent_rewrite",
		State:        "target_state_active",
	})
	require.NoError(t, err)

	entities := result.Entities().([]*models.CompactionTarget)
	require.Len(t, entities, 1)
	require.EqualValues(t, 100, entities[0].GetTargetID())
}

func TestCompactionTargetsPlainOutputIncludesFullRawRecord(t *testing.T) {
	activatedAtTime := time.Date(2026, time.June, 17, 8, 30, 0, 123000000, time.UTC)
	activatedAt := composeHybridTS(activatedAtTime, 7)
	target := models.CompactionTarget{
		TargetID:      100,
		CollectionID:  10,
		Intent:        models.TargetIntentRewrite,
		Properties:    map[string]string{"segment_ids": "[30,10]", "reason": "manual"},
		ExpectedTS:    activatedAt + 1,
		TailLimit:     3,
		State:         models.TargetStateActive,
		ActivatedAtTS: activatedAt,
	}
	result := &CompactionTargets{
		targets: []*models.CompactionTarget{models.NewCompactionTarget(target, "root/datacoord-meta/compaction-target/100")},
		total:   1,
		param:   &CompactionTargetParam{Detail: true},
	}

	output := result.PrintAs(framework.FormatDefault)
	require.Contains(t, output, "Target ID: 100")
	require.Contains(t, output, "Collection ID: 10")
	require.Contains(t, output, "Intent: INTENT_REWRITE")
	require.Contains(t, output, "State: TARGET_STATE_ACTIVE")
	require.Contains(t, output, "Expected TS: ")
	require.Contains(t, output, "Physical Time: "+time.UnixMilli(activatedAtTime.UnixMilli()).Format(tsPrintFormat))
	require.Contains(t, output, "Logical: 7")
	require.Contains(t, output, "Properties:")
	require.Contains(t, output, "reason: manual")
	require.Contains(t, output, "segment_ids: [30,10]")
	require.Contains(t, output, "Parsed Segment IDs: [30 10]")
	require.Contains(t, output, "--- Total compaction targets:  1\t Matched compaction targets:  1")
}

func TestCompactionTargetsJSONOutputPreservesRawFields(t *testing.T) {
	target := models.CompactionTarget{
		TargetID:        100,
		CollectionID:    10,
		Intent:          models.TargetIntentRewrite,
		Properties:      map[string]string{"segment_ids": "[30,10]"},
		ExpectedTS:      123,
		TailLimit:       3,
		State:           models.TargetStateInactive,
		ActivatedAtTS:   456,
		InactivatedAtTS: 789,
	}
	result := &CompactionTargets{
		targets: []*models.CompactionTarget{models.NewCompactionTarget(target, "root/datacoord-meta/compaction-target/100")},
		total:   2,
		param:   &CompactionTargetParam{},
	}

	var output struct {
		CompactionTargets []struct {
			TargetID        int64             `json:"targetID"`
			CollectionID    int64             `json:"collectionID"`
			Intent          int32             `json:"intent"`
			Properties      map[string]string `json:"properties"`
			ExpectedTS      uint64            `json:"expectedTS"`
			TailLimit       int32             `json:"tailLimit"`
			State           int32             `json:"state"`
			ActivatedAtTS   uint64            `json:"activatedAtTS"`
			InactivatedAtTS uint64            `json:"inactivatedAtTS"`
		} `json:"compaction_targets"`
		TotalCount   int64 `json:"total_count"`
		MatchedCount int   `json:"matched_count"`
	}
	require.NoError(t, json.Unmarshal([]byte(result.PrintAs(framework.FormatJSON)), &output))
	require.EqualValues(t, 2, output.TotalCount)
	require.EqualValues(t, 1, output.MatchedCount)
	require.Len(t, output.CompactionTargets, 1)
	require.EqualValues(t, 100, output.CompactionTargets[0].TargetID)
	require.EqualValues(t, 10, output.CompactionTargets[0].CollectionID)
	require.EqualValues(t, models.TargetIntentRewrite, output.CompactionTargets[0].Intent)
	require.Equal(t, map[string]string{"segment_ids": "[30,10]"}, output.CompactionTargets[0].Properties)
	require.EqualValues(t, 123, output.CompactionTargets[0].ExpectedTS)
	require.EqualValues(t, 3, output.CompactionTargets[0].TailLimit)
	require.EqualValues(t, models.TargetStateInactive, output.CompactionTargets[0].State)
	require.EqualValues(t, 456, output.CompactionTargets[0].ActivatedAtTS)
	require.EqualValues(t, 789, output.CompactionTargets[0].InactivatedAtTS)
}

func mustShowCompactionTargetValue(t *testing.T, target models.CompactionTarget) string {
	t.Helper()

	bs := marshalShowCompactionTargetValue(target)
	return string(bs)
}

func marshalShowCompactionTargetValue(target models.CompactionTarget) []byte {
	var data []byte
	data = protowire.AppendTag(data, 1, protowire.VarintType)
	data = protowire.AppendVarint(data, uint64(target.TargetID))
	data = protowire.AppendTag(data, 2, protowire.VarintType)
	data = protowire.AppendVarint(data, uint64(target.CollectionID))
	data = protowire.AppendTag(data, 3, protowire.VarintType)
	data = protowire.AppendVarint(data, uint64(target.Intent))
	for key, value := range target.Properties {
		var entry []byte
		entry = protowire.AppendTag(entry, 1, protowire.BytesType)
		entry = protowire.AppendString(entry, key)
		entry = protowire.AppendTag(entry, 2, protowire.BytesType)
		entry = protowire.AppendString(entry, value)
		data = protowire.AppendTag(data, 4, protowire.BytesType)
		data = protowire.AppendBytes(data, entry)
	}
	data = protowire.AppendTag(data, 5, protowire.VarintType)
	data = protowire.AppendVarint(data, target.ExpectedTS)
	data = protowire.AppendTag(data, 6, protowire.VarintType)
	data = protowire.AppendVarint(data, uint64(target.TailLimit))
	data = protowire.AppendTag(data, 7, protowire.VarintType)
	data = protowire.AppendVarint(data, uint64(target.State))
	data = protowire.AppendTag(data, 8, protowire.VarintType)
	data = protowire.AppendVarint(data, target.ActivatedAtTS)
	data = protowire.AppendTag(data, 9, protowire.VarintType)
	data = protowire.AppendVarint(data, target.InactivatedAtTS)
	return data
}

func composeHybridTS(physical time.Time, logical uint64) uint64 {
	return uint64(physical.UnixMilli()<<18) + logical
}
