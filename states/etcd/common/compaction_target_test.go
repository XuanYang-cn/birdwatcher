package common

import (
	"context"
	"path"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
)

type compactionTargetListKV struct {
	kv.MetaKV
	data           map[string]string
	loadedPrefixes []string
}

func (c *compactionTargetListKV) LoadWithPrefix(ctx context.Context, prefix string, opts ...kv.LoadOption) ([]string, []string, error) {
	c.loadedPrefixes = append(c.loadedPrefixes, prefix)

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

func TestListCompactionTargetsUsesDataCoordTargetPrefix(t *testing.T) {
	ctx := context.Background()
	basePath := "root"
	cli := &compactionTargetListKV{data: map[string]string{
		path.Join(basePath, DCPrefix, CompactionTargetPrefix, "100"): mustCompactionTargetValue(t, models.CompactionTarget{
			TargetID:     100,
			CollectionID: 10,
			Intent:       models.TargetIntentRewrite,
			State:        models.TargetStateActive,
		}),
		path.Join(basePath, DCPrefix, CompactionTargetPrefix, "200"): mustCompactionTargetValue(t, models.CompactionTarget{
			TargetID:     200,
			CollectionID: 20,
			Intent:       models.TargetIntentSize,
			State:        models.TargetStateInactive,
		}),
		path.Join(basePath, DCPrefix, CompactionTaskPrefix, "1", "2", "3"): "not-a-target",
	}}

	targets, err := ListCompactionTargets(ctx, cli, basePath)
	require.NoError(t, err)
	require.Equal(t, []string{path.Join(basePath, DCPrefix, CompactionTargetPrefix) + "/"}, cli.loadedPrefixes)
	require.Len(t, targets, 2)
	require.EqualValues(t, 100, targets[0].GetTargetID())
	require.Equal(t, path.Join(basePath, DCPrefix, CompactionTargetPrefix, "100"), targets[0].Key())
	require.EqualValues(t, 200, targets[1].GetTargetID())
}

func TestListCompactionTargetsKeepsPostFilters(t *testing.T) {
	ctx := context.Background()
	basePath := "root"
	cli := &compactionTargetListKV{data: map[string]string{
		path.Join(basePath, DCPrefix, CompactionTargetPrefix, "100"): mustCompactionTargetValue(t, models.CompactionTarget{
			TargetID:     100,
			CollectionID: 10,
			Intent:       models.TargetIntentRewrite,
			State:        models.TargetStateActive,
		}),
		path.Join(basePath, DCPrefix, CompactionTargetPrefix, "200"): mustCompactionTargetValue(t, models.CompactionTarget{
			TargetID:     200,
			CollectionID: 20,
			Intent:       models.TargetIntentSize,
			State:        models.TargetStateInactive,
		}),
	}}

	targets, err := ListCompactionTargets(ctx, cli, basePath, func(target *models.CompactionTarget) bool {
		return target.GetTargetID() == 100 &&
			target.GetCollectionID() == 10 &&
			target.GetIntent() == models.TargetIntentRewrite &&
			target.GetState() == models.TargetStateActive
	})
	require.NoError(t, err)
	require.Len(t, targets, 1)
	require.EqualValues(t, 100, targets[0].GetTargetID())
}

func mustCompactionTargetValue(t *testing.T, target models.CompactionTarget) string {
	t.Helper()

	bs := marshalCompactionTargetValue(target)
	return string(bs)
}

func marshalCompactionTargetValue(target models.CompactionTarget) []byte {
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
