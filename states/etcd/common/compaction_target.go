package common

import (
	"bytes"
	"context"
	"fmt"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
)

// ListCompactionTargets returns DataCoord Compaction Target metadata.
func ListCompactionTargets(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(target *models.CompactionTarget) bool) ([]*models.CompactionTarget, error) {
	prefix := path.Join(basePath, DCPrefix, CompactionTargetPrefix) + "/"
	keys, vals, err := cli.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	if len(keys) != len(vals) {
		return nil, fmt.Errorf("error: keys and vals of different size in ListCompactionTargets:%d vs %d", len(keys), len(vals))
	}

	result := make([]*models.CompactionTarget, 0, len(vals))
LOOP:
	for idx, val := range vals {
		if bytes.Equal([]byte(val), []byte{0xE2, 0x9B, 0xBC}) {
			fmt.Printf("Tombstone found, key: %s\n", keys[idx])
			continue
		}
		target, err := models.UnmarshalCompactionTarget([]byte(val), keys[idx])
		if err != nil {
			fmt.Printf("failed to unmarshal key=%s, err: %s\n", keys[idx], err.Error())
			continue
		}

		for _, filter := range filters {
			if !filter(target) {
				continue LOOP
			}
		}
		result = append(result, target)
	}
	return result, nil
}
