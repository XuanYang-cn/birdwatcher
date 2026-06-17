package models

import (
	"fmt"
	"strconv"

	"google.golang.org/protobuf/encoding/protowire"
)

type TargetState int32

const (
	TargetStateActive   TargetState = 0
	TargetStateInactive TargetState = 1
)

func (s TargetState) String() string {
	switch s {
	case TargetStateActive:
		return "TARGET_STATE_ACTIVE"
	case TargetStateInactive:
		return "TARGET_STATE_INACTIVE"
	default:
		return strconv.Itoa(int(s))
	}
}

type TargetIntent int32

const (
	TargetIntentUnknown  TargetIntent = 0
	TargetIntentRewrite  TargetIntent = 1
	TargetIntentSize     TargetIntent = 2
	TargetIntentSort     TargetIntent = 3
	TargetIntentOptimize TargetIntent = 4
	TargetIntentBackfill TargetIntent = 5
)

func (i TargetIntent) String() string {
	switch i {
	case TargetIntentUnknown:
		return "INTENT_UNKNOWN"
	case TargetIntentRewrite:
		return "INTENT_REWRITE"
	case TargetIntentSize:
		return "INTENT_SIZE"
	case TargetIntentSort:
		return "INTENT_SORT"
	case TargetIntentOptimize:
		return "INTENT_OPTIMIZE"
	case TargetIntentBackfill:
		return "INTENT_BACKFILL"
	default:
		return strconv.Itoa(int(i))
	}
}

// CompactionTarget wraps Milvus 3 DataCoord Compaction Target metadata.
// Keep this as a wire-level model while birdwatcher still links Milvus v2
// protos; generated v2 and v3 packages register conflicting descriptor names.
type CompactionTarget struct {
	TargetID        int64
	CollectionID    int64
	Intent          TargetIntent
	Properties      map[string]string
	ExpectedTS      uint64
	TailLimit       int32
	State           TargetState
	ActivatedAtTS   uint64
	InactivatedAtTS uint64
	key             string
}

func (ct *CompactionTarget) Key() string {
	return ct.key
}

func (ct *CompactionTarget) GetTargetID() int64 {
	if ct == nil {
		return 0
	}
	return ct.TargetID
}

func (ct *CompactionTarget) GetCollectionID() int64 {
	if ct == nil {
		return 0
	}
	return ct.CollectionID
}

func (ct *CompactionTarget) GetIntent() TargetIntent {
	if ct == nil {
		return TargetIntentUnknown
	}
	return ct.Intent
}

func (ct *CompactionTarget) GetProperties() map[string]string {
	if ct == nil {
		return nil
	}
	return ct.Properties
}

func (ct *CompactionTarget) GetExpectedTS() uint64 {
	if ct == nil {
		return 0
	}
	return ct.ExpectedTS
}

func (ct *CompactionTarget) GetTailLimit() int32 {
	if ct == nil {
		return 0
	}
	return ct.TailLimit
}

func (ct *CompactionTarget) GetState() TargetState {
	if ct == nil {
		return TargetStateActive
	}
	return ct.State
}

func (ct *CompactionTarget) GetActivatedAtTS() uint64 {
	if ct == nil {
		return 0
	}
	return ct.ActivatedAtTS
}

func (ct *CompactionTarget) GetInactivatedAtTS() uint64 {
	if ct == nil {
		return 0
	}
	return ct.InactivatedAtTS
}

func NewCompactionTarget(record CompactionTarget, key string) *CompactionTarget {
	record.key = key
	return &record
}

func UnmarshalCompactionTarget(data []byte, key string) (*CompactionTarget, error) {
	target := &CompactionTarget{
		key:        key,
		Properties: make(map[string]string),
	}

	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]

		switch num {
		case 1:
			value, n := consumeVarint(typ, data)
			if n < 0 {
				return nil, fieldParseError(num, n)
			}
			target.TargetID = int64(value)
			data = data[n:]
		case 2:
			value, n := consumeVarint(typ, data)
			if n < 0 {
				return nil, fieldParseError(num, n)
			}
			target.CollectionID = int64(value)
			data = data[n:]
		case 3:
			value, n := consumeVarint(typ, data)
			if n < 0 {
				return nil, fieldParseError(num, n)
			}
			target.Intent = TargetIntent(int32(value))
			data = data[n:]
		case 4:
			value, n := consumeBytes(typ, data)
			if n < 0 {
				return nil, fieldParseError(num, n)
			}
			propertyKey, propertyValue, err := parseCompactionTargetProperty(value)
			if err != nil {
				return nil, err
			}
			target.Properties[propertyKey] = propertyValue
			data = data[n:]
		case 5:
			value, n := consumeVarint(typ, data)
			if n < 0 {
				return nil, fieldParseError(num, n)
			}
			target.ExpectedTS = value
			data = data[n:]
		case 6:
			value, n := consumeVarint(typ, data)
			if n < 0 {
				return nil, fieldParseError(num, n)
			}
			target.TailLimit = int32(value)
			data = data[n:]
		case 7:
			value, n := consumeVarint(typ, data)
			if n < 0 {
				return nil, fieldParseError(num, n)
			}
			target.State = TargetState(int32(value))
			data = data[n:]
		case 8:
			value, n := consumeVarint(typ, data)
			if n < 0 {
				return nil, fieldParseError(num, n)
			}
			target.ActivatedAtTS = value
			data = data[n:]
		case 9:
			value, n := consumeVarint(typ, data)
			if n < 0 {
				return nil, fieldParseError(num, n)
			}
			target.InactivatedAtTS = value
			data = data[n:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return nil, fieldParseError(num, n)
			}
			data = data[n:]
		}
	}

	if len(target.Properties) == 0 {
		target.Properties = nil
	}
	return target, nil
}

func consumeVarint(typ protowire.Type, data []byte) (uint64, int) {
	if typ != protowire.VarintType {
		return 0, -1
	}
	return protowire.ConsumeVarint(data)
}

func consumeBytes(typ protowire.Type, data []byte) ([]byte, int) {
	if typ != protowire.BytesType {
		return nil, -1
	}
	return protowire.ConsumeBytes(data)
}

func parseCompactionTargetProperty(data []byte) (string, string, error) {
	var propertyKey string
	var propertyValue string

	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return "", "", protowire.ParseError(n)
		}
		data = data[n:]

		switch num {
		case 1:
			value, n := consumeBytes(typ, data)
			if n < 0 {
				return "", "", fieldParseError(num, n)
			}
			propertyKey = string(value)
			data = data[n:]
		case 2:
			value, n := consumeBytes(typ, data)
			if n < 0 {
				return "", "", fieldParseError(num, n)
			}
			propertyValue = string(value)
			data = data[n:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return "", "", fieldParseError(num, n)
			}
			data = data[n:]
		}
	}

	return propertyKey, propertyValue, nil
}

func fieldParseError(num protowire.Number, n int) error {
	if n == -1 {
		return fmt.Errorf("invalid wire type for compaction target field %d", num)
	}
	return protowire.ParseError(n)
}
