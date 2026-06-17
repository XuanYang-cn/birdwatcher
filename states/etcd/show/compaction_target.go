package show

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/utils"
)

type CompactionTargetParam struct {
	framework.DataSetParam `use:"show compaction-targets" desc:"list persisted DataCoord Compaction Targets"`
	TargetID               int64  `name:"targetID" default:"0" desc:"target id to filter"`
	CollectionID           int64  `name:"collectionID" default:"0" desc:"collection id to filter"`
	Intent                 string `name:"intent" default:"" desc:"target intent to filter"`
	State                  string `name:"state" default:"" desc:"target state to filter"`
	Detail                 bool   `name:"detail" default:"false" desc:"parse supported target properties in addition to raw values"`
}

func (c *ComponentShow) CompactionTargetCommand(ctx context.Context, p *CompactionTargetParam) (*framework.PresetResultSet, error) {
	var total int64
	targets, err := common.ListCompactionTargets(ctx, c.client, c.metaPath, func(target *models.CompactionTarget) bool {
		total++
		if p.TargetID > 0 && target.GetTargetID() != p.TargetID {
			return false
		}
		if p.CollectionID > 0 && target.GetCollectionID() != p.CollectionID {
			return false
		}
		if p.Intent != "" && !strings.EqualFold(p.Intent, target.GetIntent().String()) {
			return false
		}
		if p.State != "" && !strings.EqualFold(p.State, target.GetState().String()) {
			return false
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(targets, func(i, j int) bool {
		return targets[i].GetTargetID() < targets[j].GetTargetID()
	})

	return framework.NewPresetResultSet(&CompactionTargets{
		targets: targets,
		total:   total,
		param:   p,
	}, framework.NameFormat(p.Format)), nil
}

type CompactionTargets struct {
	targets []*models.CompactionTarget
	total   int64
	param   *CompactionTargetParam
}

func (rs *CompactionTargets) TableHeaders() table.Row {
	return table.Row{"TargetID", "CollectionID", "Intent", "State"}
}

func (rs *CompactionTargets) TableRows() []table.Row {
	rows := make([]table.Row, 0, len(rs.targets))
	for _, target := range rs.targets {
		rows = append(rows, table.Row{
			target.GetTargetID(),
			target.GetCollectionID(),
			target.GetIntent().String(),
			target.GetState().String(),
		})
	}
	return rows
}

func (rs *CompactionTargets) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		for _, target := range rs.targets {
			printCompactionTarget(sb, target, rs.param != nil && rs.param.Detail)
		}
		fmt.Fprintln(sb, "================================================================================")
		fmt.Fprintf(sb, "--- Total compaction targets:  %d\t Matched compaction targets:  %d\n", rs.total, len(rs.targets))
		return sb.String()
	case framework.FormatJSON:
		return rs.printAsJSON()
	}
	return ""
}

func (rs *CompactionTargets) printAsJSON() string {
	type CompactionTargetJSON struct {
		TargetID        int64             `json:"targetID"`
		CollectionID    int64             `json:"collectionID"`
		Intent          int32             `json:"intent"`
		Properties      map[string]string `json:"properties"`
		ExpectedTS      uint64            `json:"expectedTS"`
		TailLimit       int32             `json:"tailLimit"`
		State           int32             `json:"state"`
		ActivatedAtTS   uint64            `json:"activatedAtTS"`
		InactivatedAtTS uint64            `json:"inactivatedAtTS"`
	}

	type OutputJSON struct {
		CompactionTargets []CompactionTargetJSON `json:"compaction_targets"`
		TotalCount        int64                  `json:"total_count"`
		MatchedCount      int                    `json:"matched_count"`
	}

	output := OutputJSON{
		CompactionTargets: make([]CompactionTargetJSON, 0, len(rs.targets)),
		TotalCount:        rs.total,
		MatchedCount:      len(rs.targets),
	}

	for _, target := range rs.targets {
		output.CompactionTargets = append(output.CompactionTargets, CompactionTargetJSON{
			TargetID:        target.GetTargetID(),
			CollectionID:    target.GetCollectionID(),
			Intent:          int32(target.GetIntent()),
			Properties:      cloneStringMap(target.GetProperties()),
			ExpectedTS:      target.GetExpectedTS(),
			TailLimit:       target.GetTailLimit(),
			State:           int32(target.GetState()),
			ActivatedAtTS:   target.GetActivatedAtTS(),
			InactivatedAtTS: target.GetInactivatedAtTS(),
		})
	}

	return framework.MarshalJSON(output)
}

func (rs *CompactionTargets) Entities() any {
	return rs.targets
}

func printCompactionTarget(sb *strings.Builder, target *models.CompactionTarget, detail bool) {
	fmt.Fprintln(sb, "================================================================================")
	fmt.Fprintf(sb, "Target ID: %d\n", target.GetTargetID())
	fmt.Fprintf(sb, "Collection ID: %d\n", target.GetCollectionID())
	fmt.Fprintf(sb, "Intent: %s\n", target.GetIntent().String())
	fmt.Fprintf(sb, "State: %s\n", target.GetState().String())
	fmt.Fprintf(sb, "Tail Limit: %d\n", target.GetTailLimit())
	printCompactionTargetTS(sb, "Expected TS", target.GetExpectedTS())
	printCompactionTargetTS(sb, "Activated At TS", target.GetActivatedAtTS())
	printCompactionTargetTS(sb, "Inactivated At TS", target.GetInactivatedAtTS())
	printCompactionTargetProperties(sb, target.GetProperties())
	if detail {
		printCompactionTargetDetail(sb, target)
	}
}

func printCompactionTargetTS(sb *strings.Builder, label string, ts uint64) {
	physical, logical := utils.ParseTS(ts)
	fmt.Fprintf(sb, "%s: %d\tPhysical Time: %s\tLogical: %d\n", label, ts, physical.Format(tsPrintFormat), logical)
}

func printCompactionTargetProperties(sb *strings.Builder, properties map[string]string) {
	fmt.Fprintln(sb, "Properties:")
	keys := make([]string, 0, len(properties))
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Fprintf(sb, "  %s: %s\n", key, properties[key])
	}
}

func printCompactionTargetDetail(sb *strings.Builder, target *models.CompactionTarget) {
	if target.GetIntent() != models.TargetIntentRewrite {
		return
	}
	raw := target.GetProperties()["segment_ids"]
	if raw == "" {
		return
	}
	var segmentIDs []int64
	if err := json.Unmarshal([]byte(raw), &segmentIDs); err != nil {
		return
	}
	fmt.Fprintf(sb, "Parsed Segment IDs: %v\n", segmentIDs)
}

func cloneStringMap(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}
