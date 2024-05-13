package compact

import (
	"bytes"

	"github.com/docker/go-units"
	"github.com/google/btree"
	backup "github.com/pingcap/kvproto/pkg/brpb"
)

type CollectUnit struct {
	StartKey []byte
	EndKey   []byte

	Size  uint64
	Metas []*backup.DataFileInfo
}

func min(a, b []byte) []byte {
	if bytes.Compare(a, b) < 0 {
		return a
	}
	return b
}

func max(a, b []byte) []byte {
	if bytes.Compare(a, b) > 0 {
		return a
	}
	return b
}

func (c CollectUnit) lessThan(other CollectUnit) bool {
	return bytes.Compare(c.StartKey, other.StartKey) < 0
}

func (c CollectUnit) overlaps(other CollectUnit) bool {
	return bytes.Compare(c.EndKey, other.StartKey) > 0 && bytes.Compare(c.StartKey, other.EndKey) < 0
}

func (c CollectUnit) extend(other CollectUnit) CollectUnit {
	newStart := min(c.StartKey, other.StartKey)
	newEnd := max(c.EndKey, other.EndKey)
	newSize := c.Size + other.Size
	return CollectUnit{
		StartKey: newStart,
		EndKey:   newEnd,
		Size:     newSize,
		Metas:    append(c.Metas, other.Metas...),
	}
}

func lameUnitForCompare(key []byte) CollectUnit {
	return CollectUnit{StartKey: key}
}

type CommitReason int

const (
	CommitNone       CommitReason = 0
	CommitDueToSize  CommitReason = 1
	CommitDueToMerge CommitReason = 2
)

type Commit struct {
	Reason CommitReason
	Metas  []*backup.DataFileInfo
}

type RangeManager struct {
	tree *btree.BTreeG[CollectUnit]
}

func NewRangeManager() RangeManager {
	return RangeManager{
		tree: btree.NewG(16, CollectUnit.lessThan),
	}
}

func (c RangeManager) Add(u CollectUnit) (commit Commit) {
	replaced := c.findExtendTargets(u)
	switch len(replaced) {
	case 0:
		c.tree.ReplaceOrInsert(u)
		commit.Reason = CommitNone
	case 1:
		new := u.extend(replaced[0])
		c.tree.Delete(replaced[0])
		if new.Size > 128*units.MiB {
			commit.Reason = CommitDueToSize
			commit.Metas = new.Metas
		} else {
			commit.Reason = CommitNone
			c.tree.ReplaceOrInsert(new)
		}
	default:
		for _, unit := range replaced {
			commit.Metas = append(commit.Metas, unit.Metas...)
			c.tree.Delete(unit)
		}
		commit.Reason = CommitDueToMerge
		c.tree.ReplaceOrInsert(u)
	}
	return
}

func (c RangeManager) findExtendTargets(u CollectUnit) []CollectUnit {
	nexts := make([]CollectUnit, 0)
	c.tree.AscendLessThan(u, func(item CollectUnit) bool {
		if item.overlaps(u) {
			nexts = append(nexts, item)
		}
		return false
	})

	c.tree.AscendGreaterOrEqual(lameUnitForCompare(u.EndKey), func(item CollectUnit) bool {
		if item.overlaps(u) {
			nexts = append(nexts, item)
			return true
		}
		return false
	})

	return nexts
}
