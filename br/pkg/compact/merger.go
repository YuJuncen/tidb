package compact

import "github.com/pingcap/tidb/br/pkg/storage"

type Merger struct {
	source *storage.ExternalStorage
	output *storage.ExternalStorage

	MaxTs uint64
}

func (m *Merger) Commit(c Commit) {

}
