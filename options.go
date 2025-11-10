package bcdb

import (
	"bcdb/index"
)

type Options struct {
	DirPath     string
	MaxFileSize int64
	SyncWrite   bool
	IndexType   index.IndexType
}
