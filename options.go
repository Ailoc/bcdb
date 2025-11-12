package bcdb

import (
	"bcdb/index"
	"os"
)

type Options struct {
	DirPath     string
	MaxFileSize int64
	SyncWrite   bool
	IndexType   index.IndexType
}

var DefaultOptions = Options{
	DirPath:     os.TempDir(),
	MaxFileSize: 256 * 1024 * 1024, //256MB
	SyncWrite:   false,
	IndexType:   index.BTREE,
}
