package index

import (
	"bcdb/data"
	"bytes"

	"github.com/google/btree"
)

type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
}
type IndexType = int8

const (
	BTREE IndexType = iota + 1
	ART
)

func NewIndexer(indexType IndexType) Indexer {
	switch indexType {
	case BTREE:
		return NewBTree()
	case ART:
		// TODO
		return nil
	default:
		panic("unknown index type")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (item *Item) Less(b btree.Item) bool {
	return bytes.Compare(item.key, b.(*Item).key) == -1
}
