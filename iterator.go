package bcdb

import (
	"bcdb/index"
	"bytes"
)

type Iterator struct {
	indexIter index.Interator
	db        *DB
	Options   IteratorOptions
}

func (db *DB) NewIterator(options IteratorOptions) *Iterator {
	iterator := db.index.Iterator(options.Reverse)
	it := &Iterator{
		indexIter: iterator,
		db:        db,
		Options:   options,
	}
	it.filterPrefix()
	return it
}

func (it *Iterator) ReWind() {
	it.indexIter.ReWind()
	it.filterPrefix()
}

func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.filterPrefix()
}

func (it *Iterator) Next() {
	it.indexIter.Next()
	it.filterPrefix()
}

func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

func (it *Iterator) Value() ([]byte, error) {
	pos := it.indexIter.Value()
	return it.db.getValueByPos(pos)
}

func (it *Iterator) Close() {
	it.indexIter.Close()
}

func (it *Iterator) filterPrefix() {
	prefixLen := len(it.Options.Prefix)
	if prefixLen == 0 {
		return
	}
	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if prefixLen <= len(key) && bytes.HasPrefix(key, it.Options.Prefix) {
			break
		}
	}
}
