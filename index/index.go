package index

import (
	"bcdb/data"
)

type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool

	Iterator(reverse bool) Interator
	Size() int // 返回索引数量
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

type Interator interface {
	// 回到迭代器的起点
	ReWind()
	// 根据传入的key找到第一个>（或者<=）的目标key，根据这个key进行遍历
	Seek(key []byte)
	// 找到下一个key
	Next()
	// key是否有效，用于检查是否完成遍历
	Valid() bool
	// 获取当前key的值
	Key() []byte
	// 获取当前key对应的value
	Value() *data.LogRecordPos
	// 关闭迭代器，释放资源
	Close()
}
