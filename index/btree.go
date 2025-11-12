package index

import (
	"bcdb/data"
	"bytes"
	"sort"
	"sync"

	"github.com/google/btree"
)

type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (item *Item) Less(b btree.Item) bool {
	return bytes.Compare(item.key, b.(*Item).key) == -1
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	item := &Item{key: key, pos: pos}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	bt.tree.ReplaceOrInsert(item)
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	item := &Item{key: key}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	bTreeItem := bt.tree.Get(item)
	if bTreeItem == nil {
		return nil
	}
	return bTreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	item := &Item{key: key}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	oldItem := bt.tree.Delete(item)
	if oldItem == nil {
		return false
	}
	return true
}

func (bt *BTree) Iterator(reverse bool) Interator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return NewBTreeIterator(bt.tree, reverse)
}

// 迭代器
type bTreeInterator struct {
	currIndex int     // 当前位置
	reverse   bool    // 是否反向迭代
	values    []*Item // 存储迭代的值->key + pos
}

func NewBTreeIterator(tree *btree.BTree, reverse bool) *bTreeInterator {
	var idx int
	values := make([]*Item, tree.Len())

	//定义遍历规则函数，返回true继续遍历，返回false停止遍历
	visit := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}

	if reverse {
		tree.Descend(visit)
	} else {
		tree.Ascend(visit)
	}

	return &bTreeInterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (it *bTreeInterator) ReWind() {
	it.currIndex = 0
}

func (it *bTreeInterator) Seek(key []byte) {
	if it.reverse {
		it.currIndex = sort.Search(len(it.values), func(i int) bool { // 二分查找
			return bytes.Compare(it.values[i].key, key) <= 0
		})
	} else {
		it.currIndex = sort.Search(len(it.values), func(i int) bool {
			return bytes.Compare(it.values[i].key, key) >= 0
		})
	}
}

func (it *bTreeInterator) Next() {
	if it.currIndex < len(it.values) {
		it.currIndex++
	}
}

func (it *bTreeInterator) Valid() bool {
	return it.currIndex < len(it.values)
}

func (it *bTreeInterator) Key() []byte {
	return it.values[it.currIndex].key
}

func (it *bTreeInterator) Value() *data.LogRecordPos {
	return it.values[it.currIndex].pos
}

func (it *bTreeInterator) Close() {
	it.values = nil
}

func (bt *BTree) Size() int {
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return bt.tree.Len()
}
