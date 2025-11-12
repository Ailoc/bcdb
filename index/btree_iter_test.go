package index

import (
	"bcdb/data"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ==================== 迭代器测试 ====================

// 测试空树的迭代器
func TestBTree_Iterator_Empty(t *testing.T) {
	bt := NewBTree()

	// 正向迭代
	iter := bt.Iterator(false)
	assert.NotNil(t, iter)
	assert.False(t, iter.Valid())

	// 反向迭代
	iterReverse := bt.Iterator(true)
	assert.NotNil(t, iterReverse)
	assert.False(t, iterReverse.Valid())
}

// 测试单个元素的迭代器
func TestBTree_Iterator_Single(t *testing.T) {
	bt := NewBTree()
	bt.Put([]byte("key1"), &data.LogRecordPos{Fid: 1, Offset: 10})

	iter := bt.Iterator(false)
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("key1"), iter.Key())
	assert.Equal(t, uint32(1), iter.Value().Fid)
	assert.Equal(t, int64(10), iter.Value().Offset)

	iter.Next()
	assert.False(t, iter.Valid())
}

// 测试正向迭代（Ascend）
func TestBTree_Iterator_Ascend(t *testing.T) {
	bt := NewBTree()

	// 插入数据
	bt.Put([]byte("ccc"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 1, Offset: 20})
	bt.Put([]byte("eee"), &data.LogRecordPos{Fid: 1, Offset: 30})
	bt.Put([]byte("bbb"), &data.LogRecordPos{Fid: 1, Offset: 40})
	bt.Put([]byte("ddd"), &data.LogRecordPos{Fid: 1, Offset: 50})

	// 正向迭代
	iter := bt.Iterator(false)

	expected := []string{"aaa", "bbb", "ccc", "ddd", "eee"}
	idx := 0

	for iter.Valid() {
		assert.Equal(t, []byte(expected[idx]), iter.Key())
		iter.Next()
		idx++
	}

	assert.Equal(t, 5, idx)
}

// 测试反向迭代（Descend）
func TestBTree_Iterator_Descend(t *testing.T) {
	bt := NewBTree()

	// 插入数据
	bt.Put([]byte("ccc"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 1, Offset: 20})
	bt.Put([]byte("eee"), &data.LogRecordPos{Fid: 1, Offset: 30})
	bt.Put([]byte("bbb"), &data.LogRecordPos{Fid: 1, Offset: 40})
	bt.Put([]byte("ddd"), &data.LogRecordPos{Fid: 1, Offset: 50})

	// 反向迭代
	iter := bt.Iterator(true)

	expected := []string{"eee", "ddd", "ccc", "bbb", "aaa"}
	idx := 0

	for iter.Valid() {
		assert.Equal(t, []byte(expected[idx]), iter.Key())
		iter.Next()
		idx++
	}

	assert.Equal(t, 5, idx)
}

// 测试 ReWind 功能
func TestBTree_Iterator_ReWind(t *testing.T) {
	bt := NewBTree()

	bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt.Put([]byte("bbb"), &data.LogRecordPos{Fid: 1, Offset: 20})
	bt.Put([]byte("ccc"), &data.LogRecordPos{Fid: 1, Offset: 30})

	iter := bt.Iterator(false)

	// 第一次遍历
	count := 0
	for iter.Valid() {
		count++
		iter.Next()
	}
	assert.Equal(t, 3, count)
	assert.False(t, iter.Valid())

	// ReWind 回到起点
	iter.ReWind()
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("aaa"), iter.Key())

	// 第二次遍历
	count = 0
	for iter.Valid() {
		count++
		iter.Next()
	}
	assert.Equal(t, 3, count)
}

// 测试 Seek 功能 - 正向
func TestBTree_Iterator_Seek_Forward(t *testing.T) {
	bt := NewBTree()

	bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt.Put([]byte("bbb"), &data.LogRecordPos{Fid: 1, Offset: 20})
	bt.Put([]byte("ccc"), &data.LogRecordPos{Fid: 1, Offset: 30})
	bt.Put([]byte("ddd"), &data.LogRecordPos{Fid: 1, Offset: 40})
	bt.Put([]byte("eee"), &data.LogRecordPos{Fid: 1, Offset: 50})

	iter := bt.Iterator(false)

	// Seek 到 "ccc"
	iter.Seek([]byte("ccc"))
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("ccc"), iter.Key())

	// 继续遍历
	iter.Next()
	assert.Equal(t, []byte("ddd"), iter.Key())

	iter.Next()
	assert.Equal(t, []byte("eee"), iter.Key())

	iter.Next()
	assert.False(t, iter.Valid())
}

// 测试 Seek 功能 - 反向
func TestBTree_Iterator_Seek_Reverse(t *testing.T) {
	bt := NewBTree()

	bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt.Put([]byte("bbb"), &data.LogRecordPos{Fid: 1, Offset: 20})
	bt.Put([]byte("ccc"), &data.LogRecordPos{Fid: 1, Offset: 30})
	bt.Put([]byte("ddd"), &data.LogRecordPos{Fid: 1, Offset: 40})
	bt.Put([]byte("eee"), &data.LogRecordPos{Fid: 1, Offset: 50})

	iter := bt.Iterator(true)

	// Seek 到 "ccc"
	iter.Seek([]byte("ccc"))
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("ccc"), iter.Key())

	// 继续遍历（反向）
	iter.Next()
	assert.Equal(t, []byte("bbb"), iter.Key())

	iter.Next()
	assert.Equal(t, []byte("aaa"), iter.Key())

	iter.Next()
	assert.False(t, iter.Valid())
}

// 测试 Seek 到不存在的 key
func TestBTree_Iterator_Seek_NotExist(t *testing.T) {
	bt := NewBTree()

	bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt.Put([]byte("ccc"), &data.LogRecordPos{Fid: 1, Offset: 30})
	bt.Put([]byte("eee"), &data.LogRecordPos{Fid: 1, Offset: 50})

	iter := bt.Iterator(false)

	// Seek 到不存在的 key "bbb"（应该定位到 >= "bbb" 的第一个，即 "ccc"）
	iter.Seek([]byte("bbb"))
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("ccc"), iter.Key())
}

// 测试迭代器提前停止（模拟 ItemIteratorG 返回 false）
func TestBTree_Iterator_EarlyStop(t *testing.T) {
	bt := NewBTree()

	bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt.Put([]byte("bbb"), &data.LogRecordPos{Fid: 1, Offset: 20})
	bt.Put([]byte("ccc"), &data.LogRecordPos{Fid: 1, Offset: 30})
	bt.Put([]byte("ddd"), &data.LogRecordPos{Fid: 1, Offset: 40})
	bt.Put([]byte("eee"), &data.LogRecordPos{Fid: 1, Offset: 50})

	iter := bt.Iterator(false)

	// 只遍历前3个元素
	count := 0
	for iter.Valid() && count < 3 {
		count++
		iter.Next()
	}

	assert.Equal(t, 3, count)
	assert.True(t, iter.Valid()) // 应该还有元素
	assert.Equal(t, []byte("ddd"), iter.Key())
}

// 测试 Close 功能
func TestBTree_Iterator_Close(t *testing.T) {
	bt := NewBTree()

	bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt.Put([]byte("bbb"), &data.LogRecordPos{Fid: 1, Offset: 20})

	iter := bt.Iterator(false)
	assert.True(t, iter.Valid())

	// 关闭迭代器
	iter.Close()

	// 关闭后访问会导致问题，这里只是确保 Close 不会 panic
	// 实际使用中关闭后不应该再访问
}

// 测试大量数据的迭代
func TestBTree_Iterator_LargeData(t *testing.T) {
	bt := NewBTree()

	// 插入100个元素
	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		bt.Put(key, &data.LogRecordPos{Fid: uint32(i), Offset: int64(i * 10)})
	}

	// 正向迭代
	iter := bt.Iterator(false)
	count := 0
	for iter.Valid() {
		count++
		iter.Next()
	}
	assert.Equal(t, 100, count)

	// 反向迭代
	iterReverse := bt.Iterator(true)
	count = 0
	for iterReverse.Valid() {
		count++
		iterReverse.Next()
	}
	assert.Equal(t, 100, count)
}
