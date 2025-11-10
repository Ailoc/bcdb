package index

import (
	"bcdb/data"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 23})
	assert.True(t, res1)

	res2 := bt.Put([]byte("hello"), &data.LogRecordPos{Fid: 1, Offset: 32})
	assert.True(t, res2)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 23})
	assert.True(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(23), pos1.Offset)

	res2 := bt.Put([]byte("hello"), &data.LogRecordPos{Fid: 1, Offset: 32})
	assert.True(t, res2)
	res3 := bt.Put([]byte("hello"), &data.LogRecordPos{Fid: 1, Offset: 22})
	assert.True(t, res3)

	pos2 := bt.Get([]byte("hello"))
	t.Log(pos2)
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(22), pos2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 23})
	assert.True(t, res1)
	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([]byte("hello"), &data.LogRecordPos{Fid: 1, Offset: 32})
	assert.True(t, res3)
	res4 := bt.Delete([]byte("hello"))
	assert.True(t, res4)
}
