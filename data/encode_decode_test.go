package data

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeRecord(t *testing.T) {

	rec1 := &LogRecord{
		Key:   []byte("hello"),
		Value: []byte("w"),
		Type:  LogRecordNormal,
	}

	res1, n1 := EncodeLogRecord(rec1)
	t.Log(res1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))

}

func TestDecodeRecord(t *testing.T) {

	bytes := []byte{77, 133, 26, 73, 0, 10, 2}

	h1, n1 := decodeLogRecordHeader(bytes)
	assert.NotNil(t, h1)
	assert.Equal(t, int64(7), n1)
	t.Log(h1.keySize)
	t.Log(h1.valueSize)
	t.Log(h1.recordType)
	t.Log(h1.crc)
}

func TestGetLogCRC(t *testing.T) {

	rec1 := &LogRecord{
		Key:   []byte("hello"),
		Value: []byte("w"),
		Type:  LogRecordNormal,
	}

	bytes := []byte{77, 133, 26, 73, 0, 10, 2}

	crc := getLogRecordCRC(rec1, bytes)
	t.Log(crc)
}
