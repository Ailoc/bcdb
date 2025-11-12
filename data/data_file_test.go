package data

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDataFileOpen(t *testing.T) {
	dataFile, err := OpenDataFile("./", 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)
}

func TestDataFileWrite(t *testing.T) {
	dataFile, err := OpenDataFile("./", 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("test"))
	assert.Nil(t, err)
}

func TestDataFile_LoadLogRecord(t *testing.T) {
	file, err := OpenDataFile(os.TempDir(), 22)
	assert.Nil(t, err)
	assert.NotNil(t, file)

	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("zch"),
		Type:  LogRecordNormal,
	}

	encLog, logSize := EncodeLogRecord(rec1)
	err = file.Write(encLog)
	assert.Nil(t, err)

	rec2, size, err := file.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, rec1, rec2)
	assert.Equal(t, logSize, size)
	os.Remove(filepath.Join(os.TempDir(), "000000022", DataFileSuffix))
}
