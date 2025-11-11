package data

import (
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
	//
}
