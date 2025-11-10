package fio

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func RemoveTestFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestNewFileManager(t *testing.T) {
	filePath := filepath.Join("/tmp", "a.data")
	file, err := NewFileIOManager(filePath)
	defer RemoveTestFile(filePath)
	assert.Nil(t, err)
	assert.NotNil(t, file)
}

func TestFileManager_Write(t *testing.T) {
	filePath := filepath.Join("/tmp", "a.data")
	file, err := NewFileIOManager(filePath)
	defer RemoveTestFile(filePath)
	assert.Nil(t, err)
	assert.NotNil(t, file)

	n, err := file.Write([]byte("Hello, World!"))
	t.Log(n, err)

	n, err = file.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)
}

func TestFileManager_Read(t *testing.T) {
	filePath := filepath.Join("/tmp", "a.data")
	file, err := NewFileIOManager(filePath)
	defer RemoveTestFile(filePath)
	assert.Nil(t, err)
	assert.NotNil(t, file)

	n, err := file.Write([]byte("Hello, World!"))
	t.Log(n, err)

	buf := make([]byte, 5)
	n, err = file.Read(buf, 0)
	assert.Nil(t, err)
	assert.Equal(t, 5, n)
	t.Log(string(buf))

	buf = make([]byte, 5)
	n, err = file.Read(buf, 5)
	assert.Nil(t, err)
	assert.Equal(t, 5, n)
	t.Log(string(buf))
}

func TestSync(t *testing.T) {
	filePath := filepath.Join("/tmp", "a.data")
	file, err := NewFileIOManager(filePath)
	defer RemoveTestFile(filePath)
	assert.Nil(t, err)
	assert.NotNil(t, file)

	err = file.Sync()
	assert.Nil(t, err)
}

func TestClose(t *testing.T) {
	filePath := filepath.Join("/tmp", "a.data")
	file, err := NewFileIOManager(filePath)
	defer RemoveTestFile(filePath)
	assert.Nil(t, err)
	assert.NotNil(t, file)

	err = file.Close()
	assert.Nil(t, err)
}
