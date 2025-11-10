package data

import (
	"bcdb/fio"
)

const (
	DataFileSuffix = ".data"
)

type DataFile struct {
	Fid         uint32
	WriteOffset int64 // Offset of the next write operation
	IOManager   fio.IOManager
}

func OpenDataFile(dirPath string, fid uint32) (*DataFile, error) {
	return nil, nil
}

// 根据数据偏移读取数据
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}

func (df *DataFile) Sync() error {
	return nil
}

func (df *DataFile) Write(buf []byte) error {
	return nil
}
