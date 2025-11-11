package data

import (
	"bcdb/fio"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

const (
	DataFileSuffix = ".data"
)

type DataFile struct {
	Fid         uint32
	WriteOffset int64 // Offset of the next write operation
	IOManager   fio.IOManager
}

// 打开一个数据文件
func OpenDataFile(dirPath string, fid uint32) (*DataFile, error) {
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fid)+DataFileSuffix)
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		Fid:         fid,
		WriteOffset: 0,
		IOManager:   ioManager,
	}, nil
}

// 根据数据偏移读取数据
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}
	var headerBytes int64 = MaxLogRecordHeaderSize
	// 读取的数据量超过了文件大小，则只读到文件末尾
	if offset+headerBytes > fileSize {
		headerBytes = fileSize - offset
	}
	headerBuf, err := df.readNBytes(headerBytes, offset) //从offset位置读取最大头部长度
	if err != nil {
		return nil, 0, err
	}
	header, headerSize := decodeLogRecordHeader(headerBuf)
	// header == nil说明读到了文件末尾
	if header == nil {
		return nil, 0, io.EOF
	}

	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}
	// 获取keySize和valueSize
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	recordSize := headerSize + keySize + valueSize

	logRecord := &LogRecord{Type: header.recordType}
	// 读取key和value
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	// 校验数据的crc
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvaildCRC
	}
	return logRecord, recordSize, nil
}

func (df *DataFile) Sync() error {
	return df.IOManager.Sync()
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.IOManager.Write(buf)
	if err != nil {
		return err
	}
	// 更新写入DataFile的offset
	df.WriteOffset += int64(n)
	return nil
}

func (df *DataFile) Close() error {
	return df.IOManager.Close()
}

func (df *DataFile) readNBytes(n, offset int64) (buf []byte, err error) {
	buf = make([]byte, n)
	_, err = df.IOManager.Read(buf, offset)
	return
}
