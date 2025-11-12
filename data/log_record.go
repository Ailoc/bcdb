package data

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

type LogRecordType byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// Header: crc|type|keysize|valuesize
const MaxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

var ErrInvaildCRC = errors.New("invalid crc value")

type LogRecordPos struct {
	Fid    uint32 // 标识文件
	Offset int64  // 标识偏移量
}

type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType //墓碑标识
}

type logRecordHeader struct {
	crc        uint32
	recordType LogRecordType
	keySize    uint32
	valueSize  uint32
}

// 对记录进行编码
// logRecord: crc|type|keysize|valuesize|key|value
func EncodeLogRecord(lr *LogRecord) ([]byte, int64) {
	header := make([]byte, MaxLogRecordHeaderSize)
	// 第5个字节 logRecordType
	header[4] = byte(lr.Type)
	// 开始存储keySize和valueSize
	var index int = 5
	index += binary.PutVarint(header[index:], int64(len(lr.Key)))
	index += binary.PutVarint(header[index:], int64(len(lr.Value)))

	var logSize = index + len(lr.Key) + len(lr.Value)

	encBytes := make([]byte, logSize)
	// 拷贝header数据到起始位置
	copy(encBytes[:index], header[:index])

	// 拷贝key和alue
	copy(encBytes[index:], lr.Key)
	copy(encBytes[index+len(lr.Key):], lr.Value)

	// 执行crc校验
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(logSize)
}

func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: LogRecordType(buf[4]),
	}
	// 获取数据
	index := 5
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)
	return crc
}
