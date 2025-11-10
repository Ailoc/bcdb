package data

type LogRecordType byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

type LogRecordPos struct {
	Fid    uint32 // 标识文件
	Offset int64  // 标识偏移量
}

type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType //墓碑标识
}

// 对记录进行编码
func EncodeLogRecord(lr *LogRecord) ([]byte, int64) {
	return nil, 0
}
