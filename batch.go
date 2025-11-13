package bcdb

import (
	"bcdb/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

const (
	NonTxnSeqNo uint64 = 0
)

var (
	TxnFinKey = []byte("txn_fin")
)

type WriteBatch struct {
	options        WriteBatchOptions
	mu             *sync.Mutex
	db             *DB
	pendingWrites  map[string]*data.LogRecord //暂存写入的数据
	indexerStorage map[string]*data.LogRecordPos
}

func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		options:        opts,
		mu:             new(sync.Mutex),
		db:             db,
		pendingWrites:  make(map[string]*data.LogRecord),
		indexerStorage: make(map[string]*data.LogRecordPos),
	}
}

func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyisEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if uint(len(wb.pendingWrites)) >= wb.options.MaxBatchSize {
		return ErrExceedMaxBatchSize
	}

	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyisEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 数据不存在时直接返回
	if wb.db.index.Get(key) == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 如果暂存区没有数据
	if len(wb.pendingWrites) == 0 {
		return nil
	}

	// 获取最新的事务序列号
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)
	// 写入数据到数据文件当中

	for _, rec := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordWithSeqNo(rec.Key, seqNo),
			Value: rec.Value,
			Type:  rec.Type,
		})
		if err != nil {
			return err
		}
		wb.indexerStorage[string(rec.Key)] = logRecordPos
	}

	// 添加标识事务完成的数据
	finishedRecord := &data.LogRecord{
		Key:  logRecordWithSeqNo(TxnFinKey, seqNo),
		Type: data.LogRecordTxnFin,
	}
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	// 根据配置决定是否立即持久化到磁盘
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新内存索引
	for _, rec := range wb.pendingWrites {
		pos := wb.indexerStorage[string(rec.Key)]
		if rec.Type == data.LogRecordDeleted {
			if !wb.db.index.Delete(rec.Key) {
				return ErrIndexUpdateFiled
			}
		}
		if rec.Type == data.LogRecordNormal {
			if !wb.db.index.Put(rec.Key, pos) {
				return ErrIndexUpdateFiled
			}
		}
	}

	// 清空暂存数据
	wb.pendingWrites = make(map[string]*data.LogRecord)
	wb.indexerStorage = make(map[string]*data.LogRecordPos)

	return nil
}

func logRecordWithSeqNo(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq, seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}

func parseLogRecordKey(key []byte) ([]byte, uint64) {
	if len(key) == 0 {
		return nil, 0
	}

	seqNo, n := binary.Uvarint(key)
	if n <= 0 {
		return nil, 0
	}

	return key[n:], seqNo
}
