package bcdb

import (
	"bcdb/data"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ==================== NewWriteBatch 测试 ====================

func TestNewWriteBatch_Normal(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-new-write-batch"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	assert.NotNil(t, wb)
	assert.NotNil(t, wb.mu)
	assert.NotNil(t, wb.db)
	assert.NotNil(t, wb.pendingWrites)
	assert.NotNil(t, wb.indexerStorage)
	assert.Equal(t, 0, len(wb.pendingWrites))
}

func TestNewWriteBatch_CustomOptions(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-new-write-batch-custom"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	batchOpts := WriteBatchOptions{
		MaxBatchSize: 500,
		SyncWrites:   false,
	}

	wb := db.NewWriteBatch(batchOpts)
	assert.NotNil(t, wb)
	assert.Equal(t, uint(500), wb.options.MaxBatchSize)
	assert.False(t, wb.options.SyncWrites)
}

// ==================== WriteBatch.Put 测试 ====================

func TestWriteBatch_Put_Normal(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-batch-put-normal"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)

	err = wb.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(wb.pendingWrites))

	record := wb.pendingWrites["key1"]
	assert.NotNil(t, record)
	assert.Equal(t, []byte("key1"), record.Key)
	assert.Equal(t, []byte("value1"), record.Value)
	assert.Equal(t, data.LogRecordNormal, record.Type)
}

func TestWriteBatch_Put_EmptyKey(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-batch-put-empty-key"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)

	err = wb.Put([]byte(""), []byte("value"))
	assert.Equal(t, ErrKeyisEmpty, err)

	err = wb.Put(nil, []byte("value"))
	assert.Equal(t, ErrKeyisEmpty, err)
}

func TestWriteBatch_Put_EmptyValue(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-batch-put-empty-value"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)

	err = wb.Put([]byte("key1"), []byte(""))
	assert.Nil(t, err)

	err = wb.Put([]byte("key2"), nil)
	assert.Nil(t, err)

	assert.Equal(t, 2, len(wb.pendingWrites))
}

func TestWriteBatch_Put_ExceedMaxBatchSize(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-batch-put-exceed"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	batchOpts := WriteBatchOptions{
		MaxBatchSize: 3,
		SyncWrites:   true,
	}
	wb := db.NewWriteBatch(batchOpts)

	// 添加3个key应该成功
	err = wb.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)
	err = wb.Put([]byte("key2"), []byte("value2"))
	assert.Nil(t, err)
	err = wb.Put([]byte("key3"), []byte("value3"))
	assert.Nil(t, err)

	// 第4个key应该失败
	err = wb.Put([]byte("key4"), []byte("value4"))
	assert.Equal(t, ErrExceedMaxBatchSize, err)
}

func TestWriteBatch_Put_Overwrite(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-batch-put-overwrite"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)

	err = wb.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)

	// 覆盖同一个key
	err = wb.Put([]byte("key1"), []byte("value2"))
	assert.Nil(t, err)

	// 应该只有一个key
	assert.Equal(t, 1, len(wb.pendingWrites))
	record := wb.pendingWrites["key1"]
	assert.Equal(t, []byte("value2"), record.Value)
}

func TestWriteBatch_Put_MultipleKeys(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-batch-put-multiple"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)

	for i := 0; i < 100; i++ {
		key := []byte("key_" + string(rune(i)))
		value := []byte("value_" + string(rune(i)))
		err = wb.Put(key, value)
		assert.Nil(t, err)
	}

	assert.Equal(t, 100, len(wb.pendingWrites))
}

// ==================== WriteBatch.Delete 测试 ====================

func TestWriteBatch_Delete_Normal(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-batch-delete-normal"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	// 先在DB中写入数据
	err = db.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Delete([]byte("key1"))
	assert.Nil(t, err)

	assert.Equal(t, 1, len(wb.pendingWrites))
	record := wb.pendingWrites["key1"]
	assert.NotNil(t, record)
	assert.Equal(t, data.LogRecordDeleted, record.Type)
}

func TestWriteBatch_Delete_EmptyKey(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-batch-delete-empty-key"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)

	err = wb.Delete([]byte(""))
	assert.Equal(t, ErrKeyisEmpty, err)

	err = wb.Delete(nil)
	assert.Equal(t, ErrKeyisEmpty, err)
}

func TestWriteBatch_Delete_NonExistingKey(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-batch-delete-nonexist"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)

	// 删除不存在的key应该返回nil
	err = wb.Delete([]byte("nonexist"))
	assert.Nil(t, err)

	// pendingWrites应该为空
	assert.Equal(t, 0, len(wb.pendingWrites))
}

func TestWriteBatch_Delete_PendingKey(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-batch-delete-pending"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)

	// 先在批次中Put一个key
	err = wb.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(wb.pendingWrites))

	// 删除这个暂存的key
	err = wb.Delete([]byte("key1"))
	assert.Nil(t, err)

	// pendingWrites应该为空
	assert.Equal(t, 0, len(wb.pendingWrites))
}

func TestWriteBatch_Delete_ExistingThenPending(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-batch-delete-exist-pending"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	// 先在DB中写入数据
	err = db.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)

	// 在批次中也Put这个key
	err = wb.Put([]byte("key1"), []byte("value2"))
	assert.Nil(t, err)

	// 删除这个key
	err = wb.Delete([]byte("key1"))
	assert.Nil(t, err)

	// 应该标记为删除类型
	assert.Equal(t, 1, len(wb.pendingWrites))
	record := wb.pendingWrites["key1"]
	assert.Equal(t, data.LogRecordDeleted, record.Type)
}

// ==================== WriteBatch.Commit 测试 ====================

func TestWriteBatch_Commit_EmptyBatch(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-batch-commit-empty"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Commit()
	assert.Nil(t, err)
}

func TestWriteBatch_Commit_SinglePut(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-batch-commit-single"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	// 验证数据已经写入
	value, err := db.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value1"), value)

	// 验证pendingWrites已清空
	assert.Equal(t, 0, len(wb.pendingWrites))
	assert.Equal(t, 0, len(wb.indexerStorage))
}

func TestWriteBatch_Commit_MultiplePuts(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-batch-commit-multiple"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)

	// 批量写入
	for i := 0; i < 10; i++ {
		key := []byte("key_" + string(rune('a'+i)))
		value := []byte("value_" + string(rune('a'+i)))
		err = wb.Put(key, value)
		assert.Nil(t, err)
	}

	err = wb.Commit()
	assert.Nil(t, err)

	// 验证所有数据
	for i := 0; i < 10; i++ {
		key := []byte("key_" + string(rune('a'+i)))
		expectedValue := []byte("value_" + string(rune('a'+i)))
		value, err := db.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, expectedValue, value)
	}
}

func TestWriteBatch_Commit_Delete(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-batch-commit-delete"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	// 先写入数据
	err = db.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Delete([]byte("key1"))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	// 验证数据已删除
	_, err = db.Get([]byte("key1"))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestWriteBatch_Commit_MixedOperations(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-batch-commit-mixed"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	// 先写入一些数据
	db.Put([]byte("key1"), []byte("old_value1"))
	db.Put([]byte("key2"), []byte("old_value2"))

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)

	// 混合操作
	wb.Put([]byte("key1"), []byte("new_value1")) // 更新
	wb.Delete([]byte("key2"))                    // 删除
	wb.Put([]byte("key3"), []byte("value3"))     // 新增

	err = wb.Commit()
	assert.Nil(t, err)

	// 验证结果
	val1, err := db.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("new_value1"), val1)

	_, err = db.Get([]byte("key2"))
	assert.Equal(t, ErrKeyNotFound, err)

	val3, err := db.Get([]byte("key3"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value3"), val3)
}

func TestWriteBatch_Commit_SeqNoIncrement(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-batch-commit-seqno"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	initialSeqNo := db.seqNo

	wb1 := db.NewWriteBatch(DefaultWriteBatchOptions)
	wb1.Put([]byte("key1"), []byte("value1"))
	err = wb1.Commit()
	assert.Nil(t, err)
	assert.Equal(t, initialSeqNo+1, db.seqNo)

	wb2 := db.NewWriteBatch(DefaultWriteBatchOptions)
	wb2.Put([]byte("key2"), []byte("value2"))
	err = wb2.Commit()
	assert.Nil(t, err)
	assert.Equal(t, initialSeqNo+2, db.seqNo)
}

func TestWriteBatch_Commit_Persistence(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-batch-commit-persist"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	// 第一次打开数据库并写入
	db, err := Open(opts)
	assert.Nil(t, err)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	wb.Put([]byte("key1"), []byte("value1"))
	wb.Put([]byte("key2"), []byte("value2"))
	err = wb.Commit()
	assert.Nil(t, err)

	db.Close()

	// 重新打开数据库
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer db2.Close()

	// 验证数据持久化
	val1, err := db2.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value1"), val1)

	val2, err := db2.Get([]byte("key2"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value2"), val2)
}

func TestWriteBatch_Commit_SyncWrites(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-batch-commit-sync"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	// 测试SyncWrites = true
	batchOpts := WriteBatchOptions{
		MaxBatchSize: 1000,
		SyncWrites:   true,
	}
	wb := db.NewWriteBatch(batchOpts)
	wb.Put([]byte("key1"), []byte("value1"))
	err = wb.Commit()
	assert.Nil(t, err)

	// 测试SyncWrites = false
	batchOpts2 := WriteBatchOptions{
		MaxBatchSize: 1000,
		SyncWrites:   false,
	}
	wb2 := db.NewWriteBatch(batchOpts2)
	wb2.Put([]byte("key2"), []byte("value2"))
	err = wb2.Commit()
	assert.Nil(t, err)
}

func TestWriteBatch_Commit_Concurrent(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-batch-commit-concurrent"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	var wg sync.WaitGroup
	batchCount := 10

	for i := 0; i < batchCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			wb := db.NewWriteBatch(DefaultWriteBatchOptions)
			for j := 0; j < 10; j++ {
				key := []byte("key_" + string(rune(idx*10+j)))
				value := []byte("value_" + string(rune(idx*10+j)))
				err := wb.Put(key, value)
				assert.Nil(t, err)
			}
			err := wb.Commit()
			assert.Nil(t, err)
		}(i)
	}

	wg.Wait()

	// 验证所有数据都写入成功
	for i := 0; i < batchCount*10; i++ {
		key := []byte("key_" + string(rune(i)))
		value, err := db.Get(key)
		assert.Nil(t, err)
		assert.NotNil(t, value)
	}
}

func TestWriteBatch_Commit_MultipleCommits(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-batch-multi-commit"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)

	// 第一次提交
	wb.Put([]byte("key1"), []byte("value1"))
	err = wb.Commit()
	assert.Nil(t, err)

	// 第二次提交
	wb.Put([]byte("key2"), []byte("value2"))
	err = wb.Commit()
	assert.Nil(t, err)

	// 验证两次提交的数据都存在
	val1, err := db.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value1"), val1)

	val2, err := db.Get([]byte("key2"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value2"), val2)
}

// ==================== 辅助函数测试 ====================

func TestLogRecordWithSeqNo(t *testing.T) {
	key := []byte("test_key")
	seqNo := uint64(123)

	encKey := logRecordWithSeqNo(key, seqNo)
	assert.NotNil(t, encKey)
	assert.True(t, len(encKey) > len(key))

	// 解析并验证
	parsedKey, parsedSeqNo := parseLogRecordKey(encKey)
	assert.Equal(t, key, parsedKey)
	assert.Equal(t, seqNo, parsedSeqNo)
}

func TestParseLogRecordKey_EmptyKey(t *testing.T) {
	key, seqNo := parseLogRecordKey([]byte(""))
	assert.Nil(t, key)
	assert.Equal(t, uint64(0), seqNo)

	key, seqNo = parseLogRecordKey(nil)
	assert.Nil(t, key)
	assert.Equal(t, uint64(0), seqNo)
}

func TestLogRecordWithSeqNo_ZeroSeqNo(t *testing.T) {
	key := []byte("test_key")
	seqNo := uint64(0)

	encKey := logRecordWithSeqNo(key, seqNo)
	parsedKey, parsedSeqNo := parseLogRecordKey(encKey)

	assert.Equal(t, key, parsedKey)
	assert.Equal(t, seqNo, parsedSeqNo)
}

func TestLogRecordWithSeqNo_LargeSeqNo(t *testing.T) {
	key := []byte("test_key")
	seqNo := uint64(18446744073709551615) // max uint64

	encKey := logRecordWithSeqNo(key, seqNo)
	parsedKey, parsedSeqNo := parseLogRecordKey(encKey)

	assert.Equal(t, key, parsedKey)
	assert.Equal(t, seqNo, parsedSeqNo)
}

// ==================== loadIndexFromDataFiles 基本功能测试 ====================

func TestLoadIndexFromDataFiles_EmptyFiles(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-load-empty"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	assert.Equal(t, 0, db.index.Size())
	assert.Equal(t, uint64(0), db.seqNo)
}

func TestLoadIndexFromDataFiles_NonTransactionData(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-load-non-txn"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	// 第一次打开，写入非事务数据
	db, err := Open(opts)
	assert.Nil(t, err)

	// 非事务写入
	db.Put([]byte("key1"), []byte("value1"))
	db.Put([]byte("key2"), []byte("value2"))
	db.Put([]byte("key3"), []byte("value3"))

	db.Close()

	// 重新打开数据库，测试索引加载
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer db2.Close()

	// 验证索引
	assert.Equal(t, 3, db2.index.Size())

	val1, err := db2.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value1"), val1)

	val2, err := db2.Get([]byte("key2"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value2"), val2)

	val3, err := db2.Get([]byte("key3"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value3"), val3)

	// 非事务数据的seqNo应该为0
	assert.Equal(t, uint64(0), db2.seqNo)
}

func TestLoadIndexFromDataFiles_TransactionData(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-load-txn"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	// 第一次打开，使用事务写入数据
	db, err := Open(opts)
	assert.Nil(t, err)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	wb.Put([]byte("txn_key1"), []byte("txn_value1"))
	wb.Put([]byte("txn_key2"), []byte("txn_value2"))
	err = wb.Commit()
	assert.Nil(t, err)

	seqNo1 := db.seqNo
	db.Close()

	// 重新打开数据库
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer db2.Close()

	// 验证索引
	assert.Equal(t, 2, db2.index.Size())

	val1, err := db2.Get([]byte("txn_key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("txn_value1"), val1)

	val2, err := db2.Get([]byte("txn_key2"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("txn_value2"), val2)

	// 验证seqNo恢复正确
	assert.Equal(t, seqNo1, db2.seqNo)
}

func TestLoadIndexFromDataFiles_MultipleTransactions(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-load-multi-txn"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	// 第一次打开，执行多个事务
	db, err := Open(opts)
	assert.Nil(t, err)

	// 事务1
	wb1 := db.NewWriteBatch(DefaultWriteBatchOptions)
	wb1.Put([]byte("txn1_key1"), []byte("txn1_value1"))
	wb1.Put([]byte("txn1_key2"), []byte("txn1_value2"))
	err = wb1.Commit()
	assert.Nil(t, err)

	// 事务2
	wb2 := db.NewWriteBatch(DefaultWriteBatchOptions)
	wb2.Put([]byte("txn2_key1"), []byte("txn2_value1"))
	wb2.Put([]byte("txn2_key2"), []byte("txn2_value2"))
	err = wb2.Commit()
	assert.Nil(t, err)

	// 事务3
	wb3 := db.NewWriteBatch(DefaultWriteBatchOptions)
	wb3.Put([]byte("txn3_key1"), []byte("txn3_value1"))
	err = wb3.Commit()
	assert.Nil(t, err)

	finalSeqNo := db.seqNo
	db.Close()

	// 重新打开数据库
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer db2.Close()

	// 验证索引
	assert.Equal(t, 5, db2.index.Size())

	// 验证所有key都能正确读取
	val1, err := db2.Get([]byte("txn1_key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("txn1_value1"), val1)

	val2, err := db2.Get([]byte("txn2_key2"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("txn2_value2"), val2)

	val3, err := db2.Get([]byte("txn3_key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("txn3_value1"), val3)

	// 验证seqNo恢复正确
	assert.Equal(t, finalSeqNo, db2.seqNo)
}

func TestLoadIndexFromDataFiles_MixedTransactionAndNonTransaction(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-load-mixed"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	// 第一次打开，混合写入
	db, err := Open(opts)
	assert.Nil(t, err)

	// 非事务写入
	db.Put([]byte("key1"), []byte("value1"))
	db.Put([]byte("key2"), []byte("value2"))

	// 事务写入
	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	wb.Put([]byte("txn_key1"), []byte("txn_value1"))
	wb.Put([]byte("txn_key2"), []byte("txn_value2"))
	err = wb.Commit()
	assert.Nil(t, err)

	// 再次非事务写入
	db.Put([]byte("key3"), []byte("value3"))

	finalSeqNo := db.seqNo
	db.Close()

	// 重新打开数据库
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer db2.Close()

	// 验证索引
	assert.Equal(t, 5, db2.index.Size())

	// 验证非事务数据
	val1, err := db2.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value1"), val1)

	// 验证事务数据
	txnVal, err := db2.Get([]byte("txn_key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("txn_value1"), txnVal)

	// 验证seqNo恢复正确（应该是事务的seqNo，而不是0）
	assert.Equal(t, finalSeqNo, db2.seqNo)
}

// ==================== 删除操作测试 ====================

func TestLoadIndexFromDataFiles_WithDelete(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-load-delete"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	// 第一次打开
	db, err := Open(opts)
	assert.Nil(t, err)

	// 写入数据
	db.Put([]byte("key1"), []byte("value1"))
	db.Put([]byte("key2"), []byte("value2"))
	db.Put([]byte("key3"), []byte("value3"))

	// 删除一个key
	db.Delete([]byte("key2"))

	db.Close()

	// 重新打开数据库
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer db2.Close()

	// 验证索引
	assert.Equal(t, 2, db2.index.Size())

	// key2应该不存在
	_, err = db2.Get([]byte("key2"))
	assert.Equal(t, ErrKeyNotFound, err)

	// 其他key应该存在
	val1, err := db2.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value1"), val1)

	val3, err := db2.Get([]byte("key3"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value3"), val3)
}

func TestLoadIndexFromDataFiles_TransactionWithDelete(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-load-txn-delete"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	// 第一次打开
	db, err := Open(opts)
	assert.Nil(t, err)

	// 先写入数据
	db.Put([]byte("key1"), []byte("value1"))
	db.Put([]byte("key2"), []byte("value2"))
	db.Put([]byte("key3"), []byte("value3"))

	// 使用事务删除
	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	wb.Delete([]byte("key2"))
	wb.Put([]byte("key4"), []byte("value4"))
	err = wb.Commit()
	assert.Nil(t, err)

	db.Close()

	// 重新打开数据库
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer db2.Close()

	// 验证索引
	assert.Equal(t, 3, db2.index.Size())

	// key2应该被删除
	_, err = db2.Get([]byte("key2"))
	assert.Equal(t, ErrKeyNotFound, err)

	// 其他key应该存在
	val1, err := db2.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value1"), val1)

	val4, err := db2.Get([]byte("key4"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value4"), val4)
}

func TestLoadIndexFromDataFiles_MultipleDeletes(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-load-multi-delete"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	// 第一次打开
	db, err := Open(opts)
	assert.Nil(t, err)

	// 写入数据
	for i := 0; i < 10; i++ {
		key := []byte("key_" + string(rune('a'+i)))
		value := []byte("value_" + string(rune('a'+i)))
		db.Put(key, value)
	}

	// 删除部分数据
	db.Delete([]byte("key_b"))
	db.Delete([]byte("key_d"))
	db.Delete([]byte("key_f"))

	db.Close()

	// 重新打开数据库
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer db2.Close()

	// 验证索引大小
	assert.Equal(t, 7, db2.index.Size())

	// 验证被删除的key不存在
	_, err = db2.Get([]byte("key_b"))
	assert.Equal(t, ErrKeyNotFound, err)
	_, err = db2.Get([]byte("key_d"))
	assert.Equal(t, ErrKeyNotFound, err)
	_, err = db2.Get([]byte("key_f"))
	assert.Equal(t, ErrKeyNotFound, err)

	// 验证其他key存在
	val, err := db2.Get([]byte("key_a"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value_a"), val)
}

// ==================== 序列号处理测试 ====================

func TestLoadIndexFromDataFiles_SeqNoRecovery(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-load-seqno"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	// 第一次打开
	db, err := Open(opts)
	assert.Nil(t, err)

	// 执行多个事务
	for i := 0; i < 5; i++ {
		wb := db.NewWriteBatch(DefaultWriteBatchOptions)
		key := []byte("key_" + string(rune('a'+i)))
		value := []byte("value_" + string(rune('a'+i)))
		wb.Put(key, value)
		err = wb.Commit()
		assert.Nil(t, err)
	}

	expectedSeqNo := db.seqNo
	assert.Equal(t, uint64(5), expectedSeqNo)

	db.Close()

	// 重新打开数据库
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer db2.Close()

	// 验证seqNo恢复正确
	assert.Equal(t, expectedSeqNo, db2.seqNo)

	// 继续执行事务，seqNo应该继续递增
	wb := db2.NewWriteBatch(DefaultWriteBatchOptions)
	wb.Put([]byte("new_key"), []byte("new_value"))
	err = wb.Commit()
	assert.Nil(t, err)

	assert.Equal(t, uint64(6), db2.seqNo)
}

func TestLoadIndexFromDataFiles_MaxSeqNo(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-load-max-seqno"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	// 第一次打开
	db, err := Open(opts)
	assert.Nil(t, err)

	// 混合事务和非事务操作
	db.Put([]byte("non_txn_key"), []byte("non_txn_value")) // seqNo = 0

	wb1 := db.NewWriteBatch(DefaultWriteBatchOptions)
	wb1.Put([]byte("txn1_key"), []byte("txn1_value"))
	wb1.Commit() // seqNo = 1

	db.Put([]byte("non_txn_key2"), []byte("non_txn_value2")) // seqNo = 0

	wb2 := db.NewWriteBatch(DefaultWriteBatchOptions)
	wb2.Put([]byte("txn2_key"), []byte("txn2_value"))
	wb2.Commit() // seqNo = 2

	// 最大seqNo应该是2
	assert.Equal(t, uint64(2), db.seqNo)

	db.Close()

	// 重新打开数据库
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer db2.Close()

	// seqNo应该恢复为最大值
	assert.Equal(t, uint64(2), db2.seqNo)
}

// ==================== 文件处理测试 ====================

func TestLoadIndexFromDataFiles_MultipleFiles(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-load-multi-files"
	opts.MaxFileSize = 64 * 1024 // 64KB，方便触发文件切换
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	// 第一次打开
	db, err := Open(opts)
	assert.Nil(t, err)

	// 写入大量数据，触发文件切换
	for i := 0; i < 1000; i++ {
		key := []byte("key_" + string(rune(i)))
		value := make([]byte, 100) // 100字节的value
		for j := range value {
			value[j] = byte(i % 256)
		}
		err = db.Put(key, value)
		assert.Nil(t, err)
	}

	db.Close()

	// 重新打开数据库
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer db2.Close()

	// 验证所有数据都能读取
	for i := 0; i < 1000; i++ {
		key := []byte("key_" + string(rune(i)))
		value, err := db2.Get(key)
		assert.Nil(t, err)
		assert.NotNil(t, value)
		assert.Equal(t, 100, len(value))
	}
}

func TestLoadIndexFromDataFiles_ActiveFileOffset(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-load-offset"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	// 第一次打开
	db, err := Open(opts)
	assert.Nil(t, err)

	// 写入一些数据
	db.Put([]byte("key1"), []byte("value1"))
	db.Put([]byte("key2"), []byte("value2"))
	db.Put([]byte("key3"), []byte("value3"))

	// 记录当前偏移量
	expectedOffset := db.activeFile.WriteOffset
	assert.True(t, expectedOffset > 0)

	db.Close()

	// 重新打开数据库
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer db2.Close()

	// 验证活跃文件的WriteOffset正确恢复
	assert.Equal(t, expectedOffset, db2.activeFile.WriteOffset)

	// 继续写入数据，offset应该继续增长
	db2.Put([]byte("key4"), []byte("value4"))
	assert.True(t, db2.activeFile.WriteOffset > expectedOffset)
}

func TestLoadIndexFromDataFiles_OverwriteValue(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-load-overwrite"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	// 第一次打开
	db, err := Open(opts)
	assert.Nil(t, err)

	// 多次写入同一个key
	db.Put([]byte("key1"), []byte("value1"))
	db.Put([]byte("key1"), []byte("value2"))
	db.Put([]byte("key1"), []byte("value3"))

	db.Close()

	// 重新打开数据库
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer db2.Close()

	// 索引中应该只有一个key
	assert.Equal(t, 1, db2.index.Size())

	// 应该读取到最后一次写入的值
	val, err := db2.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value3"), val)
}

func TestLoadIndexFromDataFiles_PutDeletePut(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-load-put-del-put"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	// 第一次打开
	db, err := Open(opts)
	assert.Nil(t, err)

	// Put -> Delete -> Put
	db.Put([]byte("key1"), []byte("value1"))
	db.Delete([]byte("key1"))
	db.Put([]byte("key1"), []byte("value2"))

	db.Close()

	// 重新打开数据库
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer db2.Close()

	// key1应该存在，值为value2
	val, err := db2.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value2"), val)
}

func TestLoadIndexFromDataFiles_LargeDataset(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-load-large"
	opts.MaxFileSize = 128 * 1024 // 128KB
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	// 第一次打开
	db, err := Open(opts)
	assert.Nil(t, err)

	// 写入大量数据
	totalKeys := 5000
	for i := 0; i < totalKeys; i++ {
		key := []byte("large_key_" + string(rune(i)))
		value := []byte("large_value_" + string(rune(i)))
		err = db.Put(key, value)
		assert.Nil(t, err)
	}

	db.Close()

	// 重新打开数据库
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer db2.Close()

	// 验证索引大小
	assert.Equal(t, totalKeys, db2.index.Size())

	// 随机验证一些key
	checkKeys := []int{0, 100, 500, 1000, 2500, 4999}
	for _, idx := range checkKeys {
		key := []byte("large_key_" + string(rune(idx)))
		expectedValue := []byte("large_value_" + string(rune(idx)))
		value, err := db2.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, expectedValue, value)
	}
}

func TestLoadIndexFromDataFiles_TransactionWithOverwrite(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-load-txn-overwrite"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	// 第一次打开
	db, err := Open(opts)
	assert.Nil(t, err)

	// 写入初始数据
	db.Put([]byte("key1"), []byte("old_value"))

	// 使用事务覆盖
	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	wb.Put([]byte("key1"), []byte("new_value"))
	err = wb.Commit()
	assert.Nil(t, err)

	db.Close()

	// 重新打开数据库
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer db2.Close()

	// 应该读取到事务中的新值
	val, err := db2.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("new_value"), val)
}
