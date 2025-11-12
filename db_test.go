package bcdb

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// ==================== ListKeys 测试 ====================

// 测试空数据库的 ListKeys
func TestListKeys_Empty(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-listkeys-test-empty"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	keys := db.ListKeys()
	assert.Equal(t, 0, len(keys), "空数据库应该返回空切片")
}

// 测试单个 key 的 ListKeys
func TestListKeys_SingleKey(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-listkeys-test-single"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 插入一个 key
	err = db.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)

	keys := db.ListKeys()
	assert.Equal(t, 1, len(keys))
	assert.Equal(t, []byte("key1"), keys[0])
}

// 测试多个 key 的 ListKeys（正向）
func TestListKeys_MultipleKeys_Forward(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-listkeys-test-multi-forward"
	opts.Reverse = false
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 乱序插入数据
	testData := []string{"ddd", "bbb", "eee", "aaa", "ccc"}
	for _, key := range testData {
		err = db.Put([]byte(key), []byte("value_"+key))
		assert.Nil(t, err)
	}

	keys := db.ListKeys()
	assert.Equal(t, 5, len(keys))

	// 验证按升序排列
	expected := []string{"aaa", "bbb", "ccc", "ddd", "eee"}
	for i, expectedKey := range expected {
		assert.Equal(t, []byte(expectedKey), keys[i])
	}
}

// 测试多个 key 的 ListKeys（反向）
func TestListKeys_MultipleKeys_Reverse(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-listkeys-test-multi-reverse"
	opts.Reverse = true
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 乱序插入数据
	testData := []string{"ddd", "bbb", "eee", "aaa", "ccc"}
	for _, key := range testData {
		err = db.Put([]byte(key), []byte("value_"+key))
		assert.Nil(t, err)
	}

	keys := db.ListKeys()
	assert.Equal(t, 5, len(keys))

	// 验证按降序排列
	expected := []string{"eee", "ddd", "ccc", "bbb", "aaa"}
	for i, expectedKey := range expected {
		assert.Equal(t, []byte(expectedKey), keys[i])
	}
}

// 测试删除后的 ListKeys
func TestListKeys_AfterDelete(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-listkeys-test-delete"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 插入数据
	db.Put([]byte("aaa"), []byte("value_a"))
	db.Put([]byte("bbb"), []byte("value_b"))
	db.Put([]byte("ccc"), []byte("value_c"))
	db.Put([]byte("ddd"), []byte("value_d"))

	// 删除一个 key
	err = db.Delete([]byte("bbb"))
	assert.Nil(t, err)

	keys := db.ListKeys()
	assert.Equal(t, 3, len(keys), "删除后应该只有3个 key")

	// 验证不包含被删除的 key
	for _, key := range keys {
		assert.NotEqual(t, []byte("bbb"), key)
	}

	// 验证包含其他 key
	expectedKeys := [][]byte{[]byte("aaa"), []byte("ccc"), []byte("ddd")}
	for _, expectedKey := range expectedKeys {
		found := false
		for _, key := range keys {
			if bytes.Equal(key, expectedKey) {
				found = true
				break
			}
		}
		assert.True(t, found, "应该包含 key: %s", string(expectedKey))
	}
}

// 测试大量 key 的 ListKeys
func TestListKeys_LargeDataset(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-listkeys-test-large"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 插入1000个 key
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		value := []byte(fmt.Sprintf("value_%04d", i))
		err = db.Put(key, value)
		assert.Nil(t, err)
	}

	keys := db.ListKeys()
	assert.Equal(t, 1000, len(keys))

	// 验证顺序
	for i := 0; i < 999; i++ {
		assert.True(t, bytes.Compare(keys[i], keys[i+1]) < 0, "keys 应该按升序排列")
	}
}

// 测试 ListKeys 返回的 key 不包含重复
func TestListKeys_NoDuplicates(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-listkeys-test-nodup"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 多次 Put 相同的 key
	db.Put([]byte("key1"), []byte("value1"))
	db.Put([]byte("key1"), []byte("value2"))
	db.Put([]byte("key1"), []byte("value3"))
	db.Put([]byte("key2"), []byte("value2"))

	keys := db.ListKeys()
	assert.Equal(t, 2, len(keys), "应该只有2个不同的 key")
}

// ==================== Fold 测试 ====================

// 测试空数据库的 Fold
func TestFold_Empty(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-fold-test-empty"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	count := 0
	err = db.Fold(func(key, value []byte) bool {
		count++
		return true
	})

	assert.Nil(t, err)
	assert.Equal(t, 0, count, "空数据库不应该执行回调")
}

// 测试单个元素的 Fold
func TestFold_SingleElement(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-fold-test-single"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	db.Put([]byte("key1"), []byte("value1"))

	count := 0
	err = db.Fold(func(key, value []byte) bool {
		assert.Equal(t, []byte("key1"), key)
		assert.Equal(t, []byte("value1"), value)
		count++
		return true
	})

	assert.Nil(t, err)
	assert.Equal(t, 1, count)
}

// 测试遍历所有数据
func TestFold_AllElements(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-fold-test-all"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 插入数据
	testData := map[string]string{
		"aaa": "value_a",
		"bbb": "value_b",
		"ccc": "value_c",
	}

	for k, v := range testData {
		db.Put([]byte(k), []byte(v))
	}

	// 收集所有 key-value
	collected := make(map[string]string)
	err = db.Fold(func(key, value []byte) bool {
		collected[string(key)] = string(value)
		return true
	})

	assert.Nil(t, err)
	assert.Equal(t, 3, len(collected))
	assert.Equal(t, testData, collected)
}

// 测试提前退出
func TestFold_EarlyExit(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-fold-test-early-exit"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 插入5条数据
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		db.Put(key, value)
	}

	// 只遍历前3个
	count := 0
	err = db.Fold(func(key, value []byte) bool {
		count++
		return count < 3 // 第3次返回 false，停止遍历
	})

	assert.Nil(t, err)
	assert.Equal(t, 3, count, "应该只遍历3次")
}

// 测试统计操作 - 计数
func TestFold_Count(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-fold-test-count"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 插入数据
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		db.Put(key, value)
	}

	// 统计数量
	count := 0
	err = db.Fold(func(key, value []byte) bool {
		count++
		return true
	})

	assert.Nil(t, err)
	assert.Equal(t, 10, count)
}

// 测试过滤操作 - 只处理符合条件的
func TestFold_Filter(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-fold-test-filter"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 插入数据
	db.Put([]byte("user:001"), []byte("Alice"))
	db.Put([]byte("user:002"), []byte("Bob"))
	db.Put([]byte("product:001"), []byte("Book"))
	db.Put([]byte("product:002"), []byte("Pen"))
	db.Put([]byte("order:001"), []byte("Order1"))

	// 只收集 user: 开头的
	userCount := 0
	err = db.Fold(func(key, value []byte) bool {
		if bytes.HasPrefix(key, []byte("user:")) {
			userCount++
		}
		return true
	})

	assert.Nil(t, err)
	assert.Equal(t, 2, userCount, "应该有2个 user: 前缀的 key")
}

// 测试查找操作 - 找到第一个满足条件的
func TestFold_FindFirst(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-fold-test-find"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 插入数据
	db.Put([]byte("aaa"), []byte("10"))
	db.Put([]byte("bbb"), []byte("20"))
	db.Put([]byte("ccc"), []byte("150"))
	db.Put([]byte("ddd"), []byte("30"))

	// 查找第一个 value > 100 的
	var foundKey, foundValue []byte
	err = db.Fold(func(key, value []byte) bool {
		// 简单判断：value 长度 >= 3
		if len(value) >= 3 {
			foundKey = key
			foundValue = value
			return false // 找到了，停止
		}
		return true
	})

	assert.Nil(t, err)
	assert.Equal(t, []byte("ccc"), foundKey)
	assert.Equal(t, []byte("150"), foundValue)
}

// 测试反向遍历
func TestFold_Reverse(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-fold-test-reverse"
	opts.Reverse = true
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 插入数据
	db.Put([]byte("aaa"), []byte("value_a"))
	db.Put([]byte("bbb"), []byte("value_b"))
	db.Put([]byte("ccc"), []byte("value_c"))

	// 收集顺序
	var keys [][]byte
	err = db.Fold(func(key, value []byte) bool {
		keys = append(keys, key)
		return true
	})

	assert.Nil(t, err)
	assert.Equal(t, 3, len(keys))

	// 验证降序
	expected := []string{"ccc", "bbb", "aaa"}
	for i, expectedKey := range expected {
		assert.Equal(t, []byte(expectedKey), keys[i])
	}
}

// 测试 Fold 跳过已删除的数据
func TestFold_SkipDeleted(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-fold-test-skip-deleted"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 插入数据
	db.Put([]byte("aaa"), []byte("value_a"))
	db.Put([]byte("bbb"), []byte("value_b"))
	db.Put([]byte("ccc"), []byte("value_c"))

	// 删除一条
	db.Delete([]byte("bbb"))

	// 收集 key
	var keys [][]byte
	err = db.Fold(func(key, value []byte) bool {
		keys = append(keys, key)
		return true
	})

	assert.Nil(t, err)
	assert.Equal(t, 2, len(keys), "应该只有2个 key")

	// 验证不包含被删除的
	for _, key := range keys {
		assert.NotEqual(t, []byte("bbb"), key)
	}
}

// 测试聚合操作 - 拼接所有 value
func TestFold_Aggregate(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-fold-test-aggregate"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 插入数据
	db.Put([]byte("key1"), []byte("Hello"))
	db.Put([]byte("key2"), []byte(" "))
	db.Put([]byte("key3"), []byte("World"))

	// 拼接所有 value
	var result []byte
	err = db.Fold(func(key, value []byte) bool {
		result = append(result, value...)
		return true
	})

	assert.Nil(t, err)
	assert.Equal(t, []byte("Hello World"), result)
}

// 测试大数据量的 Fold
func TestFold_LargeDataset(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-fold-test-large"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 插入1000条数据
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		value := []byte(fmt.Sprintf("value_%04d", i))
		db.Put(key, value)
	}

	// 统计
	count := 0
	err = db.Fold(func(key, value []byte) bool {
		count++
		return true
	})

	assert.Nil(t, err)
	assert.Equal(t, 1000, count)
}

// ==================== Open 函数测试 ====================

// 测试使用默认配置打开数据库
func TestOpen_DefaultOptions(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-open-default"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	// assert.NotNil(t, db.activeFile)
	assert.NotNil(t, db.index)
}

// 测试打开不存在的目录（应该自动创建）
func TestOpen_CreateDirectory(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-open-create-dir/subdir/deep"
	_ = os.RemoveAll("/tmp/bcdb-test-open-create-dir")
	defer os.RemoveAll("/tmp/bcdb-test-open-create-dir")

	// 目录不存在，应该自动创建
	_, err := os.Stat(opts.DirPath)
	assert.True(t, os.IsNotExist(err))

	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 验证目录已创建
	_, err = os.Stat(opts.DirPath)
	assert.Nil(t, err)
}

// 测试无效配置 - 空目录路径
func TestOpen_InvalidOptions_EmptyDir(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = ""

	db, err := Open(opts)
	assert.Equal(t, ErrDBDirisEmpty, err)
	assert.Nil(t, db)
}

// 测试无效配置 - MaxFileSize <= 0
func TestOpen_InvalidOptions_InvalidFileSize(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-open-invalid-size"
	opts.MaxFileSize = 0

	db, err := Open(opts)
	assert.Equal(t, ErrMaxFileSizeInvalid, err)
	assert.Nil(t, db)

	opts.MaxFileSize = -100
	db, err = Open(opts)
	assert.Equal(t, ErrMaxFileSizeInvalid, err)
	assert.Nil(t, db)
}

// 测试打开已存在数据的数据库（加载索引）
func TestOpen_LoadExistingData(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-open-existing"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	// 第一次打开，写入数据
	db1, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db1)

	db1.Put([]byte("key1"), []byte("value1"))
	db1.Put([]byte("key2"), []byte("value2"))
	db1.Put([]byte("key3"), []byte("value3"))

	// 关闭（注意：当前代码没有 Close 方法，这里只是模拟）
	// 第二次打开，应该加载之前的数据
	db2, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db2)

	// 验证数据已加载
	val, err := db2.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value1"), val)

	val, err = db2.Get([]byte("key2"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value2"), val)

	val, err = db2.Get([]byte("key3"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value3"), val)
}

// 测试打开空数据库
func TestOpen_EmptyDatabase(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-open-empty"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 验证索引为空
	keys := db.ListKeys()
	assert.Equal(t, 0, len(keys))
}

// ==================== Put 函数测试 ====================

// 测试插入正常的 key-value
func TestPut_Normal(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-put-normal"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	err = db.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)

	// 验证能读取
	val, err := db.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value1"), val)
}

// 测试插入空 key（应该报错）
func TestPut_EmptyKey(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-put-empty-key"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	err = db.Put([]byte(""), []byte("value"))
	assert.Equal(t, ErrKeyisEmpty, err)

	err = db.Put(nil, []byte("value"))
	assert.Equal(t, ErrKeyisEmpty, err)
}

// 测试插入空 value（应该允许）
func TestPut_EmptyValue(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-put-empty-value"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	// 空 value 应该允许
	err = db.Put([]byte("key1"), []byte(""))
	assert.Nil(t, err)

	val, err := db.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte(""), val)

	// nil value 也应该允许
	err = db.Put([]byte("key2"), nil)
	assert.Nil(t, err)

	val, err = db.Get([]byte("key2"))
	assert.Nil(t, err)
	assert.Equal(t, 0, len(val))
}

// 测试插入大 value
func TestPut_LargeValue(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-put-large-value"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	// 创建 1MB 的 value
	largeValue := make([]byte, 1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	err = db.Put([]byte("large_key"), largeValue)
	assert.Nil(t, err)

	// 验证能读取
	val, err := db.Get([]byte("large_key"))
	assert.Nil(t, err)
	assert.Equal(t, largeValue, val)
}

// 测试覆盖已存在的 key
func TestPut_OverwriteKey(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-put-overwrite"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	// 第一次写入
	err = db.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)

	val, err := db.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value1"), val)

	// 覆盖写入
	err = db.Put([]byte("key1"), []byte("value2"))
	assert.Nil(t, err)

	val, err = db.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value2"), val)

	// 再次覆盖
	err = db.Put([]byte("key1"), []byte("value3"))
	assert.Nil(t, err)

	val, err = db.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value3"), val)
}

// 测试并发写入
func TestPut_Concurrent(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-put-concurrent"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	var wg sync.WaitGroup
	count := 100

	// 并发写入
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key_%d", idx))
			value := []byte(fmt.Sprintf("value_%d", idx))
			err := db.Put(key, value)
			assert.Nil(t, err)
		}(i)
	}

	wg.Wait()

	// 验证所有数据
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		expectedValue := []byte(fmt.Sprintf("value_%d", i))
		val, err := db.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, expectedValue, val)
	}
}

// 测试写入触发文件切换
func TestPut_FileSwitching(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-put-file-switching"
	opts.MaxFileSize = 1024 // 设置很小的文件大小，便于触发切换
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	// 写入多条数据，触发文件切换
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d_padding_to_make_it_larger", i))
		err = db.Put(key, value)
		assert.Nil(t, err)
	}

	// 验证有多个文件
	assert.True(t, len(db.olderFiles) > 0, "应该有旧文件")

	// 验证所有数据仍然可读
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		expectedValue := []byte(fmt.Sprintf("value_%d_padding_to_make_it_larger", i))
		val, err := db.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, expectedValue, val)
	}
}

// 测试同步写入
func TestPut_SyncWrite(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-put-sync"
	opts.SyncWrite = true // 开启同步写入
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	err = db.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)

	// 验证能读取
	val, err := db.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value1"), val)
}

// 测试插入大量数据
func TestPut_BulkInsert(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-put-bulk"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	// 插入 10000 条数据
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key_%06d", i))
		value := []byte(fmt.Sprintf("value_%06d", i))
		err = db.Put(key, value)
		assert.Nil(t, err)
	}

	// 验证数量
	keys := db.ListKeys()
	assert.Equal(t, 10000, len(keys))
}

// ==================== Get 函数测试 ====================

// 测试读取存在的 key
func TestGet_ExistingKey(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-get-existing"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	db.Put([]byte("key1"), []byte("value1"))

	val, err := db.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value1"), val)
}

// 测试读取不存在的 key
func TestGet_NonExistingKey(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-get-nonexisting"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	val, err := db.Get([]byte("nonexistent"))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, val)
}

// 测试读取空 key
func TestGet_EmptyKey(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-get-empty-key"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	val, err := db.Get([]byte(""))
	assert.Equal(t, ErrKeyisEmpty, err)
	assert.Nil(t, val)

	val, err = db.Get(nil)
	assert.Equal(t, ErrKeyisEmpty, err)
	assert.Nil(t, val)
}

// 测试读取被覆盖的 key（返回最新值）
func TestGet_OverwrittenKey(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-get-overwritten"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	// 多次写入同一个 key
	db.Put([]byte("key1"), []byte("value1"))
	db.Put([]byte("key1"), []byte("value2"))
	db.Put([]byte("key1"), []byte("value3"))

	// 应该返回最新的值
	val, err := db.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value3"), val)
}

// 测试读取被删除的 key
func TestGet_DeletedKey(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-get-deleted"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	// 写入后删除
	db.Put([]byte("key1"), []byte("value1"))
	db.Delete([]byte("key1"))

	// 读取应该报错
	val, err := db.Get([]byte("key1"))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, val)
}

// 测试并发读取
func TestGet_Concurrent(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-get-concurrent"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	// 先写入数据
	count := 100
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		db.Put(key, value)
	}

	// 并发读取
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key_%d", idx))
			expectedValue := []byte(fmt.Sprintf("value_%d", idx))
			val, err := db.Get(key)
			assert.Nil(t, err)
			assert.Equal(t, expectedValue, val)
		}(i)
	}

	wg.Wait()
}

// 测试读写并发
func TestGet_ConcurrentReadWrite(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-get-rw-concurrent"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	var wg sync.WaitGroup

	// 并发写入
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key_%d", idx))
			value := []byte(fmt.Sprintf("value_%d", idx))
			db.Put(key, value)
		}(i)
	}

	// 并发读取
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key_%d", idx))
			db.Get(key) // 可能读到也可能读不到，不报错即可
		}(i)
	}

	wg.Wait()
}

// 测试读取大 value
func TestGet_LargeValue(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-get-large-value"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	// 创建 5MB 的 value
	largeValue := make([]byte, 5*1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	db.Put([]byte("large_key"), largeValue)

	// 读取
	val, err := db.Get([]byte("large_key"))
	assert.Nil(t, err)
	assert.Equal(t, largeValue, val)
}

// 测试从旧文件读取数据
func TestGet_FromOlderFile(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-get-older-file"
	opts.MaxFileSize = 1024 // 小文件，容易触发切换
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	// 写入数据触发文件切换
	db.Put([]byte("key1"), []byte("value1_padding_to_make_larger_for_file_switching"))

	// 写入更多数据，确保 key1 在旧文件中
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d_padding_large_data", i))
		db.Put(key, value)
	}

	// 验证能从旧文件读取
	val, err := db.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value1_padding_to_make_larger_for_file_switching"), val)
}

// ==================== Delete 函数测试 ====================

// 测试删除存在的 key
func TestDelete_ExistingKey(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-delete-existing"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	// 先插入
	db.Put([]byte("key1"), []byte("value1"))

	// 验证存在
	val, err := db.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value1"), val)

	// 删除
	err = db.Delete([]byte("key1"))
	assert.Nil(t, err)

	// 验证已删除
	val, err = db.Get([]byte("key1"))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, val)
}

// 测试删除不存在的 key（不应该报错）
func TestDelete_NonExistingKey(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-delete-nonexisting"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	// 删除不存在的 key 不应该报错
	err = db.Delete([]byte("nonexistent"))
	assert.Nil(t, err)
}

// 测试删除空 key
func TestDelete_EmptyKey(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-delete-empty-key"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	err = db.Delete([]byte(""))
	assert.Equal(t, ErrKeyNotFound, err)

	err = db.Delete(nil)
	assert.Equal(t, ErrKeyNotFound, err)
}

// 测试删除后再插入相同 key
func TestDelete_ThenPutSameKey(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-delete-then-put"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	// 插入
	db.Put([]byte("key1"), []byte("value1"))

	// 删除
	db.Delete([]byte("key1"))

	// 再次插入
	err = db.Put([]byte("key1"), []byte("value2"))
	assert.Nil(t, err)

	// 验证能读取新值
	val, err := db.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value2"), val)
}

// 测试并发删除
func TestDelete_Concurrent(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-delete-concurrent"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	// 先插入数据
	count := 100
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		db.Put(key, value)
	}

	// 并发删除
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key_%d", idx))
			err := db.Delete(key)
			assert.Nil(t, err)
		}(i)
	}

	wg.Wait()

	// 验证所有数据已删除
	keys := db.ListKeys()
	assert.Equal(t, 0, len(keys))
}

// 测试删除大量数据
func TestDelete_BulkDelete(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-delete-bulk"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	// 插入 1000 条数据
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		value := []byte(fmt.Sprintf("value_%04d", i))
		db.Put(key, value)
	}

	// 删除前 500 条
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		err = db.Delete(key)
		assert.Nil(t, err)
	}

	// 验证剩余 500 条
	keys := db.ListKeys()
	assert.Equal(t, 500, len(keys))

	// 验证前 500 条不可读
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		_, err := db.Get(key)
		assert.Equal(t, ErrKeyNotFound, err)
	}

	// 验证后 500 条可读
	for i := 500; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		val, err := db.Get(key)
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}
}

// 测试重复删除同一个 key
func TestDelete_DuplicateDelete(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-delete-duplicate"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	// 插入
	db.Put([]byte("key1"), []byte("value1"))

	// 第一次删除
	err = db.Delete([]byte("key1"))
	assert.Nil(t, err)

	// 第二次删除（不应该报错）
	err = db.Delete([]byte("key1"))
	assert.Nil(t, err)

	// 第三次删除
	err = db.Delete([]byte("key1"))
	assert.Nil(t, err)
}

// ==================== 集成测试 ====================

// 测试完整的增删改查流程
func TestDB_CRUD_Integration(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-crud-integration"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	// 批量插入
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		value := []byte(fmt.Sprintf("value_%03d", i))
		err = db.Put(key, value)
		assert.Nil(t, err)
	}

	// 验证插入
	keys := db.ListKeys()
	assert.Equal(t, 100, len(keys))

	// 更新部分数据
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		value := []byte(fmt.Sprintf("updated_value_%03d", i))
		err = db.Put(key, value)
		assert.Nil(t, err)
	}

	// 删除部分数据
	for i := 50; i < 75; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		err = db.Delete(key)
		assert.Nil(t, err)
	}

	// 验证最终状态
	keys = db.ListKeys()
	assert.Equal(t, 75, len(keys))

	// 验证更新的数据
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		val, err := db.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("updated_value_%03d", i)), val)
	}

	// 验证未更新的数据
	for i := 75; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		val, err := db.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("value_%03d", i)), val)
	}
}

// 测试数据持久化（重启后数据仍在）
func TestDB_Persistence(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-persistence"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	// 第一次打开，写入数据
	db1, err := Open(opts)
	assert.Nil(t, err)

	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key_%02d", i))
		value := []byte(fmt.Sprintf("value_%02d", i))
		db1.Put(key, value)
	}

	// 模拟重启：重新打开数据库
	db2, err := Open(opts)
	assert.Nil(t, err)

	// 验证数据仍然存在
	keys := db2.ListKeys()
	assert.Equal(t, 50, len(keys))

	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key_%02d", i))
		val, err := db2.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("value_%02d", i)), val)
	}
}

// ==================== Close 方法测试 ====================

// 测试关闭空数据库（activeFile 为 nil）
func TestClose_EmptyDatabase(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-close-empty"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 空数据库 activeFile 为 nil
	assert.Nil(t, db.activeFile)

	// 关闭不应该报错
	err = db.Close()
	assert.Nil(t, err)
}

// 测试关闭有数据的数据库（只有活跃文件）
func TestClose_WithActiveFile(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-close-active"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	// 写入一些数据
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		err = db.Put(key, value)
		assert.Nil(t, err)
	}

	assert.NotNil(t, db.activeFile)
	assert.Equal(t, 0, len(db.olderFiles))

	// 关闭数据库
	err = db.Close()
	assert.Nil(t, err)
}

// 测试关闭有多个文件的数据库
func TestClose_WithMultipleFiles(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-close-multiple"
	opts.MaxFileSize = 1024 // 小文件，容易触发切换
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	// 写入大量数据，触发文件切换
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d_with_padding_to_make_it_larger", i))
		err = db.Put(key, value)
		assert.Nil(t, err)
	}

	// 验证有多个文件
	assert.NotNil(t, db.activeFile)
	assert.True(t, len(db.olderFiles) > 0, "应该有旧文件")

	oldFileCount := len(db.olderFiles)
	t.Logf("旧文件数量: %d", oldFileCount)

	// 关闭数据库
	err = db.Close()
	assert.Nil(t, err)
}

// 测试重复关闭
func TestClose_MultipleTimes(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-close-multiple-times"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	// 写入数据
	db.Put([]byte("key1"), []byte("value1"))

	// 第一次关闭
	err = db.Close()
	assert.Nil(t, err)

	// 第二次关闭（可能会报错，取决于实现）
	err = db.Close()
	// 注意：这里可能报错，也可能不报错，取决于具体实现
	// 如果要求幂等性，应该不报错
}

// 测试关闭后尝试 Put
func TestClose_ThenPut(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-close-then-put"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	// 写入数据
	err = db.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)

	// 关闭数据库
	err = db.Close()
	assert.Nil(t, err)

	// 尝试写入（应该返回错误）
	err = db.Put([]byte("key2"), []byte("value2"))
	// ✅ 修改
	assert.NotNil(t, err, "关闭后写入应该返回错误")
	assert.Equal(t, ErrDBClosed, err)
}

// 测试关闭后尝试 Get
func TestClose_ThenGet(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-close-then-get"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	// 写入数据
	db.Put([]byte("key1"), []byte("value1"))

	// 关闭数据库
	err = db.Close()
	assert.Nil(t, err)

	// 尝试读取（应该返回错误）
	_, err = db.Get([]byte("key1"))
	// ✅ 修改
	assert.NotNil(t, err, "关闭后读取应该返回错误")
	assert.Equal(t, ErrDBClosed, err)
}

// 测试关闭后重新打开
func TestClose_ThenReopen(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-close-reopen"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	// 第一次打开，写入数据
	db1, err := Open(opts)
	assert.Nil(t, err)

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		db1.Put(key, value)
	}

	// 关闭
	err = db1.Close()
	assert.Nil(t, err)

	// 重新打开
	db2, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db2)

	// 验证数据仍然存在
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		val, err := db2.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("value_%d", i)), val)
	}

	// 关闭第二个实例
	db2.Close()
}

// 测试并发关闭（虽然不推荐）
func TestClose_Concurrent(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-close-concurrent"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	// 写入数据
	db.Put([]byte("key1"), []byte("value1"))

	// 并发关闭（不推荐，但应该安全）
	var wg sync.WaitGroup
	errorCount := 0
	var mu sync.Mutex

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := db.Close()
			if err != nil {
				mu.Lock()
				errorCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	// 大部分调用应该成功或安全失败
	t.Logf("并发关闭中的错误次数: %d", errorCount)
}

// ==================== Sync 方法测试 ====================

// 测试同步空数据库
func TestSync_EmptyDatabase(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-sync-empty"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	// 空数据库同步不应该报错
	err = db.Sync()
	assert.Nil(t, err)
}

// 测试同步有数据的数据库
func TestSync_WithData(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-sync-data"
	opts.SyncWrite = false // 关闭自动同步
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	// 写入数据
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		err = db.Put(key, value)
		assert.Nil(t, err)
	}

	// 手动同步
	err = db.Sync()
	assert.Nil(t, err)
}

// 测试写入后立即同步
func TestSync_AfterEachWrite(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-sync-after-write"
	opts.SyncWrite = false
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	// 每次写入后都同步
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))

		err = db.Put(key, value)
		assert.Nil(t, err)

		err = db.Sync()
		assert.Nil(t, err)
	}
}

// 测试多次同步
func TestSync_MultipleTimes(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-sync-multiple"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	// 写入数据
	db.Put([]byte("key1"), []byte("value1"))

	// 多次同步（应该是幂等的）
	for i := 0; i < 5; i++ {
		err = db.Sync()
		assert.Nil(t, err)
	}
}

// 测试并发同步
func TestSync_Concurrent(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-sync-concurrent"
	opts.SyncWrite = false
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	// 写入数据
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		db.Put(key, value)
	}

	// 并发同步
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := db.Sync()
			assert.Nil(t, err)
		}()
	}

	wg.Wait()
}

// 测试 Sync 与 SyncWrite 选项的配合
func TestSync_WithSyncWriteOption(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-sync-with-option"
	opts.SyncWrite = true // 开启自动同步
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	// 写入数据（自动同步）
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		err = db.Put(key, value)
		assert.Nil(t, err)
	}

	// 额外手动同步（应该也没问题）
	err = db.Sync()
	assert.Nil(t, err)
}

// 测试同步后数据可恢复（模拟崩溃重启）
func TestSync_DataPersistence(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-sync-persistence"
	opts.SyncWrite = false
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	// 第一阶段：写入数据并同步
	db1, err := Open(opts)
	assert.Nil(t, err)

	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		err = db1.Put(key, value)
		assert.Nil(t, err)
	}

	// 手动同步
	err = db1.Sync()
	assert.Nil(t, err)

	// 关闭
	db1.Close()

	// 第二阶段：重新打开，验证数据
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer db2.Close()

	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		val, err := db2.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("value_%d", i)), val)
	}
}

// 测试不同步时数据可能丢失（对比测试）
func TestSync_WithoutSyncDataLoss(t *testing.T) {
	// 注意：这个测试可能不稳定，因为即使不调用 Sync，
	// 操作系统也可能会自动刷新缓冲区
	t.Skip("此测试依赖操作系统行为，可能不稳定")

	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-sync-no-sync"
	opts.SyncWrite = false
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	// 写入数据但不同步
	db.Put([]byte("key1"), []byte("value1"))

	// 模拟崩溃：不调用 Close，直接设置为 nil
	// 注意：这只是模拟，实际测试可能无法真正验证数据丢失
	db = nil

	// 在实际场景中，如果没有 Sync，数据可能会丢失
}

// 测试 Sync 性能影响
func TestSync_Performance(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-sync-performance"
	opts.SyncWrite = false
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	// 测试不同步的写入速度
	start1 := time.Now()
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		db.Put(key, value)
	}
	duration1 := time.Since(start1)
	t.Logf("1000次写入（不同步）耗时: %v", duration1)

	// 测试每次都同步的写入速度
	start2 := time.Now()
	for i := 1000; i < 2000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		db.Put(key, value)
		db.Sync() // 每次都同步
	}
	duration2 := time.Since(start2)
	t.Logf("1000次写入（每次同步）耗时: %v", duration2)

	// 同步会显著增加耗时
	t.Logf("同步带来的性能开销: %.2fx", float64(duration2)/float64(duration1))
}

// ==================== Close 和 Sync 集成测试 ====================

// 测试 Sync 后 Close
func TestSync_ThenClose(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-sync-then-close"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	// 写入数据
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		db.Put(key, value)
	}

	// 同步
	err = db.Sync()
	assert.Nil(t, err)

	// 关闭
	err = db.Close()
	assert.Nil(t, err)

	// 重新打开验证
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer db2.Close()

	keys := db2.ListKeys()
	assert.Equal(t, 10, len(keys))
}

// 测试关闭后尝试同步
func TestClose_ThenSync(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-close-then-sync"
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)

	db.Put([]byte("key1"), []byte("value1"))

	// 关闭
	err = db.Close()
	assert.Nil(t, err)

	// 尝试同步（应该返回错误）
	err = db.Sync()
	// ✅ 修改：期望返回 ErrDBClosed 错误
	assert.NotNil(t, err, "关闭后同步应该返回错误")
	assert.Equal(t, ErrDBClosed, err)
}

// 测试写入、同步、关闭、重启的完整流程
func TestSync_FullLifecycle(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-test-sync-lifecycle"
	opts.SyncWrite = false
	_ = os.RemoveAll(opts.DirPath)
	defer os.RemoveAll(opts.DirPath)

	// 第一轮：写入数据
	db1, err := Open(opts)
	assert.Nil(t, err)

	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		db1.Put(key, value)
	}

	db1.Sync()
	db1.Close()

	// 第二轮：继续写入
	db2, err := Open(opts)
	assert.Nil(t, err)

	for i := 50; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		db2.Put(key, value)
	}

	db2.Sync()
	db2.Close()

	// 第三轮：验证所有数据
	db3, err := Open(opts)
	assert.Nil(t, err)
	defer db3.Close()

	keys := db3.ListKeys()
	assert.Equal(t, 100, len(keys))

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		val, err := db3.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("value_%d", i)), val)
	}
}
