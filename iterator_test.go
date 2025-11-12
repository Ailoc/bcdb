package bcdb

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ==================== 辅助函数 ====================

// 创建测试用的 DB
func createTestDB(t *testing.T) *DB {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bcdb-iterator-test"

	// 清理旧数据
	_ = os.RemoveAll(opts.DirPath)

	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	return db
}

// 清理测试 DB
func destroyTestDB(t *testing.T, db *DB) {
	if db != nil {
		err := os.RemoveAll(db.options.DirPath)
		assert.Nil(t, err)
	}
}

// ==================== 基础功能测试 ====================

// 测试空数据库的迭代器
func TestIterator_Empty(t *testing.T) {
	db := createTestDB(t)
	defer destroyTestDB(t, db)

	// 正向迭代
	iter := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iter)
	assert.False(t, iter.Valid(), "空数据库迭代器应该无效")
	iter.Close()

	// 反向迭代
	opts := IteratorOptions{Reverse: true}
	iterReverse := db.NewIterator(opts)
	assert.NotNil(t, iterReverse)
	assert.False(t, iterReverse.Valid(), "空数据库反向迭代器应该无效")
	iterReverse.Close()
}

// 测试单个元素的迭代
func TestIterator_SingleElement(t *testing.T) {
	db := createTestDB(t)
	defer destroyTestDB(t, db)

	// 插入一条数据
	err := db.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)

	// 迭代
	iter := db.NewIterator(DefaultIteratorOptions)
	defer iter.Close()

	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("key1"), iter.Key())

	value, err := iter.Value()
	assert.Nil(t, err)
	assert.Equal(t, []byte("value1"), value)

	iter.Next()
	assert.False(t, iter.Valid(), "单元素迭代后应该无效")
}

// 测试正向迭代所有数据
func TestIterator_ForwardIteration(t *testing.T) {
	db := createTestDB(t)
	defer destroyTestDB(t, db)

	// 插入多条数据（乱序）
	testData := map[string]string{
		"ddd": "value_d",
		"bbb": "value_b",
		"eee": "value_e",
		"aaa": "value_a",
		"ccc": "value_c",
	}

	for k, v := range testData {
		err := db.Put([]byte(k), []byte(v))
		assert.Nil(t, err)
	}

	// 正向迭代，应该按 key 升序
	iter := db.NewIterator(DefaultIteratorOptions)
	defer iter.Close()

	expected := []string{"aaa", "bbb", "ccc", "ddd", "eee"}
	idx := 0

	for iter.Valid() {
		assert.Equal(t, []byte(expected[idx]), iter.Key())

		value, err := iter.Value()
		assert.Nil(t, err)
		assert.Equal(t, []byte("value_"+string(expected[idx][0])), value)

		iter.Next()
		idx++
	}

	assert.Equal(t, 5, idx, "应该遍历所有5个元素")
}

// 测试反向迭代所有数据
func TestIterator_ReverseIteration(t *testing.T) {
	db := createTestDB(t)
	defer destroyTestDB(t, db)

	// 插入多条数据
	testData := map[string]string{
		"ddd": "value_d",
		"bbb": "value_b",
		"eee": "value_e",
		"aaa": "value_a",
		"ccc": "value_c",
	}

	for k, v := range testData {
		err := db.Put([]byte(k), []byte(v))
		assert.Nil(t, err)
	}

	// 反向迭代，应该按 key 降序
	opts := IteratorOptions{Reverse: true}
	iter := db.NewIterator(opts)
	defer iter.Close()

	expected := []string{"eee", "ddd", "ccc", "bbb", "aaa"}
	idx := 0

	for iter.Valid() {
		assert.Equal(t, []byte(expected[idx]), iter.Key())

		value, err := iter.Value()
		assert.Nil(t, err)
		assert.Equal(t, []byte("value_"+string(expected[idx][0])), value)

		iter.Next()
		idx++
	}

	assert.Equal(t, 5, idx, "应该遍历所有5个元素")
}

// ==================== ReWind 和 Seek 测试 ====================

// 测试 ReWind 重置功能
func TestIterator_ReWind(t *testing.T) {
	db := createTestDB(t)
	defer destroyTestDB(t, db)

	// 插入数据
	db.Put([]byte("aaa"), []byte("value_a"))
	db.Put([]byte("bbb"), []byte("value_b"))
	db.Put([]byte("ccc"), []byte("value_c"))

	iter := db.NewIterator(DefaultIteratorOptions)
	defer iter.Close()

	// 第一次遍历
	count := 0
	for iter.Valid() {
		count++
		iter.Next()
	}
	assert.Equal(t, 3, count)
	assert.False(t, iter.Valid())

	// ReWind 重置
	iter.ReWind()
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("aaa"), iter.Key())

	// 第二次遍历
	count = 0
	for iter.Valid() {
		count++
		iter.Next()
	}
	assert.Equal(t, 3, count)
}

// 测试 Seek 定位
func TestIterator_Seek(t *testing.T) {
	db := createTestDB(t)
	defer destroyTestDB(t, db)

	// 插入数据
	db.Put([]byte("aaa"), []byte("value_a"))
	db.Put([]byte("bbb"), []byte("value_b"))
	db.Put([]byte("ccc"), []byte("value_c"))
	db.Put([]byte("ddd"), []byte("value_d"))
	db.Put([]byte("eee"), []byte("value_e"))

	iter := db.NewIterator(DefaultIteratorOptions)
	defer iter.Close()

	// Seek 到 "ccc"
	iter.Seek([]byte("ccc"))
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("ccc"), iter.Key())

	// 继续遍历
	iter.Next()
	assert.Equal(t, []byte("ddd"), iter.Key())

	iter.Next()
	assert.Equal(t, []byte("eee"), iter.Key())
}

// ==================== 前缀过滤测试（核心功能） ====================

// 测试前缀过滤 - 正向遍历
func TestIterator_PrefixFilter_Forward(t *testing.T) {
	db := createTestDB(t)
	defer destroyTestDB(t, db)

	// 插入多组数据
	db.Put([]byte("user:001"), []byte("Alice"))
	db.Put([]byte("user:002"), []byte("Bob"))
	db.Put([]byte("user:003"), []byte("Charlie"))
	db.Put([]byte("product:001"), []byte("Book"))
	db.Put([]byte("product:002"), []byte("Pen"))
	db.Put([]byte("order:001"), []byte("Order1"))

	// 使用前缀 "user:" 过滤
	opts := IteratorOptions{
		Prefix:  []byte("user:"),
		Reverse: false,
	}
	iter := db.NewIterator(opts)
	defer iter.Close()

	// 应该只遍历 user: 开头的数据
	expectedKeys := []string{"user:001", "user:002", "user:003"}
	expectedValues := []string{"Alice", "Bob", "Charlie"}
	idx := 0

	for iter.Valid() {
		assert.Equal(t, []byte(expectedKeys[idx]), iter.Key())

		value, err := iter.Value()
		assert.Nil(t, err)
		assert.Equal(t, []byte(expectedValues[idx]), value)

		iter.Next()
		idx++
	}

	assert.Equal(t, 3, idx, "应该只遍历3个 user: 前缀的数据")
}

// 测试前缀过滤 - 反向遍历
func TestIterator_PrefixFilter_Reverse(t *testing.T) {
	db := createTestDB(t)
	defer destroyTestDB(t, db)

	// 插入多组数据
	db.Put([]byte("user:001"), []byte("Alice"))
	db.Put([]byte("user:002"), []byte("Bob"))
	db.Put([]byte("user:003"), []byte("Charlie"))
	db.Put([]byte("product:001"), []byte("Book"))
	db.Put([]byte("product:002"), []byte("Pen"))

	// 使用前缀 "user:" 反向遍历
	opts := IteratorOptions{
		Prefix:  []byte("user:"),
		Reverse: true,
	}
	iter := db.NewIterator(opts)
	defer iter.Close()

	// 应该按降序遍历 user: 开头的数据
	expectedKeys := []string{"user:003", "user:002", "user:001"}
	expectedValues := []string{"Charlie", "Bob", "Alice"}
	idx := 0

	for iter.Valid() {
		assert.Equal(t, []byte(expectedKeys[idx]), iter.Key())

		value, err := iter.Value()
		assert.Nil(t, err)
		assert.Equal(t, []byte(expectedValues[idx]), value)

		iter.Next()
		idx++
	}

	assert.Equal(t, 3, idx, "应该只遍历3个 user: 前缀的数据")
}

// 测试前缀不存在的情况
func TestIterator_PrefixFilter_NoMatch(t *testing.T) {
	db := createTestDB(t)
	defer destroyTestDB(t, db)

	// 插入数据
	db.Put([]byte("aaa"), []byte("value_a"))
	db.Put([]byte("bbb"), []byte("value_b"))
	db.Put([]byte("ccc"), []byte("value_c"))

	// 使用不存在的前缀
	opts := IteratorOptions{
		Prefix: []byte("zzz"),
	}
	iter := db.NewIterator(opts)
	defer iter.Close()

	// 应该立即无效
	assert.False(t, iter.Valid(), "不匹配的前缀应该使迭代器无效")
}

// 测试空前缀（遍历所有数据）
func TestIterator_EmptyPrefix(t *testing.T) {
	db := createTestDB(t)
	defer destroyTestDB(t, db)

	// 插入数据
	db.Put([]byte("aaa"), []byte("value_a"))
	db.Put([]byte("bbb"), []byte("value_b"))
	db.Put([]byte("ccc"), []byte("value_c"))

	// 空前缀应该遍历所有数据
	opts := IteratorOptions{
		Prefix: nil,
	}
	iter := db.NewIterator(opts)
	defer iter.Close()

	count := 0
	for iter.Valid() {
		count++
		iter.Next()
	}

	assert.Equal(t, 3, count, "空前缀应该遍历所有数据")
}

// 测试前缀匹配边界情况
func TestIterator_PrefixFilter_Boundary(t *testing.T) {
	db := createTestDB(t)
	defer destroyTestDB(t, db)

	// 插入边界数据
	db.Put([]byte("user"), []byte("user_root")) // 刚好等于前缀
	db.Put([]byte("user:001"), []byte("Alice")) // 匹配前缀
	db.Put([]byte("user:002"), []byte("Bob"))   // 匹配前缀
	db.Put([]byte("userx"), []byte("userx"))    // 不匹配（虽然开头相同）
	db.Put([]byte("usr"), []byte("usr"))        // 不匹配

	// 使用前缀 "user:"
	opts := IteratorOptions{
		Prefix: []byte("user:"),
	}
	iter := db.NewIterator(opts)
	defer iter.Close()

	// 应该只匹配 user:001 和 user:002
	count := 0
	for iter.Valid() {
		key := iter.Key()
		assert.True(t, len(key) >= 5 && string(key[:5]) == "user:")
		count++
		iter.Next()
	}

	assert.Equal(t, 2, count, "应该只匹配带 user: 前缀的数据")
}

// 测试 Seek 与前缀过滤结合
func TestIterator_Seek_WithPrefix(t *testing.T) {
	db := createTestDB(t)
	defer destroyTestDB(t, db)

	// 插入数据
	db.Put([]byte("user:001"), []byte("Alice"))
	db.Put([]byte("user:002"), []byte("Bob"))
	db.Put([]byte("user:003"), []byte("Charlie"))
	db.Put([]byte("user:004"), []byte("David"))
	db.Put([]byte("product:001"), []byte("Book"))

	// 前缀 + Seek
	opts := IteratorOptions{
		Prefix: []byte("user:"),
	}
	iter := db.NewIterator(opts)
	defer iter.Close()

	// Seek 到 user:002
	iter.Seek([]byte("user:002"))
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("user:002"), iter.Key())

	// 继续遍历，仍然受前缀限制
	count := 0
	for iter.Valid() {
		count++
		iter.Next()
	}

	assert.Equal(t, 3, count, "从 user:002 开始应该还有3个元素")
}

// ==================== 删除数据测试 ====================

// 测试迭代过程中跳过已删除的数据
func TestIterator_SkipDeleted(t *testing.T) {
	db := createTestDB(t)
	defer destroyTestDB(t, db)

	// 插入数据
	db.Put([]byte("aaa"), []byte("value_a"))
	db.Put([]byte("bbb"), []byte("value_b"))
	db.Put([]byte("ccc"), []byte("value_c"))
	db.Put([]byte("ddd"), []byte("value_d"))

	// 删除一条数据
	err := db.Delete([]byte("bbb"))
	assert.Nil(t, err)

	// 迭代，应该跳过被删除的 key
	iter := db.NewIterator(DefaultIteratorOptions)
	defer iter.Close()

	expectedKeys := []string{"aaa", "ccc", "ddd"}
	idx := 0

	for iter.Valid() {
		assert.Equal(t, []byte(expectedKeys[idx]), iter.Key())
		idx++
		iter.Next()
	}

	assert.Equal(t, 3, idx, "应该只遍历3个未删除的数据")
}

// ==================== 大数据量测试 ====================

// 测试大量数据的迭代
func TestIterator_LargeDataset(t *testing.T) {
	db := createTestDB(t)
	defer destroyTestDB(t, db)

	// 插入1000条数据
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		value := []byte(fmt.Sprintf("value_%04d", i))
		err := db.Put(key, value)
		assert.Nil(t, err)
	}

	// 正向遍历
	iter := db.NewIterator(DefaultIteratorOptions)
	defer iter.Close()

	count := 0
	for iter.Valid() {
		count++
		iter.Next()
	}

	assert.Equal(t, 1000, count, "应该遍历所有1000条数据")
}

// 测试大量数据的前缀过滤
func TestIterator_LargeDataset_WithPrefix(t *testing.T) {
	db := createTestDB(t)
	defer destroyTestDB(t, db)

	// 插入多组数据
	for i := 0; i < 100; i++ {
		db.Put([]byte(fmt.Sprintf("user:%03d", i)), []byte(fmt.Sprintf("user_%d", i)))
		db.Put([]byte(fmt.Sprintf("product:%03d", i)), []byte(fmt.Sprintf("product_%d", i)))
		db.Put([]byte(fmt.Sprintf("order:%03d", i)), []byte(fmt.Sprintf("order_%d", i)))
	}

	// 前缀过滤
	opts := IteratorOptions{
		Prefix: []byte("user:"),
	}
	iter := db.NewIterator(opts)
	defer iter.Close()

	count := 0
	for iter.Valid() {
		key := iter.Key()
		assert.True(t, len(key) >= 5 && string(key[:5]) == "user:")
		count++
		iter.Next()
	}

	assert.Equal(t, 100, count, "应该只遍历100条 user: 前缀的数据")
}

// ==================== 复杂场景测试 ====================

// 测试多个迭代器并发工作（快照隔离）
func TestIterator_MultipleIterators(t *testing.T) {
	db := createTestDB(t)
	defer destroyTestDB(t, db)

	// 插入初始数据
	db.Put([]byte("aaa"), []byte("value_a"))
	db.Put([]byte("bbb"), []byte("value_b"))
	db.Put([]byte("ccc"), []byte("value_c"))

	// 创建第一个迭代器
	iter1 := db.NewIterator(DefaultIteratorOptions)
	defer iter1.Close()

	// 添加新数据
	db.Put([]byte("ddd"), []byte("value_d"))

	// 创建第二个迭代器
	iter2 := db.NewIterator(DefaultIteratorOptions)
	defer iter2.Close()

	// iter1 应该只看到3条数据（快照）
	count1 := 0
	for iter1.Valid() {
		count1++
		iter1.Next()
	}
	assert.Equal(t, 3, count1, "iter1 应该只看到创建时的3条数据")

	// iter2 应该看到4条数据
	count2 := 0
	for iter2.Valid() {
		count2++
		iter2.Next()
	}
	assert.Equal(t, 4, count2, "iter2 应该看到4条数据")
}

// 测试 ReWind 与前缀过滤结合
func TestIterator_ReWind_WithPrefix(t *testing.T) {
	db := createTestDB(t)
	defer destroyTestDB(t, db)

	// 插入数据
	db.Put([]byte("user:001"), []byte("Alice"))
	db.Put([]byte("user:002"), []byte("Bob"))
	db.Put([]byte("product:001"), []byte("Book"))

	// 前缀迭代
	opts := IteratorOptions{
		Prefix: []byte("user:"),
	}
	iter := db.NewIterator(opts)
	defer iter.Close()

	// 第一次遍历
	count := 0
	for iter.Valid() {
		count++
		iter.Next()
	}
	assert.Equal(t, 2, count)

	// ReWind 后再次遍历
	iter.ReWind()
	count = 0
	for iter.Valid() {
		count++
		iter.Next()
	}
	assert.Equal(t, 2, count, "ReWind 后应该能再次遍历前缀数据")
}

// 测试 Close 后的行为
func TestIterator_Close(t *testing.T) {
	db := createTestDB(t)
	defer destroyTestDB(t, db)

	db.Put([]byte("aaa"), []byte("value_a"))

	iter := db.NewIterator(DefaultIteratorOptions)
	assert.True(t, iter.Valid())

	// 关闭迭代器
	iter.Close()

	// Close 不应该 panic
	// 注意：实际使用中，Close 后不应该再访问迭代器
}
