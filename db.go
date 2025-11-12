package bcdb

import (
	"bcdb/data"
	"bcdb/index"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// 面向用户的操作接口
type DB struct {
	options    Options
	fidList    []int // 文件ID列表，加载索引时使用
	mu         *sync.RWMutex
	activeFile *data.DataFile
	olderFiles map[uint32]*data.DataFile
	index      index.Indexer // 内存索引结构，例如BTree
	closed     bool
}

func Open(options Options) (*DB, error) {
	// 校验配置项
	if err := checkOptions(options); err != nil {
		return nil, err
	}
	// 创建数据目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}
	// 加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 加载内存索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}
	return db, nil
}

func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyisEmpty
	}
	if db.closed {
		return ErrDBClosed
	}
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	recordPos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}
	// 更新内存索引
	if ok := db.index.Put(key, recordPos); !ok {
		return ErrIndexUpdateFiled
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	// 加锁
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {

		return nil, ErrDBClosed
	}
	// 判断key是否合法
	if len(key) == 0 {
		return nil, ErrKeyisEmpty
	}
	// 查找key
	recordPos := db.index.Get(key)
	if recordPos == nil {
		return nil, ErrKeyNotFound
	}

	return db.getValueByPos(recordPos)
}

func (db *DB) getValueByPos(recordPos *data.LogRecordPos) ([]byte, error) {
	// 根据recordPos找到文件以及数据位置
	var dataFile *data.DataFile

	if recordPos.Fid == db.activeFile.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[recordPos.Fid]
	}

	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据数据偏移读取数据
	logRecord, _, err := dataFile.ReadLogRecord(recordPos.Offset)
	if err != nil {
		return nil, err
	}
	// 判断logRecord是否已被删除
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil

}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyNotFound
	}
	if db.closed {
		return ErrDBClosed
	}
	if pos := db.index.Get(key); pos == nil {
		return nil
	}
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}
	// 从内存索引中删除数据
	if ok := db.index.Delete(key); !ok {
		return ErrIndexUpdateFiled
	}
	return nil
}

// 获取数据库中所有的key
func (db *DB) ListKeys() [][]byte {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil
	}
	iter := db.index.Iterator(db.options.Reverse)
	keys := make([][]byte, db.index.Size())
	var idx int
	for iter.ReWind(); iter.Valid(); iter.Next() {
		keys[idx] = iter.Key()
		idx++
	}
	return keys
}

// 获取所有数据，执行特定的操作
func (db *DB) Fold(fn func(key, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return ErrDBClosed
	}
	iter := db.index.Iterator(db.options.Reverse)
	for iter.ReWind(); iter.Valid(); iter.Next() {
		value, err := db.getValueByPos(iter.Value())
		if err != nil {
			return err
		}
		if !fn(iter.Key(), value) {
			break
		}
	}
	return nil
}

func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.activeFile.Close(); err != nil {
		return err
	}

	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	db.activeFile = nil
	db.activeFile = nil
	db.closed = true
	return nil
}

// 持久化数据文件
func (db *DB) Sync() error {
	if db.closed {
		return ErrDBClosed
	}
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.activeFile.Sync()
}

func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.activeFile == nil {
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	// 对记录进行编码
	encodedRecord, recordLen := data.EncodeLogRecord(logRecord)
	// 如果写入文件达到了活跃文件的阈值，关闭当前活跃文件，构造新的活跃文件
	if db.activeFile.WriteOffset+recordLen > db.options.MaxFileSize {
		// 持久化当前活跃文件数据到磁盘当中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 将当前文件放入旧的数据文件中
		db.olderFiles[db.activeFile.Fid] = db.activeFile
		// 构造新的数据文件
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}
	// 写入数据
	writeOffset := db.activeFile.WriteOffset
	if err := db.activeFile.Write(encodedRecord); err != nil {
		return nil, err
	}
	// 如果开启了同步写入，持久化数据到磁盘
	if db.options.SyncWrite {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}
	// 返回索引信息
	recordPos := &data.LogRecordPos{
		Fid:    db.activeFile.Fid,
		Offset: writeOffset,
	}
	return recordPos, nil
}

func (db *DB) setActiveFile() error {
	var initialFileID uint32 = 0
	if db.activeFile != nil {
		initialFileID = db.activeFile.Fid + 1
	}
	// 构造新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileID)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return ErrDBDirisEmpty
	}
	if options.MaxFileSize <= 0 {
		return ErrMaxFileSizeInvalid
	}
	return nil
}

func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fidList []int
	// 遍历所有以.data结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileSuffix) {
			fidStr := strings.Split(entry.Name(), ".")[0]
			fid, err := strconv.Atoi(fidStr)
			if err != nil {
				// 数据文件损坏
				return ErrDataFileCorrupted
			}
			fidList = append(fidList, fid)
		}
	}
	// 对数据文件进行排序
	sort.Ints(fidList)
	db.fidList = fidList
	for i, fid := range fidList {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		if i == len(fidList)-1 {
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(fid)] = dataFile
		}
	}

	return nil
}

// 从数据文件中加载索引
func (db *DB) loadIndexFromDataFiles() error {
	if len(db.fidList) == 0 {
		return nil
	}
	//取出所有文件中的数据
	for i, fid := range db.fidList {
		var fileID = uint32(fid)
		var dataFile *data.DataFile
		if fileID == db.activeFile.Fid {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileID]
		}

		var offset int64
		// 持续读取文件中的数据
		for {
			logRecord, recordSize, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			logRecordPos := &data.LogRecordPos{Fid: fileID, Offset: offset}

			if logRecord.Type == data.LogRecordDeleted {
				db.index.Delete(logRecord.Key)
			} else {
				db.index.Put(logRecord.Key, logRecordPos)
			}

			offset += recordSize
		}

		// 更新当前活跃文件的写入Offset
		if i == len(db.fidList)-1 {
			db.activeFile.WriteOffset = offset
		}
	}
	return nil
}
