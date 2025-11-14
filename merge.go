package bcdb

import (
	"bcdb/data"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	MergeDirName = "-merge"
	MergeFinKey  = "merge_finished"
)

func (db *DB) Merge() error {
	// 没有数据文件的情况
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	// 如果正在合并，返回错误
	if db.isMerge {
		db.mu.Unlock()
		return ErrMergeInProgress
	}
	db.isMerge = true
	defer func() {
		db.isMerge = false
		db.mu.Unlock()
	}()

	// 持久化当前活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	// 保存到旧文件当中
	db.olderFiles[db.activeFile.Fid] = db.activeFile

	if err := db.setActiveFile(); err != nil {
		db.mu.Unlock()
		return err
	}

	finFid := db.activeFile.Fid

	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}

	db.mu.Unlock()
	// 对需要merge的datafile进行排序
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].Fid < mergeFiles[j].Fid
	})

	mergePath := db.getMergePath()
	// 如果Merge目录存在，删除后重新创建
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	//打开一个新的DB实例
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrite = false

	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}
	// 打开hint文件存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 解析日志记录的Key
			realKey, _ := parseLogRecordKey(logRecord.Key)
			logRecord.Key = realKey

			// 与索引中的数据进行比较
			logRecordPos := db.index.Get(realKey)
			if logRecordPos != nil && logRecordPos.Fid == dataFile.Fid && logRecordPos.Offset == offset {
				// 清除事务标记
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				// 将位置索引写入到hint文件中
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}

			offset += size
		}
	}

	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}

	// 全部merge完成
	mergeFinFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	mergeRecord := &data.LogRecord{
		Key:   []byte(MergeFinKey),
		Value: []byte(strconv.Itoa(int(finFid))),
	}

	encRecord, _ := data.EncodeLogRecord(mergeRecord)
	if err := mergeFinFile.Write(encRecord); err != nil {
		return err
	}

	if err := mergeFinFile.Sync(); err != nil {
		return err
	}

	return nil
}

func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath)) // 获取数据文件目录的父目录
	base := path.Base(db.options.DirPath)           // 获取数据文件目录名称
	return path.Join(dir, base+MergeDirName)
}

func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()
	entryList, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}
	//查看merge完成标识
	var mergeFinished bool = false
	var mergeFileNames []string
	for _, entry := range entryList {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		} else {
			mergeFileNames = append(mergeFileNames, entry.Name())
		}
	}
	if !mergeFinished {
		return nil
	}
	mergeFid, err := db.getRecentMergeFid(mergePath)
	if err != nil {
		return err
	}

	// 删除原目录中的相应数据文件
	var fid uint32 = 0
	for ; fid <= mergeFid; fid++ {
		dataFileName := data.GetDataFileName(db.options.DirPath, fid)
		if _, err := os.Stat(dataFileName); err == nil {
			if err := os.Remove(dataFileName); err != nil {
				return err
			}
		}
	}

	// 将新的数据文件移动到数据目录下
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		dstPath := filepath.Join(db.options.DirPath, fileName)
		if err := os.Rename(srcPath, dstPath); err != nil {
			return err
		}
	}
	return nil
}

// 获取已经完成合并的数据文件Fid
func (db *DB) getRecentMergeFid(dirPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	rec, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	nonMergeFid, err := strconv.Atoi(string(rec.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFid), nil
}

func (db *DB) loadIndexFromHintFile() error {
	hintFileName := filepath.Join(db.getMergePath(), data.HintFileName)
	// 索引文件不存在
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}
	//打开索引文件
	hintFile, err := data.OpenHintFile(hintFileName)
	if err != nil {
		return err
	}
	var offset int64 = 0

	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		pos := data.DecodeLogRecordPos(logRecord.Value)
		if !db.index.Put(logRecord.Key, pos) {
			return ErrIndexUpdateFiled
		}
		offset += size
	}
	return nil
}
