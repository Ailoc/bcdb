package fio

const DataFilePerm = 0644

type IOManager interface {
	Read([]byte, int64) (int, error) // 从指定位置读取对应的数据
	Write([]byte) (int, error)       // 写入对应的数据到文件中
	Sync() error                     // 同步文件数据到磁盘
	Close() error                    // 关闭文件
	Size() (int64, error)            // 获取文件大小
}

func NewIOManager(path string) (IOManager, error) {
	return NewFileIOManager(path)
}
