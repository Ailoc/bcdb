package bcdb

import (
	"errors"
)

var (
	ErrKeyisEmpty       = errors.New("key is empty")
	ErrIndexUpdateFiled = errors.New("index update filed")
	ErrKeyNotFound      = errors.New("key not found")
	ErrDataFileNotFound = errors.New("data file not found")

	ErrDBDirisEmpty       = errors.New("db dir is empty")
	ErrMaxFileSizeInvalid = errors.New("max file size is invalid")
	ErrDataFileCorrupted  = errors.New("data file corrupted")

	ErrInvaildCRC = errors.New("invalid crc value")
)
