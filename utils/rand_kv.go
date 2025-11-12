package utils

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	randStr = rand.New(rand.NewSource(time.Now().Unix()))
	letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

func GetTestKet(i int) []byte {
	return []byte(fmt.Sprintf("test-Key-%d", i))
}

func GetTestValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[randStr.Intn(len(letters))]
	}
	return []byte("test-value-" + string(b))
}
