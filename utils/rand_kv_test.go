package utils

import "testing"

func TestGetRandKV(t *testing.T) {
	for i := 0; i < 10; i++ {
		t.Log(string(GetTestKet(i)))
	}
}

func TestGetValue(t *testing.T) {
	for i := 0; i < 10; i++ {
		t.Log(string(GetTestValue(10)))
	}
}
