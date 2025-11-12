package main

import (
	"bcdb"
	"fmt"
)

func main() {
	opts := bcdb.DefaultOptions
	opts.DirPath = "./"

	db, err := bcdb.Open(opts)
	if err != nil {
		panic(err)
	}

	// err = db.Put([]byte("hello"), []byte("world"))
	// if err != nil {
	// 	panic(err)
	// }
	value, err := db.Get([]byte("hello"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(value))

	// err = db.Delete([]byte("hello"))
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Println("key hello is deleted.")
	// value, err = db.Get([]byte("hello"))
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Println(string(value))
}
