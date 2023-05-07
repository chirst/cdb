package main

import (
	"fmt"
	"log"
)

func main() {
	kv, err := NewKv("db.db")
	if err != nil {
		log.Fatal(err)
	}
	kv.Set([]byte{1}, []byte{'c', 'a', 'r', 'l'})
	res, found := kv.Get([]byte{1})
	fmt.Print(res, found)
}
