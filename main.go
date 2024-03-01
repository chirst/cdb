package main

import (
	"fmt"
	"log"
)

func main() {
	kv, err := NewKv(false)
	if err != nil {
		log.Fatal(err)
	}
	kv.Set(1, []byte{1}, []byte{'c', 'a', 'r', 'l'})
	res, found, err := kv.Get(1, []byte{1})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(res, found)
}
