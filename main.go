package main

import (
	"log"

	"github.com/chirst/cdb/db"
	"github.com/chirst/cdb/repl"
)

func main() {
	db, err := db.New(false)
	if err != nil {
		log.Fatal(err)
	}
	repl.New(db).Run()
}
