package main

import (
	"flag"
	"log"

	"github.com/chirst/cdb/db"
	"github.com/chirst/cdb/pager"
	"github.com/chirst/cdb/repl"
)

const fFlagHelp = "Specify the database file name"
const mFlagHelp = "Run the database in memory with no persistence"

func main() {
	dbfName := flag.String("f", pager.DefaultDBFileName, fFlagHelp)
	isMemory := flag.Bool("m", false, mFlagHelp)
	flag.Parse()
	db, err := db.New(*isMemory, *dbfName)
	if err != nil {
		log.Fatal(err)
	}
	repl.New(db).Run()
}
