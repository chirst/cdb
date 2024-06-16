package main

import "log"

func main() {
	db, err := newDb()
	if err != nil {
		log.Fatal(err)
	}
	r := newRepl(db)
	r.run()
}
