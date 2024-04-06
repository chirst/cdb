package main

func main() {
	db := newDb()
	r := newRepl(db)
	r.run()
}
