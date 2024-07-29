package main

import (
	"testing"

	"github.com/chirst/cdb/db"
)

func TestBulk(t *testing.T) {
	d, _ := db.New(true, "")
	r := d.Execute("CREATE TABLE test (id INTEGER PRIMARY KEY, junk TEXT)")
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	for i := 0; i < 1000; i += 1 {
		r = d.Execute("INSERT INTO test (junk) VALUES ('asdf')")
		if r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	r = d.Execute("SELECT * FROM test")
	if r.Err != nil {
		t.Fatal(r.Err)
	}
}
