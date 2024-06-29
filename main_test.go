package main

import (
	"testing"

	"github.com/chirst/cdb/db"
)

func TestBulk(t *testing.T) {
	d, _ := db.New(true, "")
	r := d.Execute("CREATE TABLE test (id INTEGER, junk TEXT)")
	if r.Err != nil {
		t.Fatal(r.Err.Error())
	}
	for i := 0; i < 1000; i += 1 {
		r = d.Execute("INSERT INTO test (junk) VALUES ('asdf')")
		if r.Err != nil {
			t.Fatal(r.Err.Error())
		}
	}
	r = d.Execute("SELECT * FROM test")
	if r.Err != nil {
		t.Fatal(r.Err.Error())
	}
}
