package driver_test

import (
	"database/sql"
	"testing"
)

func TestDriver(t *testing.T) {
	db, err := sql.Open("cdb", ":memory:")
	if err != nil {
		t.Fatalf("open err %s", err.Error())
	}
	_, err = db.Exec("CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("exec err %s", err.Error())
	}
	_, err = db.Exec("INSERT INTO foo (name) VALUES ('one')")
	if err != nil {
		t.Fatalf("exec err %s", err.Error())
	}
	rows, err := db.Query("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("query err %s", err.Error())
	}
	type foo struct {
		id   int
		name string
	}
	fs := make([]*foo, 0)
	for rows.Next() {
		f := &foo{}
		rows.Scan(&f.id, &f.name)
		fs = append(fs, f)
	}
	expectCount := 1
	if d := len(fs); d != expectCount {
		t.Fatalf("expected %d got %d", expectCount, d)
	}
	if fs[0].name != "one" {
		t.Fatalf("expected one got %s", fs[0].name)
	}
	if fs[0].id != 1 {
		t.Fatalf("expected %d got %d", 1, fs[0].id)
	}
}
