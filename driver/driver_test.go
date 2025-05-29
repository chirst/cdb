package driver_test

import (
	"database/sql"
	"testing"
)

func mustOpenSqlDb(t *testing.T) *sql.DB {
	db, err := sql.Open("cdb", ":memory:")
	if err != nil {
		t.Fatalf("open err %s", err)
	}
	return db
}

func mustExecute(t *testing.T, db *sql.DB, sql string) {
	_, err := db.Exec(sql)
	if err != nil {
		t.Fatalf("failed to exec %s with err %s", sql, err)
	}
}

type foo struct {
	id   int
	name string
}

func toFoos(rows *sql.Rows) []*foo {
	fs := make([]*foo, 0)
	for rows.Next() {
		f := &foo{}
		rows.Scan(&f.id, &f.name)
		fs = append(fs, f)
	}
	return fs
}

func TestSchema1(t *testing.T) {
	db := mustOpenSqlDb(t)
	mustExecute(t, db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")
	mustExecute(t, db, "INSERT INTO foo (name) VALUES ('one')")

	// TODO make params work for statements other than select. Will also test
	// exec.

	t.Run("TestQuery", func(t *testing.T) {
		rows, err := db.Query("SELECT * FROM foo")
		if err != nil {
			t.Fatalf("query err %s", err)
		}
		expectCount := 1
		fs := toFoos(rows)
		if d := len(fs); d != expectCount {
			t.Fatalf("expected %d got %d", expectCount, d)
		}
		if fs[0].name != "one" {
			t.Fatalf("expected one got %s", fs[0].name)
		}
		if fs[0].id != 1 {
			t.Fatalf("expected %d got %d", 1, fs[0].id)
		}
	})

	t.Run("TestQueryWithParam", func(t *testing.T) {
		rows, err := db.Query("SELECT * FROM foo WHERE id = ?", 1)
		if err != nil {
			t.Fatalf("query err %s", err)
		}
		fs := toFoos(rows)
		expectCount := 1
		if d := len(fs); d != expectCount {
			t.Fatalf("expected %d got %d", expectCount, d)
		}
	})

	t.Run("TestQueryWithParams", func(t *testing.T) {
		rows, err := db.Query("SELECT * FROM foo WHERE ? + ? = 3", 2, 1)
		if err != nil {
			t.Fatalf("query err %s", err)
		}
		fs := toFoos(rows)
		expectCount := 1
		if d := len(fs); d != expectCount {
			t.Fatalf("expected %d got %d", expectCount, d)
		}
	})
}
