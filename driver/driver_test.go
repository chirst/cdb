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

	t.Run("TestConstantQueryWithParams", func(t *testing.T) {
		rows, err := db.Query("SELECT ? AS id, ? AS name WHERE 2 = ?", 1, "one", 2)
		if err != nil {
			t.Fatalf("query err %s", err)
		}
		fs := toFoos(rows)
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
	})
}

func TestInsertWithParams(t *testing.T) {
	type paramCase struct {
		caseName     string
		sql          string
		args         []any
		expectedId   int
		expectedName string
	}

	cases := []paramCase{
		{
			caseName:     "SQLInjectionParam",
			sql:          "INSERT INTO foo (name) VALUES (?)",
			args:         []any{"'w'); DROP TABLE foo;--"},
			expectedId:   1,
			expectedName: "'w'); DROP TABLE foo;--",
		},
		{
			caseName:     "PrimaryKey",
			sql:          "INSERT INTO foo (id, name) VALUES (?, 'asdf')",
			args:         []any{3},
			expectedId:   3,
			expectedName: "asdf",
		},
		{
			caseName:     "WithTwoParams",
			sql:          "INSERT INTO foo (id, name) VALUES (?, ?)",
			args:         []any{4, "baz"},
			expectedId:   4,
			expectedName: "baz",
		},
		{
			caseName:     "WithTwoParamsReverseOrder",
			sql:          "INSERT INTO foo (name, id) VALUES (?, ?)",
			args:         []any{"baz", 4},
			expectedId:   4,
			expectedName: "baz",
		},
	}

	for _, c := range cases {
		t.Run(c.caseName, func(t *testing.T) {
			db := mustOpenSqlDb(t)
			mustExecute(t, db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")
			_, err := db.Exec(c.sql, c.args...)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := db.Query("SELECT * FROM foo")
			if err != nil {
				t.Fatalf("query err %s", err)
			}
			expectCount := 1
			fs := toFoos(rows)
			if d := len(fs); d != expectCount {
				t.Fatalf("expected %d got %d", expectCount, d)
			}
			if fs[0].id != c.expectedId {
				t.Fatalf("expected %d got %d", c.expectedId, fs[0].id)
			}
			if fs[0].name != c.expectedName {
				t.Fatalf("expected %s got %s", c.expectedName, fs[0].name)
			}
		})
	}
}
