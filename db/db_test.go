package db

import (
	"strconv"
	"testing"

	"github.com/chirst/cdb/vm"
)

func mustCreateDB(t *testing.T) *DB {
	db, err := New(true, "")
	if err != nil {
		t.Fatalf("err creating db: %s", err)
	}
	return db
}

func mustExecute(t *testing.T, db *DB, sql string) vm.ExecuteResult {
	statements := db.Tokenize(sql)
	res := db.Execute(statements[0], []any{})
	if res.Err != nil {
		t.Fatalf("%s executing sql: %s", res.Err, sql)
	}
	return res
}

func TestExecute(t *testing.T) {
	db := mustCreateDB(t)
	mustExecute(t, db, "CREATE TABLE person (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, age INTEGER)")
	schemaRes := mustExecute(t, db, "SELECT * FROM cdb_schema")
	schemaSelectExpectations := []string{
		"1",
		"table",
		"person",
		"person",
		"2",
		"{\"columns\":[{\"name\":\"id\",\"type\":\"INTEGER\",\"primaryKey\":true},{\"name\":\"first_name\",\"type\":\"TEXT\",\"primaryKey\":false},{\"name\":\"last_name\",\"type\":\"TEXT\",\"primaryKey\":false},{\"name\":\"age\",\"type\":\"INTEGER\",\"primaryKey\":false}]}",
	}
	for i, s := range schemaSelectExpectations {
		if c := *schemaRes.ResultRows[0][i]; c != s {
			t.Fatalf("expected %s got %s", s, c)
		}
	}
	mustExecute(t, db, "INSERT INTO person (first_name, last_name, age) VALUES ('John', 'Smith', 50)")
	selectPersonRes := mustExecute(t, db, "SELECT * FROM person")
	selectPersonExpectations := []string{
		"1",
		"John",
		"Smith",
		"50",
	}
	for i, s := range selectPersonExpectations {
		if c := *selectPersonRes.ResultRows[0][i]; c != s {
			t.Fatalf("expected %s got %s", s, c)
		}
	}
}

func TestBulkInsert(t *testing.T) {
	db := mustCreateDB(t)
	mustExecute(t, db, "CREATE TABLE test (id INTEGER PRIMARY KEY, junk TEXT)")
	expectedTotal := 100_000
	for i := 0; i < expectedTotal; i += 1 {
		mustExecute(t, db, "INSERT INTO test (junk) VALUES ('asdf')")
	}
	selectRes := mustExecute(t, db, "SELECT * FROM test")
	if gotT := len(selectRes.ResultRows); expectedTotal != gotT {
		t.Fatalf("expected %d got %d", expectedTotal, gotT)
	}
	for i, r := range selectRes.ResultRows {
		left, err := strconv.Atoi(*r[0])
		if err != nil {
			t.Fatal(err)
		}
		if left != i+1 {
			t.Fatalf("expected %d got %d", i+1, left)
		}
	}
	selectCountRes := mustExecute(t, db, "SELECT COUNT(*) FROM test")
	gotCS := selectCountRes.ResultRows[0][0]
	gotC, err := strconv.Atoi(*gotCS)
	if err != nil {
		t.Fatal(err)
	}
	if expectedTotal != gotC {
		t.Fatalf("got count %d want %d", gotC, expectedTotal)
	}
}

func TestPrimaryKeyUniqueConstraintViolation(t *testing.T) {
	db := mustCreateDB(t)
	mustExecute(t, db, "CREATE TABLE test (id INTEGER PRIMARY KEY, junk TEXT)")
	mustExecute(t, db, "INSERT INTO test (id, junk) VALUES (1, 'asdf')")
	statements := db.Tokenize("INSERT INTO test (id, junk) VALUES (1, 'asdf')")
	dupePKResponse := db.Execute(statements[0], []any{})
	if dupePKResponse.Err.Error() != "pk unique constraint violated" {
		t.Fatalf("expected unique constraint error to be raised but got %s", dupePKResponse.Err)
	}
}

func TestOperators(t *testing.T) {
	db := mustCreateDB(t)
	res := mustExecute(t, db, "SELECT 1+2-3*4+5^7-8*9/2")
	got := *res.ResultRows[0][0]
	want := "78080"
	if got != want {
		t.Fatalf("want %s but got %s", want, got)
	}
}

func TestAddColumns(t *testing.T) {
	db := mustCreateDB(t)
	mustExecute(t, db, "CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER)")
	mustExecute(t, db, "INSERT INTO test (id, val) VALUES (78, 112)")
	res := mustExecute(t, db, "SELECT id + val FROM test")
	got := *res.ResultRows[0][0]
	want := "190"
	if got != want {
		t.Fatalf("want %s but got %s", want, got)
	}
}

func TestSelectWithWhere(t *testing.T) {
	db := mustCreateDB(t)
	mustExecute(t, db, "CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER)")
	mustExecute(t, db, "INSERT INTO test (id, val) VALUES (3, 929), (1, 444), (2, 438)")
	res := mustExecute(t, db, "SELECT * FROM test WHERE val = 444")
	if rowCount := len(res.ResultRows); rowCount != 1 {
		t.Fatalf("want 1 row but got %d", rowCount)
	}
	got := *res.ResultRows[0][0]
	want := "1"
	if got != want {
		t.Fatalf("want %s but got %s", want, got)
	}
}

func TestSelectRangeWithWhere(t *testing.T) {
	db := mustCreateDB(t)
	mustExecute(t, db, "CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER)")
	mustExecute(t, db, "INSERT INTO test (id, val) VALUES (3, 333), (1, 334), (2, 335)")
	res := mustExecute(t, db, "SELECT * FROM test WHERE val > 334")
	if rowCount := len(res.ResultRows); rowCount != 1 {
		t.Fatalf("want 1 row but got %d", rowCount)
	}
	got := *res.ResultRows[0][0]
	want := "2"
	if got != want {
		t.Fatalf("want %s but got %s", want, got)
	}
}
