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
	res := db.Execute(sql)
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
	dupePKResponse := db.Execute("INSERT INTO test (id, junk) VALUES (1, 'asdf')")
	if dupePKResponse.Err.Error() != "pk unique constraint violated" {
		t.Fatalf("expected unique constraint error to be raised but got %s", dupePKResponse.Err)
	}
}

func TestOperators(t *testing.T) {
	db := mustCreateDB(t)
	res := db.Execute("SELECT 1+2-3*4+5^7-8*9/2")
	if res.Err != nil {
		t.Fatalf("expected no err but got %s", res.Err)
	}
	got := *res.ResultRows[0][0]
	want := "78080"
	if got != want {
		t.Fatalf("want %s but got %s", want, got)
	}
}
