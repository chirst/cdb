package db

import (
	"strconv"
	"testing"

	"github.com/chirst/cdb/catalog"
	"github.com/chirst/cdb/vm"
)

// TODO test insert on id primary key only table

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
	schemaTypeExpectations := []catalog.CdbType{
		{ID: catalog.CTInt},
		{ID: catalog.CTStr},
		{ID: catalog.CTStr},
		{ID: catalog.CTStr},
		{ID: catalog.CTInt},
		{ID: catalog.CTStr},
	}
	for i, ste := range schemaTypeExpectations {
		if rt := schemaRes.ResultTypes[i]; rt != ste {
			t.Fatalf("expected type %d got %d", ste, rt)
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
	selectTypeExpectations := []catalog.CdbType{
		{ID: catalog.CTInt},
		{ID: catalog.CTStr},
		{ID: catalog.CTStr},
		{ID: catalog.CTInt},
	}
	for i, ste := range selectTypeExpectations {
		if rt := selectPersonRes.ResultTypes[i]; rt != ste {
			t.Fatalf("expected type %d got %d", ste, rt)
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
	wantCountType := catalog.CdbType{ID: catalog.CTInt}
	gotCountType := selectCountRes.ResultTypes[0]
	if wantCountType != gotCountType {
		t.Fatalf("got type %d want type %d", gotCountType, wantCountType)
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
	gotType := res.ResultTypes[0]
	wantType := catalog.CdbType{ID: catalog.CTInt}
	if gotType != wantType {
		t.Fatalf("want type %d but got %d type", wantType, gotType)
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

func TestSelectHeaders(t *testing.T) {
	db := mustCreateDB(t)
	mustExecute(t, db, "CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER)")
	mustExecute(t, db, "INSERT INTO test (val) VALUES (1)")
	res := mustExecute(t, db, "SELECT *, id AS foo FROM test")
	if rowCount := len(res.ResultRows); rowCount != 1 {
		t.Fatalf("want 1 row but got %d", rowCount)
	}
	if got := res.ResultHeader[0]; got != "id" {
		t.Fatalf("want id but got %s", got)
	}
	if got := res.ResultHeader[1]; got != "val" {
		t.Fatalf("want val but got %s", got)
	}
	if got := res.ResultHeader[2]; got != "foo" {
		t.Fatalf("want foo but got %s", got)
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

func TestResultColumnExprs(t *testing.T) {
	type rcCase struct {
		statement string
		want      string
	}
	rcCases := []rcCase{
		{
			statement: "SELECT val + 2 FROM test",
			want:      "24",
		},
		{
			statement: "SELECT 3 + val + 2 FROM test",
			want:      "27",
		},
		{
			statement: "SELECT val > 2 FROM test",
			want:      "1",
		},
		{
			statement: "SELECT 3 > val > 2 FROM test",
			want:      "0",
		},
		{
			statement: "SELECT val < 2 FROM test",
			want:      "0",
		},
		{
			statement: "SELECT 3 < val < 2 FROM test",
			want:      "1",
		},
		{
			statement: "SELECT val = 2 FROM test",
			want:      "0",
		},
		{
			statement: "SELECT 3 = val = 2 FROM test",
			want:      "0",
		},
		{
			statement: "SELECT val - 2 FROM test",
			want:      "20",
		},
		{
			statement: "SELECT 3 - val - 2 FROM test",
			want:      "-21",
		},
		{
			statement: "SELECT val ^ 2 FROM test",
			want:      "484",
		},
		{
			statement: "SELECT val ^ 2 ^ 2 FROM test",
			want:      "234256",
		},
		{
			statement: "SELECT val * 2 FROM test",
			want:      "44",
		},
		{
			statement: "SELECT 2 * val * 2 FROM test",
			want:      "88",
		},
		{
			statement: "SELECT val / 2 FROM test",
			want:      "11",
		},
		{
			statement: "SELECT 44 / val / 2 FROM test",
			want:      "1",
		},
	}
	db := mustCreateDB(t)
	mustExecute(t, db, "CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER)")
	mustExecute(t, db, "INSERT INTO test (val) VALUES (22)")
	for _, rcc := range rcCases {
		t.Run(rcc.statement, func(t *testing.T) {
			res := mustExecute(t, db, rcc.statement)
			expectedRowCount := 1
			if rowCount := len(res.ResultRows); rowCount != expectedRowCount {
				t.Fatalf("want %d row but got %d", expectedRowCount, rowCount)
			}
			got := *res.ResultRows[0][0]
			if got != rcc.want {
				t.Fatalf("want %s but got %s", rcc.want, got)
			}
		})
	}
}

func TestPredicateExprs(t *testing.T) {
	type pCase struct {
		statement        string
		expectedRowCount int
	}
	pCases := []pCase{
		{
			statement:        "SELECT 1 FROM test WHERE val + 1",
			expectedRowCount: 1,
		},
		{
			statement:        "SELECT 1 FROM test WHERE 3 + val + 1",
			expectedRowCount: 1,
		},
		{
			statement:        "SELECT 1 FROM test WHERE val / 2",
			expectedRowCount: 1,
		},
		{
			statement:        "SELECT 1 FROM test WHERE 44 / val / 2",
			expectedRowCount: 1,
		},
		{
			statement:        "SELECT 1 FROM test WHERE val * 2",
			expectedRowCount: 1,
		},
		{
			statement:        "SELECT 1 FROM test WHERE 2 * val * 2",
			expectedRowCount: 1,
		},
		{
			statement:        "SELECT 1 FROM test WHERE val ^ 2",
			expectedRowCount: 1,
		},
		{
			statement:        "SELECT 1 FROM test WHERE val ^ 2 ^ 2",
			expectedRowCount: 1,
		},
		{
			statement:        "SELECT 1 FROM test WHERE val - 2",
			expectedRowCount: 1,
		},
		{
			statement:        "SELECT 1 FROM test WHERE 2 - val - 2",
			expectedRowCount: 1,
		},
		{
			statement:        "SELECT 1 FROM test WHERE val > 2",
			expectedRowCount: 1,
		},
		{
			statement:        "SELECT 1 FROM test WHERE 2 > val > 2",
			expectedRowCount: 0,
		},
		{
			statement:        "SELECT 1 FROM test WHERE val < 2",
			expectedRowCount: 0,
		},
		{
			statement:        "SELECT 1 FROM test WHERE 2 < val < 2",
			expectedRowCount: 1,
		},
		{
			statement:        "SELECT 1 FROM test WHERE val = 2",
			expectedRowCount: 0,
		},
		{
			statement:        "SELECT 1 FROM test WHERE 3 = val = 2",
			expectedRowCount: 0,
		},
	}
	db := mustCreateDB(t)
	mustExecute(t, db, "CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER)")
	mustExecute(t, db, "INSERT INTO test (val) VALUES (22)")
	for _, pc := range pCases {
		t.Run(pc.statement, func(t *testing.T) {
			res := mustExecute(t, db, pc.statement)
			if rowCount := len(res.ResultRows); rowCount != pc.expectedRowCount {
				t.Fatalf("want %d row but got %d", pc.expectedRowCount, rowCount)
			}
		})
	}
}

func TestUpdateStatement(t *testing.T) {
	db := mustCreateDB(t)
	mustExecute(t, db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);")
	mustExecute(t, db, "INSERT INTO foo (a, b) VALUES (1,2), (3,4), (5,6);")
	mustExecute(t, db, "UPDATE foo SET b = 1;")
	res := mustExecute(t, db, "SELECT b FROM foo WHERE b = 1;")
	if len(res.ResultRows) != 3 {
		t.Fatalf("expected all 3 rows to be 1")
	}
}

func TestDeleteAll(t *testing.T) {
	db := mustCreateDB(t)
	mustExecute(t, db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, a INTEGER);")
	mustExecute(t, db, "INSERT INTO foo (a) VALUES (1), (2), (3);")
	mustExecute(t, db, "DELETE FROM foo;")
	res := mustExecute(t, db, "SELECT * FROM foo;")
	if lrr := len(res.ResultRows); lrr != 0 {
		t.Fatalf("expected no rows but got %d", lrr)
	}
}

func TestDeleteStatementWithWhere(t *testing.T) {
	db := mustCreateDB(t)
	mustExecute(t, db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, a INTEGER);")
	mustExecute(t, db, "INSERT INTO foo (a) VALUES (11), (12), (13);")
	mustExecute(t, db, "DELETE FROM foo WHERE a = 12;")
	res := mustExecute(t, db, "SELECT * FROM foo;")
	expectedRows := 2
	if lrr := len(res.ResultRows); lrr != expectedRows {
		t.Fatalf("expected %d rows but got %d", expectedRows, lrr)
	}
	want1 := "11"
	if got1 := *res.ResultRows[0][1]; got1 != want1 {
		t.Fatalf("expected %s but got %s", want1, got1)
	}
	want2 := "13"
	if got2 := *res.ResultRows[1][1]; got2 != want2 {
		t.Fatalf("expected %s but got %s", want2, got2)
	}
}
