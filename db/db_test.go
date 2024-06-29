package db

import (
	"strconv"
	"testing"
)

func TestExecute(t *testing.T) {
	t.Run("create, select schema, insert, select", func(t *testing.T) {
		db, err := New(true)
		if err != nil {
			t.Fatal(err.Error())
		}
		createSql := "CREATE TABLE person (id INTEGER, first_name TEXT, last_name TEXT, age INTEGER)"
		createRes := db.Execute(createSql)
		if createRes.Err != nil {
			t.Fatal(createRes.Err.Error())
		}
		selectSchemaSql := "SELECT * FROM cdb_schema"
		schemaRes := db.Execute(selectSchemaSql)
		if schemaRes.Err != nil {
			t.Fatal(schemaRes.Err.Error())
		}
		schemaSelectExpectations := []string{
			"1",
			"table",
			"person",
			"person",
			"2",
			"{\"columns\":[{\"name\":\"id\",\"type\":\"INTEGER\"},{\"name\":\"first_name\",\"type\":\"TEXT\"},{\"name\":\"last_name\",\"type\":\"TEXT\"},{\"name\":\"age\",\"type\":\"INTEGER\"}]}",
		}
		for i, s := range schemaSelectExpectations {
			if c := *schemaRes.ResultRows[0][i]; c != s {
				t.Fatalf("expected %s got %s", s, c)
			}
		}
		insertSql := "INSERT INTO person (first_name, last_name, age) VALUES ('John', 'Smith', 50)"
		insertRes := db.Execute(insertSql)
		if insertRes.Err != nil {
			t.Fatal(insertRes.Err.Error())
		}
		selectPersonSql := "SELECT * FROM person"
		selectPersonRes := db.Execute(selectPersonSql)
		if selectPersonRes.Err != nil {
			t.Fatal(selectPersonRes.Err.Error())
		}
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
	})

	t.Run("bulk insert", func(t *testing.T) {
		db, err := New(true)
		if err != nil {
			t.Fatal(err.Error())
		}
		createSql := "CREATE TABLE test (id INTEGER, junk TEXT)"
		createRes := db.Execute(createSql)
		if createRes.Err != nil {
			t.Fatal(createRes.Err.Error())
		}
		expectedTotal := 100_000
		for i := 0; i < expectedTotal; i += 1 {
			insertSql := "INSERT INTO test (junk) VALUES ('asdf')"
			insertRes := db.Execute(insertSql)
			if insertRes.Err != nil {
				t.Fatal(insertRes.Err.Error())
			}
		}
		selectSql := "SELECT * FROM test"
		selectRes := db.Execute(selectSql)
		if selectRes.Err != nil {
			t.Fatal(selectRes.Err.Error())
		}
		if gotT := len(selectRes.ResultRows); expectedTotal != gotT {
			t.Fatalf("expected %d got %d", expectedTotal, gotT)
		}
		selectCountSql := "SELECT COUNT(*) FROM test"
		selectCountRes := db.Execute(selectCountSql)
		if selectCountRes.Err != nil {
			t.Fatal(selectCountRes.Err.Error())
		}
		gotCS := selectCountRes.ResultRows[0][0]
		gotC, err := strconv.Atoi(*gotCS)
		if err != nil {
			t.Fatal(err.Error())
		}
		if expectedTotal != gotC {
			t.Fatalf("got count %d want %d", gotC, expectedTotal)
		}
	})
}
