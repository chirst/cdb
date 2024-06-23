package main

import (
	"testing"
)

func TestExecute(t *testing.T) {
	db, err := newDb(true)
	if err != nil {
		t.Fatal(err)
	}
	createSql := "CREATE TABLE person (id INTEGER, first_name TEXT, last_name TEXT, age INTEGER)"
	createRes := db.execute(createSql)
	if createRes.err != nil {
		t.Fatal(err.Error())
	}
	selectSchemaSql := "SELECT * FROM cdb_schema"
	schemaRes := db.execute(selectSchemaSql)
	if schemaRes.err != nil {
		t.Fatal(schemaRes.err.Error())
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
		if c := *schemaRes.resultRows[1][i]; c != s {
			t.Fatalf("expected %s got %s", s, c)
		}
	}
	insertSql := "INSERT INTO person (first_name, last_name, age) VALUES ('John', 'Smith', 50)"
	insertRes := db.execute(insertSql)
	if insertRes.err != nil {
		t.Fatal(err.Error())
	}
	selectPersonSql := "SELECT * FROM person"
	selectPersonRes := db.execute(selectPersonSql)
	if selectPersonRes.err != nil {
		t.Fatal(err.Error())
	}
	selectPersonExpectations := []string{
		"1",
		"John",
		"Smith",
		"50",
	}
	for i, s := range selectPersonExpectations {
		if c := *selectPersonRes.resultRows[1][i]; c != s {
			t.Fatalf("expected %s got %s", s, c)
		}
	}
}
