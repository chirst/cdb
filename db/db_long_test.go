package db

// This file contains tests that take a long time to run due to the tests
// testing the ability to operate on millions of records.

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestInsertAndSelectMillions(t *testing.T) {
	if os.Getenv("LONG_TEST") == "" {
		t.Skip("skipped long test")
	}
	err := os.Remove("millions.db")
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal("could not remove existing millions.db database file")
	}
	// Create database with a file for this test since it cannot be all in
	// memory.
	db, err := New(false, "millions")
	if err != nil {
		t.Fatalf("err creating db: %s", err)
	}
	mustExecute(t, db, "CREATE TABLE test (id INTEGER PRIMARY KEY, junk TEXT)")
	inserts := 100_000
	recordsPerInsert := 100
	values := []string{}
	for range recordsPerInsert {
		values = append(values, "('asdf')")
	}
	insertStatement := "INSERT INTO test (junk) VALUES " + strings.Join(values, ",")
	t.Log("inserting millions")
	for i := 0; i < inserts; i += 1 {
		mustExecute(t, db, insertStatement)
	}
	t.Log("inserted millions")
	t.Log("selecting from millions")
	selectRes := mustExecute(t, db, "SELECT * FROM test WHERE id > 9999995")
	t.Log("selected from millions")
	selectExpects := &[]string{
		"9999996",
		"9999997",
		"9999998",
		"9999999",
		"10000000",
	}
	for i, se := range *selectExpects {
		if got := *selectRes.ResultRows[i][0]; got != se {
			t.Fatalf("select failed got: %s want: %s", got, se)
		}
	}
	t.Log("counting from millions")
	selectCountRes := mustExecute(t, db, "SELECT COUNT(*) FROM test")
	t.Log("counted millions")
	gotCS := selectCountRes.ResultRows[0][0]
	gotC, err := strconv.Atoi(*gotCS)
	if err != nil {
		t.Fatal(err)
	}
	if wantC := inserts * recordsPerInsert; wantC != gotC {
		t.Fatalf("got count %d want %d", gotC, wantC)
	}
	err = os.Remove("millions.db")
	if err != nil {
		t.Fatal("could not cleanup millions.db database file")
	}
}
