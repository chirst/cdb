package db

import (
	"errors"
	"os"
	"testing"
)

// This test is mostly to assert the platform is capable of running the code
// that is abstracted by the in memory storage. In theory, I could write these
// tests around the file storage, but I don't want to.
func TestReadWriteFile(t *testing.T) {
	err := os.Remove("file_test.db")
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal("could not remove existing file_test.db file")
	}
	db, err := New(false, "file_test")
	if err != nil {
		t.Fatalf("err creating db: %s", err)
	}
	mustExecute(t, db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT);")
	mustExecute(t, db, "INSERT INTO foo (name) VALUES ('gud dude');")
	mustExecute(t, db, "SELECT * FROM foo;")
	if err := os.Remove("file_test.db"); err != nil {
		t.Fatal("failed to clean up file_test.db file")
	}
}
