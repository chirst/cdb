package db

import (
	"errors"
	"os"
	"os/exec"
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

// Tests the page cache flushes appropriately when two separate processes have
// cached pages and a write occurs in one of them.
func TestDirtyReads(t *testing.T) {
	err := os.Remove("dirty_read_test.db")
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal("could not remove existing dirty_read_test.db file")
	}
	db, err := New(false, "dirty_read_test")
	if err != nil {
		t.Fatalf("err creating db: %s", err)
	}

	// Create a table and select all to get the pages in the cache
	mustExecute(t, db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT);")
	mustExecute(t, db, "INSERT INTO foo (name) VALUES ('gud dude');")
	mustExecute(t, db, "SELECT * FROM foo;")

	// A subprocess writes to the table which means the cache must be
	// invalidated to prevent a dirty read.
	cmd := exec.Command("go", "test", "-run", "^TestDirtyReadsSub$", "github.com/chirst/cdb/db")
	cmd.Env = append(os.Environ(), "TEST_DIRTY_READS_SUB=1")
	cmd.Start()
	err = cmd.Wait()
	if err != nil {
		t.Fatal(err)
	}

	// Check the cache was invalidated by seeing a value was inserted
	result := mustExecute(t, db, "SELECT * FROM foo;")
	expectedRows := 2
	if gotRows := len(result.ResultRows); gotRows != expectedRows {
		t.Fatalf("expected %d rows but got %d", expectedRows, gotRows)
	}

	if err := os.Remove("dirty_read_test.db"); err != nil {
		t.Fatal("failed to clean up dirty_read_test.db file")
	}
}

func TestDirtyReadsSub(t *testing.T) {
	if os.Getenv("TEST_DIRTY_READS_SUB") == "" {
		t.Skip("skipping helper test")
	}
	db, err := New(false, "dirty_read_test")
	if err != nil {
		t.Fatalf("err creating db: %s", err)
	}
	mustExecute(t, db, "INSERT INTO foo (name) VALUES ('gud dude 2');")
}
