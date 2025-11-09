package pager

import (
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
)

func TestMultipleExclusive(t *testing.T) {
	fl, err := os.CreateTemp("", "*.db")
	if err != nil {
		t.Fatalf("error opening db file: %s", err)
	}
	defer fl.Close()
	l := newPlatformLock(fl.Fd())
	didErrShared := false
	didErrLocking := false
	sharedCount := 0
	wg := sync.WaitGroup{}
	criticalCount := 2

	wg.Add(criticalCount)
	for range criticalCount {
		go func() {
			err := l.Lock()
			if err != nil {
				didErrLocking = true
			}
			sharedCount += 1
			if sharedCount > 1 {
				didErrShared = true
			}
			time.Sleep(time.Second * 3)
			if sharedCount > 1 {
				didErrShared = true
			}
			sharedCount -= 1
			l.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()

	if didErrShared {
		t.Fatal("two or more in critical section")
	}
	if didErrLocking {
		t.Fatal("a lock failed")
	}
}

// TestCrossProcess uses TestCrossProcessSub to test two "instances" or
// processes of the database maintain exclusive access to the database file.
func TestCrossProcess(t *testing.T) {
	cmd1 := exec.Command("go", "test", "-run", "^TestCrossProcessSub$", "github.com/chirst/cdb/pager")
	cmd2 := exec.Command("go", "test", "-run", "^TestCrossProcessSub$", "github.com/chirst/cdb/pager")
	start := time.Now()
	err := cmd1.Start()
	if err != nil {
		t.Fatal(err)
	}
	err = cmd2.Start()
	if err != nil {
		t.Fatal(err)
	}
	err = cmd2.Wait()
	if err != nil {
		t.Fatal(err)
	}
	err = cmd1.Wait()
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if elapsed < (time.Second * 6) {
		t.Fatalf("expected at least 6 seconds but got %f seconds", elapsed.Seconds())
	}
}

// TestCrossProcessSub is a hacky way to test the file locking works for two
// processes of the database. This test alone is useless, it is just a simple
// way to run a function in a new process. See TestCrossProcess for the actual
// test. TODO perhaps there is a better way to achieve this with something like
// fork?
func TestCrossProcessSub(t *testing.T) {
	fl, err := os.OpenFile("filelock_test.db", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("error opening db file: %s", err)
	}
	defer fl.Close()
	l := newPlatformLock(fl.Fd())

	if err = l.Lock(); err != nil {
		t.Fatalf("lock failed unexpectedly with err %s", err)
	}
	time.Sleep(time.Second * 3)
	l.Unlock()
}
