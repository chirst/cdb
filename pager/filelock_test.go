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
	cmd1.Env = append(os.Environ(), "TEST_CROSS_PROCESS_SUB=1")
	cmd2 := exec.Command("go", "test", "-run", "^TestCrossProcessSub$", "github.com/chirst/cdb/pager")
	cmd2.Env = append(os.Environ(), "TEST_CROSS_PROCESS_SUB=1")
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
	if err := os.Remove("filelock_test.db"); err != nil {
		t.Fatal("failed to clean up filelock_test.db file")
	}
}

func TestCrossProcessSub(t *testing.T) {
	if os.Getenv("TEST_CROSS_PROCESS_SUB") == "" {
		t.Skip("skipping helper test")
	}
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
