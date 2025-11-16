package pager

import (
	"fmt"
	"runtime"
	"sync"
	"syscall"
)

// lock is a RWMutex. When there is no file it is implemented by the memoryLock
// When there is a file it is implemented by the implementation returned from
// newPlatformLock.
type lock interface {
	Lock() error
	Unlock()
	RLock() error
	RUnlock()
}

// memoryLock implements lock and is used when there is no file to lock.
type memoryLock struct {
	l *sync.RWMutex
}

func (m *memoryLock) Lock() error {
	m.l.Lock()
	return nil
}

func (m *memoryLock) Unlock() {
	m.l.Unlock()
}

func (m *memoryLock) RLock() error {
	m.l.RLock()
	return nil
}

func (m *memoryLock) RUnlock() {
	m.l.RUnlock()
}

// newPlatformLock returns a lock interface implementation for the detected
// platform.
func newPlatformLock(fd uintptr) lock {
	if !(runtime.GOOS == "linux" || runtime.GOOS == "darwin") {
		panic(fmt.Sprintf("file lock does not support %s", runtime.GOOS))
	}
	return &linuxOrDarwinLock{
		fileDescriptor: int(fd),
		processLock:    sync.RWMutex{},
	}
}

// linuxOrDarwinLock is a lock capable of acting as a cross process RWMutex.
// This implementation comes with a couple of subtle drawbacks.
//
// For starters it is an advisory lock. Meaning only processes built to respect
// advisory locks will be prevented from accessing the file out of turn.
//
// Secondly, it allows multiple readers and a single writer, but it does not
// prevent a situation known as "writer starvation". In short, this situation
// occurs when many readers constantly control the lock, leaving a writer in an
// infinite pending state. TODO fix writer starvation.
//
// Lastly, there is the problem that unlocking can fail. The current
// implementation panics, which isn't great. TODO maybe there is a way to
// recover from unlocking errors or maybe it is a non issue.
type linuxOrDarwinLock struct {
	// fileDescriptor is the fileDescriptor of the lockable file.
	fileDescriptor int
	// processLock is a helper lock to sync threads within process since the
	// syscall locks only block across processes.
	processLock sync.RWMutex
}

func (l *linuxOrDarwinLock) Lock() error {
	l.processLock.Lock()
	err := syscall.Flock(
		l.fileDescriptor,
		syscall.LOCK_EX,
	)
	if err != nil {
		l.processLock.Unlock()
		return fmt.Errorf("err LOCK_EX file: %w", err)
	}
	return nil
}

func (l *linuxOrDarwinLock) Unlock() {
	if err := syscall.Flock(
		l.fileDescriptor,
		syscall.LOCK_UN,
	); err != nil {
		panic(fmt.Sprintf("err Unlock LOCK_UN file: %s", err))
	}
	l.processLock.Unlock()
}

func (l *linuxOrDarwinLock) RLock() error {
	l.processLock.RLock()
	err := syscall.Flock(
		l.fileDescriptor,
		syscall.LOCK_SH,
	)
	if err != nil {
		l.processLock.RUnlock()
		return fmt.Errorf("err LOCK_SH file: %w", err)
	}
	return nil
}

func (l *linuxOrDarwinLock) RUnlock() {
	if err := syscall.Flock(
		l.fileDescriptor,
		syscall.LOCK_UN,
	); err != nil {
		panic(fmt.Sprintf("err RUnlock LOCK_UN file: %s", err))
	}
	l.processLock.RUnlock()
}
