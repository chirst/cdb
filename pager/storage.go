package pager

// Storage provides an interface for accessing the filesystem. This allows the
// database to run on an in memory buffer if desired.

import (
	"fmt"
	"io"
	"os"
)

type storage interface {
	io.ReaderAt
	io.WriterAt
	CreateJournal() error
	DeleteJournal() error
}

type memoryStorage struct {
	buf []byte
}

func newMemoryStorage() storage {
	return &memoryStorage{
		buf: make([]byte, pageSize),
	}
}

func (mf *memoryStorage) WriteAt(p []byte, off int64) (n int, err error) {
	for len(mf.buf) < int(off)+len(p) {
		mf.buf = append(mf.buf, make([]byte, pageSize)...)
	}
	copy(mf.buf[off:len(p)+int(off)], p)
	return 0, nil
}

func (mf *memoryStorage) ReadAt(p []byte, off int64) (n int, err error) {
	for len(mf.buf) < int(off)+len(p) {
		mf.buf = append(mf.buf, make([]byte, pageSize)...)
	}
	copy(p, mf.buf[off:len(p)+int(off)])
	return 0, nil
}

func (mf *memoryStorage) CreateJournal() error {
	// journal does not matter in memory since all data is lost on a crash
	return nil
}

func (mf *memoryStorage) DeleteJournal() error {
	// journal does not matter in memory since all data is lost on a crash
	return nil
}

type fileStorage struct {
	file *os.File
}

func newFileStorage() (storage, error) {
	jfl, err := os.OpenFile(journalFileName, os.O_RDWR, 0644)
	// if journal file doesn't exist open normal db file
	if err != nil && os.IsNotExist(err) {
		fl, err := os.OpenFile(dbFileName, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, fmt.Errorf("error opening db file: %w", err)
		}
		return &fileStorage{
			file: fl,
		}, nil
	}
	// if journal file has an error
	if err != nil {
		return nil, fmt.Errorf("error opening journal: %w", err)
	}
	// if no error opening journal use journal as main file
	fl, err := os.OpenFile(dbFileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("error opening db file to restore journal: %w", err)
	}
	_, err = io.Copy(fl, jfl)
	if err != nil {
		return nil, fmt.Errorf("error copying journal to db file: %w", err)
	}
	os.Remove(journalFileName)
	return &fileStorage{
		file: fl,
	}, nil
}

func (s *fileStorage) WriteAt(p []byte, off int64) (n int, err error) {
	return s.file.WriteAt(p, off)
}

func (s *fileStorage) ReadAt(p []byte, off int64) (n int, err error) {
	return s.file.ReadAt(p, off)
}

func (s *fileStorage) CreateJournal() error {
	f, err := os.OpenFile(journalFileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	if f.Close() != nil {
		return err
	}
	return nil
}

func (s *fileStorage) DeleteJournal() error {
	err := os.Remove(journalFileName)
	if err != nil {
		return err
	}
	return nil
}
