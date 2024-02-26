// Storage provides an interface for accessing the filesystem. This allows the
// database to run on an in memory buffer if desired.
package main

import "io"

type storage interface {
	io.WriterAt
	io.ReaderAt
	io.Reader
}

type memoryFile struct {
	buf []byte
}

func (mf *memoryFile) WriteAt(p []byte, off int64) (n int, err error) {
	for len(mf.buf) < int(off)+len(p) {
		mf.buf = append(mf.buf, make([]byte, PAGE_SIZE)...)
	}
	copy(mf.buf[off:len(p)+int(off)], p)
	return 0, nil
}

func (mf *memoryFile) ReadAt(p []byte, off int64) (n int, err error) {
	for len(mf.buf) < int(off)+len(p) {
		mf.buf = append(mf.buf, make([]byte, PAGE_SIZE)...)
	}
	copy(p, mf.buf[off:len(p)+int(off)])
	return 0, nil
}

func (mf *memoryFile) Read(p []byte) (int, error) {
	for len(mf.buf) < len(p) {
		mf.buf = append(mf.buf, make([]byte, PAGE_SIZE)...)
	}
	copy(p, mf.buf[:len(p)])
	return 0, io.EOF
}

func newMemoryFile() storage {
	return &memoryFile{
		buf: make([]byte, PAGE_SIZE),
	}
}
