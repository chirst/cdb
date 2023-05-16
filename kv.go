package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sort"
)

type kv struct {
	pager *pager
}

func NewKv(filename string) (*kv, error) {
	pager, err := newPager(filename)
	if err != nil {
		return nil, err
	}
	return &kv{
		pager: pager,
	}, nil
}

func (kv *kv) Get(key []byte) ([]byte, bool) {
	page := kv.pager.getPage(0)
	return page.getValue(key)
}

func (kv *kv) Set(key, value []byte) {
	page := kv.pager.getPage(0)
	page.setValue(key, value)
	kv.pager.writePage(0, page.content)
}

// func (kv *kv) Delete(key []byte) Who wants to delete anyways?

const (
	PAGE_SIZE                = 4096
	PAGE_TYPE_INTERNAL       = 1
	PAGE_TYPE_LEAF           = 2
	PAGE_TYPE_OFFSET         = 0
	PAGE_TYPE_SIZE           = 2
	PAGE_RECORD_COUNT_OFFSET = PAGE_TYPE_SIZE
	PAGE_RECORD_COUNT_SIZE   = 2
	PAGE_ROW_OFFSETS_OFFSET  = PAGE_TYPE_SIZE + PAGE_RECORD_COUNT_SIZE
	PAGE_ROW_OFFSET_SIZE     = 2
	PAGE_KEY_SIZE            = 2
	ROOT_PAGE_START          = 0
	ROOT_PAGE_NUMBER         = 0
)

type storage interface {
	io.WriterAt
	io.ReaderAt
}

type memoryFile struct {
	buf []byte
}

func (mf *memoryFile) WriteAt(p []byte, off int64) (n int, err error) {
	for len(mf.buf) < int(off)+len(p) {
		mf.buf = append(mf.buf, make([]byte, PAGE_SIZE)...)
	}
	copy(mf.buf[off:len(p)], p)
	return 0, nil
}

func (mf *memoryFile) ReadAt(p []byte, off int64) (n int, err error) {
	for len(mf.buf) < int(off)+len(p) {
		mf.buf = append(mf.buf, make([]byte, PAGE_SIZE)...)
	}
	copy(p, mf.buf[off:len(p)])
	return 0, nil
}

func newMemoryFile() storage {
	return &memoryFile{
		buf: make([]byte, PAGE_SIZE),
	}
}

type pager struct {
	file  storage
	cache map[int][]byte // Map of page numbers to chunks
}

func newPager(filename string) (*pager, error) {
	var f storage
	if filename == "" {
		f = newMemoryFile()
	} else {
		fl, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}
		f = fl
	}
	p := &pager{
		file:  f,
		cache: make(map[int][]byte),
	}
	p.createRootPageIfNeeded()
	return p, nil
}

func (p *pager) getPage(pageNumber int) *page {
	page := make([]byte, PAGE_SIZE)
	p.file.ReadAt(page, int64(ROOT_PAGE_START+pageNumber*PAGE_SIZE))
	return newPage(page)
}

func (p *pager) writePage(pageNumber int, content []byte) error {
	_, err := p.file.WriteAt(content, int64(ROOT_PAGE_START+pageNumber*PAGE_SIZE))
	if err != nil {
		return err
	}
	return nil
}

func (p *pager) createRootPageIfNeeded() {
	rootPage := p.getPage(0)
	if rootPage.getType() == 0 {
		rootPage.setType(PAGE_TYPE_LEAF)
		p.writePage(0, rootPage.content)
	}
}

/*
A page is structured as follows:
  - Page type uint16. Tells if internal or leaf node.
  - Count of records uint16.
  - Record offsets uint16 each. Variable length multiple of record count. First
    offset is for the first key second offset is for the first value and so on.
  - Records. Variable length multiple of record count. First record ends at
    the end of the page. Second record ends at start of first record and so on.
*/
type page struct {
	content []byte
}

func newPage(content []byte) *page {
	return &page{
		content: content,
	}
}

// internal of leaf
func (p *page) getType() uint16 {
	return binary.LittleEndian.Uint16(p.content[PAGE_TYPE_OFFSET:PAGE_TYPE_SIZE])
}

func (p *page) setType(t uint16) {
	bytePageType := make([]byte, PAGE_TYPE_SIZE)
	binary.LittleEndian.PutUint16(bytePageType, t)
	copy(p.content[PAGE_TYPE_OFFSET:PAGE_TYPE_SIZE], bytePageType)
}

func (p *page) getRecordCount() uint16 {
	return binary.LittleEndian.Uint16(
		p.content[PAGE_RECORD_COUNT_OFFSET : PAGE_RECORD_COUNT_OFFSET+PAGE_RECORD_COUNT_SIZE],
	)
}

func (p *page) setRecordCount(newCount uint16) {
	byteRecordCount := make([]byte, PAGE_RECORD_COUNT_SIZE)
	binary.LittleEndian.PutUint16(byteRecordCount, newCount)
	copy(
		p.content[PAGE_RECORD_COUNT_OFFSET:PAGE_RECORD_COUNT_OFFSET+PAGE_RECORD_COUNT_SIZE],
		byteRecordCount,
	)
}

func (p *page) getRowOffsets() []uint16 {
	rowOffsets := []uint16{}
	recordCount := p.getRecordCount()
	for i := uint16(0); i < recordCount; i += 1 {
		startOffset := PAGE_ROW_OFFSETS_OFFSET + i*PAGE_ROW_OFFSET_SIZE
		endOffset := PAGE_ROW_OFFSETS_OFFSET + i*PAGE_ROW_OFFSET_SIZE + PAGE_ROW_OFFSET_SIZE
		offset := binary.LittleEndian.Uint16(p.content[startOffset:endOffset])
		rowOffsets = append(rowOffsets, offset)
	}
	sort.Slice(rowOffsets, func(a, b int) bool { return rowOffsets[a] > rowOffsets[b] })
	return rowOffsets
}

func (p *page) setRowOffset(idx, offset uint16) {
	byteRecordOffset := make([]byte, PAGE_RECORD_COUNT_SIZE)
	binary.LittleEndian.PutUint16(byteRecordOffset, offset)
	copy(p.content[PAGE_ROW_OFFSETS_OFFSET+PAGE_ROW_OFFSET_SIZE*idx:], byteRecordOffset)
}

func (p *page) setValue(key, value []byte) {
	offsets := p.getRowOffsets()
	lastOffset := uint16(PAGE_SIZE)
	if len(offsets) != 0 {
		lastOffset = offsets[len(offsets)-1]
	}

	recordCount := p.getRecordCount()
	p.setRecordCount(uint16(recordCount) + 1)

	offset := lastOffset - uint16(len(value)) - PAGE_KEY_SIZE
	p.setRowOffset(recordCount, uint16(offset))

	copy(p.content[offset:offset+PAGE_KEY_SIZE], key)
	copy(p.content[offset+PAGE_KEY_SIZE:offset+PAGE_KEY_SIZE+uint16(len(value))], value)
}

func (p *page) getValue(key []byte) ([]byte, bool) {
	offsets := p.getRowOffsets()
	endOffset := uint16(PAGE_SIZE)
	for i, o := range offsets {
		k := p.content[o : o+PAGE_KEY_SIZE]
		v := p.content[o+PAGE_KEY_SIZE : endOffset]
		keyBuf := make([]byte, 2)
		copy(keyBuf, key)
		if bytes.Equal(k, keyBuf) {
			return v, true
		}
		endOffset = offsets[i]
	}
	return []byte{}, false
}
