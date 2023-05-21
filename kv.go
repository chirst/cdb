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
	file storage
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
		file: f,
	}
	p.createRootPageIfNeeded()
	return p, nil
}

func (p *pager) getPage(pageNumber int) *leafPage {
	page := make([]byte, PAGE_SIZE)
	p.file.ReadAt(page, int64(ROOT_PAGE_START+pageNumber*PAGE_SIZE))
	return newLeafPage(page)
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

// leafPage is structured as follows:
// - 2 bytes for the page type.
// - 2 bytes for the count of tuples.
// - 4 bytes for the tuple offsets (2 bytes key 2 bytes value) multiplied by the
// amount of tuples.
// - Variable length key and value tuples filling the remaining space.
//
// Tuple offsets are sorted and listed in order. Tuples are stored in reverse
// order starting at the end of the page. This is so the end of each tuple can
// be calculated by the start of the previous tuple and in the case of the first
// tuple the size of the page.
type leafPage struct {
	content []byte
}

// leafPageTuple is a variable length key value pair.
type leafPageTuple struct {
	key   []byte
	value []byte
}

func newLeafPage(content []byte) *leafPage {
	return &leafPage{
		content: content,
	}
}

func (p *leafPage) getType() uint16 {
	return binary.LittleEndian.Uint16(p.content[PAGE_TYPE_OFFSET:PAGE_TYPE_SIZE])
}

func (p *leafPage) setType(t uint16) {
	bytePageType := make([]byte, PAGE_TYPE_SIZE)
	binary.LittleEndian.PutUint16(bytePageType, t)
	copy(p.content[PAGE_TYPE_OFFSET:PAGE_TYPE_SIZE], bytePageType)
}

func (p *leafPage) getRecordCount() uint16 {
	return binary.LittleEndian.Uint16(
		p.content[PAGE_RECORD_COUNT_OFFSET : PAGE_RECORD_COUNT_OFFSET+PAGE_RECORD_COUNT_SIZE],
	)
}

func (p *leafPage) setRecordCount(newCount uint16) {
	byteRecordCount := make([]byte, PAGE_RECORD_COUNT_SIZE)
	binary.LittleEndian.PutUint16(byteRecordCount, newCount)
	copy(
		p.content[PAGE_RECORD_COUNT_OFFSET:PAGE_RECORD_COUNT_OFFSET+PAGE_RECORD_COUNT_SIZE],
		byteRecordCount,
	)
}

func (p *leafPage) getFreeSpace() int {
	allocatedSpace := 0
	allocatedSpace += PAGE_TYPE_SIZE
	allocatedSpace += PAGE_RECORD_COUNT_SIZE
	entries := p.getEntries()
	allocatedSpace += len(entries) * (PAGE_ROW_OFFSET_SIZE + PAGE_ROW_OFFSET_SIZE)
	for _, e := range entries {
		allocatedSpace += len(e.key)
		allocatedSpace += len(e.value)
	}
	return PAGE_SIZE - allocatedSpace
}

func (p *leafPage) setEntries(entries []leafPageTuple) {
	copy(p.content[PAGE_ROW_OFFSETS_OFFSET:PAGE_SIZE], make([]byte, PAGE_SIZE-PAGE_ROW_OFFSETS_OFFSET))
	sort.Slice(entries, func(a, b int) bool { return bytes.Compare(entries[a].key, entries[b].key) == -1 })
	shift := PAGE_ROW_OFFSETS_OFFSET
	entryEnd := PAGE_SIZE
	for _, entry := range entries {
		startKeyOffset := shift
		endKeyOffset := shift + PAGE_ROW_OFFSET_SIZE
		endValueOffset := shift + PAGE_ROW_OFFSET_SIZE + PAGE_ROW_OFFSET_SIZE

		// set key offset
		keyOffset := uint16(entryEnd - len(entry.key) - len(entry.value))
		byteKeyOffset := make([]byte, PAGE_ROW_OFFSET_SIZE)
		binary.LittleEndian.PutUint16(byteKeyOffset, keyOffset)
		copy(p.content[startKeyOffset:endKeyOffset], byteKeyOffset)

		// set value offset
		valueOffset := uint16(entryEnd - len(entry.value))
		byteValueOffset := make([]byte, PAGE_ROW_OFFSET_SIZE)
		binary.LittleEndian.PutUint16(byteValueOffset, valueOffset)
		copy(p.content[endKeyOffset:endValueOffset], byteValueOffset)

		// set key
		copy(p.content[keyOffset:valueOffset], entry.key)

		// set value
		copy(p.content[valueOffset:valueOffset+uint16(len(entry.value))], entry.value)

		// update for next iteration
		shift = endValueOffset
		entryEnd = int(keyOffset)
	}
	p.setRecordCount(uint16(len(entries)))
}

func (p *leafPage) getEntries() []leafPageTuple {
	entries := []leafPageTuple{}
	recordCount := p.getRecordCount()
	entryEnd := PAGE_SIZE
	for i := uint16(0); i < recordCount; i += 1 {
		startKeyOffset := PAGE_ROW_OFFSETS_OFFSET + (i * (PAGE_ROW_OFFSET_SIZE + PAGE_ROW_OFFSET_SIZE))
		endKeyOffset := PAGE_ROW_OFFSETS_OFFSET + (i * (PAGE_ROW_OFFSET_SIZE + PAGE_ROW_OFFSET_SIZE)) + PAGE_ROW_OFFSET_SIZE
		endValueOffset := PAGE_ROW_OFFSETS_OFFSET + (i * (PAGE_ROW_OFFSET_SIZE + PAGE_ROW_OFFSET_SIZE)) + PAGE_ROW_OFFSET_SIZE + PAGE_ROW_OFFSET_SIZE

		keyOffset := binary.LittleEndian.Uint16(p.content[startKeyOffset:endKeyOffset])
		valueOffset := binary.LittleEndian.Uint16(p.content[endKeyOffset:endValueOffset])

		// These must be copied otherwise the underlying byte array is returned.
		// This causes what seems a unique value to be treated as a reference.
		byteKey := make([]byte, valueOffset-keyOffset)
		copy(byteKey, p.content[keyOffset:valueOffset])
		byteValue := make([]byte, entryEnd-int(valueOffset))
		copy(byteValue, p.content[valueOffset:entryEnd])
		entries = append(entries, leafPageTuple{
			key:   byteKey,
			value: byteValue,
		})
		entryEnd = int(keyOffset)
	}
	return entries
}

func (p *leafPage) setValue(key, value []byte) {
	if p.getFreeSpace() < len(key)+len(value) {
		panic("page cannot fit record")
	}
	_, found := p.getValue(key)
	if found {
		withoutFound := []leafPageTuple{}
		e := p.getEntries()
		for _, entry := range e {
			if !bytes.Equal(entry.key, key) {
				withoutFound = append(withoutFound, entry)
			}
		}
		p.setEntries(append(withoutFound, leafPageTuple{key, value}))
	} else {
		p.setEntries(append(p.getEntries(), leafPageTuple{key, value}))
	}
}

func (p *leafPage) getValue(key []byte) ([]byte, bool) {
	e := p.getEntries()
	for _, entry := range e {
		if bytes.Equal(entry.key, key) {
			return entry.value, true
		}
	}
	return []byte{}, false
}
