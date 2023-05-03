package main

import (
	"bytes"
	"encoding/binary"
	"os"
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

func (kv *kv) Get(key []byte) []byte {
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
	PAGE_SIZE               = 4096
	PAGE_TYPE_INTERNAL      = 1
	PAGE_TYPE_LEAF          = 2
	PAGE_TYPE_KEY_START     = 0
	PAGE_TYPE_KEY_END       = 2
	PAGE_KEY_SIZE           = 4
	PAGE_RECORD_COUNT_START = 2
	PAGE_RECORD_COUNT_END   = 4
	PAGE_ROW_OFFSETS_START  = 4
	PAGE_ROW_OFFSET_LENGTH  = 2
	ROOT_PAGE_START         = 0
	ROOT_PAGE_NUMBER        = 0
)

type pager struct {
	file  *os.File
	cache map[int][]byte // Map of page numbers to chunks
}

func newPager(filename string) (*pager, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
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
- Page type. Integer. Fixed length.
- Count of records. Fixed length.
- Record offsets. Variable length multiple of record count.
- Records. Variable length multiple of record count.
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
	return binary.LittleEndian.Uint16(p.content[PAGE_TYPE_KEY_START:PAGE_TYPE_KEY_END])
}

func (p *page) setType(t uint16) {
	bytePageType := make([]byte, 2)
	binary.LittleEndian.PutUint16(bytePageType, t)
	copy(p.content[PAGE_TYPE_KEY_START:PAGE_TYPE_KEY_END], bytePageType)
}

func (p *page) getRecordCount() uint16 {
	return binary.LittleEndian.Uint16(p.content[PAGE_RECORD_COUNT_START:PAGE_RECORD_COUNT_END])
}

func (p *page) setRecordCount(newCount uint16) {
	byteRecordCount := make([]byte, 2)
	binary.LittleEndian.PutUint16(byteRecordCount, newCount)
	copy(p.content[PAGE_RECORD_COUNT_START:PAGE_RECORD_COUNT_END], byteRecordCount)
}

func (p *page) getRowOffsets() []uint16 {
	rowOffsets := []uint16{}
	recordCount := p.getRecordCount()
	for i := uint16(0); i < recordCount; i += 1 {
		startOffset := PAGE_ROW_OFFSETS_START + i*PAGE_ROW_OFFSET_LENGTH
		endOffset := PAGE_ROW_OFFSETS_START + i*PAGE_ROW_OFFSET_LENGTH + PAGE_ROW_OFFSET_LENGTH
		offset := binary.LittleEndian.Uint16(p.content[startOffset:endOffset])
		rowOffsets = append(rowOffsets, offset)
	}
	return rowOffsets
}

func (p *page) setRowOffset(idx, offset uint16) {
	byteRecordOffset := make([]byte, 2)
	binary.LittleEndian.PutUint16(byteRecordOffset, offset)
	copy(p.content[PAGE_ROW_OFFSETS_START+PAGE_ROW_OFFSET_LENGTH*idx:], byteRecordOffset)
}

func (p *page) setValue(key, value []byte) {
	recordCount := p.getRecordCount()
	p.setRecordCount(uint16(recordCount) + 1)

	offset := PAGE_SIZE - uint16(len(value)) - PAGE_KEY_SIZE
	p.setRowOffset(recordCount, uint16(offset))

	copy(p.content[offset:offset+PAGE_KEY_SIZE], key)
	copy(p.content[offset+PAGE_KEY_SIZE:offset+PAGE_KEY_SIZE+uint16(len(value))], value)
}

func (p *page) getValue(key []byte) []byte {
	offsets := p.getRowOffsets()
	for i, j := 0, len(offsets)-1; i < j; i, j = i+1, j-1 {
		offsets[i], offsets[j] = offsets[j], offsets[i]
	}
	for i, o := range offsets {
		endOffset := uint16(PAGE_SIZE)
		if i+1 < len(offsets)-1 {
			endOffset = offsets[i+1]
		}
		if bytes.Equal(p.content[o:o+uint16(len(key))], key) {
			return p.content[o:endOffset]
		}
	}
	// err
	return []byte{}
}
