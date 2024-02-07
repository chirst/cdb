// Accessed by the kv layer. The pager provides an API for read and write access
// of pages. The pager handles caching the file operations of loading pages into
// memory. It also handles locking.
package main

// TODO handle read write transactions
// TODO handle page caching
// TODO probably make this it's own package or better define public api

import (
	"bytes"
	"encoding/binary"
	"os"
	"sort"
)

const (
	PAGE_SIZE                = 4096
	PAGE_TYPE_UNKNOWN        = 0
	PAGE_TYPE_INTERNAL       = 1
	PAGE_TYPE_LEAF           = 2
	PAGE_TYPE_OFFSET         = 0
	PAGE_TYPE_SIZE           = 2
	PAGE_POINTER_SIZE        = 4
	PARENT_POINTER_OFFSET    = PAGE_TYPE_OFFSET + PAGE_TYPE_SIZE
	LEFT_POINTER_OFFSET      = PARENT_POINTER_OFFSET + PAGE_POINTER_SIZE
	RIGHT_POINTER_OFFSET     = LEFT_POINTER_OFFSET + PAGE_POINTER_SIZE
	PAGE_RECORD_COUNT_OFFSET = RIGHT_POINTER_OFFSET + PAGE_POINTER_SIZE
	PAGE_RECORD_COUNT_SIZE   = 2
	PAGE_ROW_OFFSETS_OFFSET  = PAGE_RECORD_COUNT_OFFSET + PAGE_RECORD_COUNT_SIZE
	PAGE_ROW_OFFSET_SIZE     = 2
	ROOT_PAGE_START          = 3
	FREE_PAGE_COUNTER_SIZE   = 2
	FREE_PAGE_COUNTER_OFFSET = 0
	EMPTY_PARENT_PAGE_NUMBER = 0
)

type pager struct {
	file           storage
	currentMaxPage uint16
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
	cmpb := make([]byte, FREE_PAGE_COUNTER_SIZE)
	f.ReadAt(cmpb, FREE_PAGE_COUNTER_OFFSET)
	cmpi := binary.LittleEndian.Uint16(cmpb)
	if cmpi == EMPTY_PARENT_PAGE_NUMBER {
		// The max page cannot be the reserved page number
		cmpi = 1
	}
	p := &pager{
		file:           f,
		currentMaxPage: cmpi,
	}
	p.getPage(1)
	return p, nil
}

// TODO this is a dirty way of flushing pages
var pageCache []*page = []*page{}

func (p *pager) flush() {
	for _, fp := range pageCache {
		p.writePage(fp)
	}
	pageCache = []*page{}
}

func (p *pager) getPage(pageNumber uint16) *page {
	page := make([]byte, PAGE_SIZE)
	// Page number subtracted by one since 0 is reserved as a pointer to nothing
	p.file.ReadAt(page, int64(ROOT_PAGE_START+(pageNumber-1)*PAGE_SIZE))
	ap := allocatePage(pageNumber, page)
	pageCache = append(pageCache, ap)
	return ap
}

// TODO could possibly just take a page as an argument since pages should know
// their number
func (p *pager) writePage(page *page) error {
	// Page number subtracted by one since 0 is reserved as a pointer to nothing
	_, err := p.file.WriteAt(page.content, int64(ROOT_PAGE_START+(page.getNumber()-1)*PAGE_SIZE))
	if err != nil {
		return err
	}
	return nil
}

func (p *pager) newPage() *page {
	p.currentMaxPage += 1
	cmpb := make([]byte, FREE_PAGE_COUNTER_SIZE)
	binary.LittleEndian.PutUint16(cmpb, p.currentMaxPage)
	p.file.WriteAt(cmpb, FREE_PAGE_COUNTER_OFFSET)
	return allocatePage(p.currentMaxPage, make([]byte, PAGE_SIZE))
}

func allocatePage(pageNumber uint16, content []byte) *page {
	np := &page{
		content: content,
		number:  pageNumber,
	}
	if np.getType() == PAGE_TYPE_UNKNOWN {
		np.setType(PAGE_TYPE_LEAF)
	}
	pageCache = append(pageCache, np)
	return np
}

// page is structured as follows:
// - 2 bytes for the page type.
// - 4 bytes for the parent pointer.
// - 4 bytes for the left pointer.
// - 4 bytes for the right pointer.
// - 2 bytes for the count of tuples.
// - 4 bytes for the tuple offsets (2 bytes key 2 bytes value) multiplied by the
// amount of tuples.
// - Variable length key and value tuples filling the remaining space.
//
// Tuple offsets are sorted and listed in order. Tuples are stored in reverse
// order starting at the end of the page. This is so the end of each tuple can
// be calculated by the start of the previous tuple and in the case of the first
// tuple the size of the page.
type page struct {
	content []byte
	number  uint16
}

// pageTuple is a variable length key value pair.
type pageTuple struct {
	key   []byte
	value []byte
}

func (p *page) getParentPageNumber() (hasParent bool, pageNumber uint16) {
	pn := binary.LittleEndian.Uint16(p.content[PARENT_POINTER_OFFSET : PARENT_POINTER_OFFSET+PAGE_POINTER_SIZE])
	// An unsigned int page number has to be reserved to tell if the current
	// page is a root.
	if pn == EMPTY_PARENT_PAGE_NUMBER {
		return false, EMPTY_PARENT_PAGE_NUMBER
	}
	return true, pn
}

func (p *page) setParentPageNumber(pageNumber uint16) {
	bpn := make([]byte, PAGE_POINTER_SIZE)
	binary.LittleEndian.PutUint16(bpn, pageNumber)
	copy(p.content[PARENT_POINTER_OFFSET:PARENT_POINTER_OFFSET+PAGE_POINTER_SIZE], bpn)
}

func (p *page) getNumber() uint16 {
	return p.number
}

func (p *page) getNumberAsBytes() []byte {
	n := p.getNumber()
	bn := make([]byte, FREE_PAGE_COUNTER_SIZE)
	binary.LittleEndian.PutUint16(bn, n)
	return bn
}

func (p *page) getType() uint16 {
	return binary.LittleEndian.Uint16(p.content[PAGE_TYPE_OFFSET:PAGE_TYPE_SIZE])
}

func (p *page) setType(t uint16) {
	bytePageType := make([]byte, PAGE_TYPE_SIZE)
	binary.LittleEndian.PutUint16(bytePageType, t)
	copy(p.content[PAGE_TYPE_OFFSET:PAGE_TYPE_OFFSET+PAGE_TYPE_SIZE], bytePageType)
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

func (p *page) canInsertTuple(key, value []byte) bool {
	return p.canInsertTuples([]pageTuple{
		{
			key,
			value,
		},
	})
}

func (p *page) canInsertTuples(pageTuples []pageTuple) bool {
	s := 0
	s += PAGE_TYPE_SIZE
	s += PAGE_RECORD_COUNT_SIZE
	s += PAGE_POINTER_SIZE // parent
	s += PAGE_POINTER_SIZE // left
	s += PAGE_POINTER_SIZE // right
	entries := append(pageTuples, p.getEntries()...)
	s += len(entries) * (PAGE_ROW_OFFSET_SIZE + PAGE_ROW_OFFSET_SIZE)
	for _, e := range entries {
		s += len(e.key)
		s += len(e.value)
	}
	return PAGE_SIZE >= s
}

func (p *page) setEntries(entries []pageTuple) {
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

func (p *page) getEntries() []pageTuple {
	entries := []pageTuple{}
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
		entries = append(entries, pageTuple{
			key:   byteKey,
			value: byteValue,
		})
		entryEnd = int(keyOffset)
	}
	return entries
}

func (p *page) setValue(key, value []byte) {
	_, found := p.getValue(key)
	if found {
		withoutFound := []pageTuple{}
		e := p.getEntries()
		for _, entry := range e {
			if !bytes.Equal(entry.key, key) {
				withoutFound = append(withoutFound, entry)
			}
		}
		p.setEntries(append(withoutFound, pageTuple{key, value}))
	} else {
		p.setEntries(append(p.getEntries(), pageTuple{key, value}))
	}
}

func (p *page) getValue(key []byte) ([]byte, bool) {
	e := p.getEntries()
	for _, entry := range e {
		if bytes.Equal(entry.key, key) {
			return entry.value, true
		}
	}
	return []byte{}, false
}
