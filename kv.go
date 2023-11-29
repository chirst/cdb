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
	// 1. Need a source page to start from. Will start from 0 if there is no source
	// page specified. This source page has to do with a table. 0 has to be the
	// system catalog.
	var pageNumber uint16 = 0
	for {
		page := kv.pager.getPage(pageNumber)
		// 2. Decide whether the page is an internal node or a leaf node. This
		// can be determined by asking what the page type is.
		if page.getType() == PAGE_TYPE_LEAF {
			// 4. Find the value for the key and return.
			return page.getValue(key)
		}
		v, found := page.getValue(key)
		if !found {
			return nil, false
		}
		// 3. If the page is internal jump to the next page and go back to step
		// 2. This process guarantees that we are on a leaf page for step 4.
		pageNumber = binary.LittleEndian.Uint16(v)
	}
}

func (kv *kv) Set(key, value []byte) {
	// TODO perhaps there needs to be something like a cursor that handles the
	// write transaction of updating and creating all of these pages.
	var pageNumber uint16 = 0
	// TODO advancing to a leaf page would make for a nice function of its own
	leafPage := kv.pager.getPage(pageNumber)
	for leafPage.getType() != PAGE_TYPE_LEAF {
		// Increment to the next page. TODO This should likely not be
		// page.getValue(key), but should be something like page.search(key) in
		// order to get to a leaf node
		nextPage, found := leafPage.getValue(key)
		if !found {
			// TODO need to be searching not looking directly at key
			return
		}
		pageNumber = binary.LittleEndian.Uint16(nextPage)
		leafPage = kv.pager.getPage(pageNumber)
	}
	// At this point the loop has advanced "leafPage" to a leaf node and can try
	// to update or insert the key value pair within "page".
	// TODO this should likely be a smarter calculation that figures out if the
	// key value can be updated or inserted without a split.
	if leafPage.getFreeSpace() < len(key)+len(value) {
		// TODO this logic of splitting ranges could likely be a function
		// Need to split the node in order to insert
		entries := leafPage.getEntries()
		// allocate left page
		leftPage := kv.pager.newPage()
		leftEntries := entries[:len(entries)/2]
		leftPage.setEntries(leftEntries)
		leftKey := leftPage.getEntries()[0].key
		// allocate right page
		rightPage := kv.pager.newPage()
		rightEntries := entries[len(entries)/2:]
		rightPage.setEntries(rightEntries)
		rightKey := rightPage.getEntries()[0].key
		// TODO possibly flip this switch
		if hasParent, parentPageNumber := leafPage.getParentPageNumber(); !hasParent {
			newParent := kv.pager.newPage()
			newParent.setType(PAGE_TYPE_INTERNAL)
			newParent.setEntries([]pageTuple{
				{
					key:   leftKey,
					value: leftPage.getNumberAsBytes(),
				},
				{
					key:   rightKey,
					value: rightPage.getNumberAsBytes(),
				},
			})
		} else {
			parentPage := kv.pager.getPage(parentPageNumber)
			parentPage.internalInsert(
				kv.pager,
				leftKey,
				leftPage.getNumberAsBytes(),
				rightKey,
				rightPage.getNumberAsBytes(),
			)
		}
		return
	}
	// No splitting needed just update or insert the value
	leafPage.setValue(key, value)
}

const (
	PAGE_SIZE                = 4096
	PAGE_TYPE_UNKNOWN        = 0
	PAGE_TYPE_INTERNAL       = 1
	PAGE_TYPE_LEAF           = 2
	PAGE_TYPE_OFFSET         = 0
	PAGE_TYPE_SIZE           = 2
	PAGE_RECORD_COUNT_OFFSET = PAGE_TYPE_SIZE
	PAGE_RECORD_COUNT_SIZE   = 2
	PAGE_ROW_OFFSETS_OFFSET  = PAGE_TYPE_SIZE + PAGE_RECORD_COUNT_SIZE
	PAGE_ROW_OFFSET_SIZE     = 2
	ROOT_PAGE_START          = 3
	ROOT_PAGE_NUMBER         = 0
	FREE_PAGE_COUNTER_SIZE   = 2
	FREE_PAGE_COUNTER_OFFSET = 0
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

func newMemoryFile() storage {
	return &memoryFile{
		buf: make([]byte, PAGE_SIZE),
	}
}

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
	p := &pager{
		file:           f,
		currentMaxPage: binary.LittleEndian.Uint16(cmpb),
	}
	p.getPage(0)
	return p, nil
}

func (p *pager) getPage(pageNumber uint16) *page {
	page := make([]byte, PAGE_SIZE)
	p.file.ReadAt(page, int64(ROOT_PAGE_START+pageNumber*PAGE_SIZE))
	return allocatePage(pageNumber, page)
}

// TODO could possibly just take a page as an argument since pages should know their number
func (p *pager) writePage(pageNumber uint16, content []byte) error {
	_, err := p.file.WriteAt(content, int64(ROOT_PAGE_START+pageNumber*PAGE_SIZE))
	if err != nil {
		return err
	}
	return nil
}

// page is structured as follows:
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
type page struct {
	content []byte
	number  uint16
}

// pageTuple is a variable length key value pair.
type pageTuple struct {
	key   []byte
	value []byte
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
	return np
}

func (p *page) getParentPageNumber() (hasParent bool, pageNumber uint16) {
	// TODO actually store the parent page number and plan to store sibling
	// pages.
	return false, 0
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

func (p *page) getFreeSpace() int {
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

func (p *page) internalInsert(pager *pager, k1, v1, k2, v2 []byte) {
	// TODO should this be more involved with kv or a cursor of some sort
	// instead of passing the parameter pager as a pointer?
	// Check if the internal page can hold the proposed entries and perform
	// another split or root node allocation if needed. This should have
	// recursive behavior. Otherwise just insert the entries.
	// TODO this if should do a more precise calculation.
	if p.getFreeSpace() < len(k1)+len(v1)+len(k2)+len(v2) {
		// Need to split
		entries := p.getEntries()
		// allocate left page
		leftPage := pager.newPage()
		leftEntries := entries[:len(entries)/2]
		leftPage.setEntries(leftEntries)
		leftKey := leftPage.getEntries()[0].key
		// allocate right page
		rightPage := pager.newPage()
		rightEntries := entries[len(entries)/2:]
		rightPage.setEntries(rightEntries)
		rightKey := rightPage.getEntries()[0].key
		if hasParent, parentPageNumber := p.getParentPageNumber(); hasParent {
			// Try to insert the newly introduced pointers into the parent page.
			parent := pager.getPage(parentPageNumber)
			parent.internalInsert(
				pager,
				leftKey,
				leftPage.getNumberAsBytes(),
				rightKey,
				rightPage.getNumberAsBytes(),
			)
		} else {
			// The root node needs to be split. It is wise to keep the root node
			// the same page so the table catalog doesn't need to be updated
			// every time a root node splits.
			p.setType(PAGE_TYPE_INTERNAL)
			p.setEntries([]pageTuple{
				{
					key:   leftKey,
					value: leftPage.getNumberAsBytes(),
				},
				{
					key:   rightKey,
					value: rightPage.getNumberAsBytes(),
				},
			})
		}
		return
	}
	p.setValue(k1, v1)
	p.setValue(k2, v2)
}
