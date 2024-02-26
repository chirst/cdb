// Accessed by the kv layer. The pager provides an API for read and write access
// of pages. The pager handles caching the file operations of loading pages into
// memory. It also handles locking.
package main

// TODO handle page caching
// TODO probably make this it's own package or better define public api
// TODO handle log fatal
// TODO probably mock file creation with storage layer better

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"os"
	"sort"
	"sync"
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
	JOURNAL_FILE_NAME        = "journal.db"
)

type pager struct {
	// file implements storage
	file storage
	// currentMaxPage is a counter that holds a free page number
	currentMaxPage uint16
	// fileLock enables read and writes to be in isolation. The RWMutex allows
	// either many readers or only one writer. RWMutex also prevents writer
	// starvation by only allowing readers to acquire a lock if there is no
	// pending writer.
	fileLock sync.RWMutex
	// isWriting is a helper flag that is true when a writer has acquired a
	// lock. This enables functions distributing pages to the kv layer to mark
	// the pages as dirty so the pages can be flushed to disk before the write
	// lock is released.
	isWriting bool
	// dirtyPages is a list of pages that need to be flushed to disk in order
	// for a write to be considered complete.
	dirtyPages []*page
}

func newPager(filename string) (*pager, error) {
	var f storage
	if filename == "" {
		f = newMemoryFile()
	} else {
		// Open journal file
		jfl, err := os.OpenFile(JOURNAL_FILE_NAME, os.O_RDWR, 0644)
		if err != nil && os.IsNotExist(err) {
			// if journal file doesn't exist open normal db file
			fl, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				return nil, err
			}
			f = fl
		} else if err != nil {
			// if error opening journal fail
			return nil, err
		} else {
			// if no error opening journal use journal as main file
			fl, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				return nil, err
			}
			io.Copy(fl, jfl)
			os.Remove(JOURNAL_FILE_NAME)
			f = fl
		}
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
		fileLock:       sync.RWMutex{},
		dirtyPages:     []*page{},
	}
	p.getPage(1)
	return p, nil
}

func (p *pager) beginRead() {
	p.fileLock.RLock()
}

func (p *pager) endRead() {
	p.fileLock.RUnlock()
}

func (p *pager) beginWrite() {
	p.fileLock.Lock()
	p.isWriting = true
}

// endWrite creates a copy of the database called a journal. endWrite proceeds
// to write pages to disk and removes the journal after all pages have been
// written. If there is a crash while the pages are being written the journal
// will be promoted to the main database file the next time the db is started.
func (p *pager) endWrite() {
	f, err := os.OpenFile(JOURNAL_FILE_NAME, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
	}
	if _, err = io.Copy(f, p.file); err != nil {
		log.Fatal(err)
	}
	if f.Close() != nil {
		log.Fatal(err)
	}
	for _, fp := range p.dirtyPages {
		p.writePage(fp)
	}
	p.dirtyPages = []*page{}
	p.writeMaxPageNumber()
	if err = os.Remove(JOURNAL_FILE_NAME); err != nil {
		log.Fatal(err)
	}
	p.isWriting = false
	p.fileLock.Unlock()
}

func (p *pager) getPage(pageNumber uint16) *page {
	page := make([]byte, PAGE_SIZE)
	// Page number subtracted by one since 0 is reserved as a pointer to nothing
	p.file.ReadAt(page, int64(ROOT_PAGE_START+(pageNumber-1)*PAGE_SIZE))
	ap := allocatePage(pageNumber, page)
	if p.isWriting {
		p.dirtyPages = append(p.dirtyPages, ap)
	}
	return ap
}

func (p *pager) writePage(page *page) error {
	// Page number subtracted by one since 0 is reserved as a pointer to nothing
	_, err := p.file.WriteAt(page.content, int64(ROOT_PAGE_START+(page.getNumber()-1)*PAGE_SIZE))
	if err != nil {
		return err
	}
	return nil
}

func (p *pager) writeMaxPageNumber() {
	cmpb := make([]byte, FREE_PAGE_COUNTER_SIZE)
	binary.LittleEndian.PutUint16(cmpb, p.currentMaxPage)
	p.file.WriteAt(cmpb, FREE_PAGE_COUNTER_OFFSET)
}

func (p *pager) newPage() *page {
	p.currentMaxPage += 1
	np := allocatePage(p.currentMaxPage, make([]byte, PAGE_SIZE))
	if p.isWriting {
		p.dirtyPages = append(p.dirtyPages, np)
	}
	return np
}

// allocatePage not being a receiver allows for easy construction of a page
// during unit testing.
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
	if p.getType() == PAGE_TYPE_LEAF {
		for _, entry := range e {
			c := bytes.Compare(entry.key, key)
			if c == 0 { // entryKey == searchKey
				return entry.value, true
			}
		}
		return []byte{}, false
	}
	var prevEntry *pageTuple = nil
	for _, entry := range e {
		c := bytes.Compare(entry.key, key)
		if c == 0 { // entryKey == searchKey
			return entry.value, true
		}
		if c == 1 { // searchKey < entryKey
			return prevEntry.value, true
		}
		prevEntry = &entry
	}
	if prevEntry != nil {
		return prevEntry.value, true
	}
	return []byte{}, false
}
