// Accessed by the kv layer. The pager provides an API for read and write access
// of pages. The pager handles caching the file operations of loading pages into
// memory. It also handles locking.
package main

// TODO probably make this it's own package or better define public api
// TODO pageCache should have different implementations that can be swapped.
// Should also have customizable cache size. Could also have command to clear
// cache.
// TODO think about the similarities and differences between pageCache and
// dirtyPages. Think about why the pageCache stores raw bytes of a page and the
// dirtyPages stores a pointer to a page. Think about how caching should be
// handled during a write. For instance, newPage hits dirtyPages, but not
// pageCache.

import (
	"bytes"
	"encoding/binary"
	"log"
	"sort"
	"sync"

	"github.com/golang/groupcache/lru"
)

const (
	PAGE_CACHE_SIZE          = 1000
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
	// store implements storage and is typically a file
	store storage
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
	// pageCache caches frequently used pages to reduce expensive reads from
	// the filesystem.
	pageCache pageCache
}

func newPager(useMemory bool) (*pager, error) {
	var s storage
	var err error
	if useMemory {
		s = newMemoryStorage()
	} else {
		s, err = newFileStorage()
	}
	if err != nil {
		return nil, err
	}
	cmpb := make([]byte, FREE_PAGE_COUNTER_SIZE)
	s.ReadAt(cmpb, FREE_PAGE_COUNTER_OFFSET)
	cmpi := binary.LittleEndian.Uint16(cmpb)
	if cmpi == EMPTY_PARENT_PAGE_NUMBER {
		// The max page cannot be the reserved page number
		cmpi = 1
	}
	p := &pager{
		store:          s,
		currentMaxPage: cmpi,
		fileLock:       sync.RWMutex{},
		dirtyPages:     []*page{},
		pageCache:      newLruPageCache(PAGE_CACHE_SIZE),
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
func (p *pager) endWrite() error {
	if err := p.store.CreateJournal(); err != nil {
		return err
	}
	for _, fp := range p.dirtyPages {
		p.writePage(fp)
		p.pageCache.remove(fp.getNumber())
	}
	p.dirtyPages = []*page{}
	p.writeMaxPageNumber()
	if err := p.store.DeleteJournal(); err != nil {
		return err
	}
	p.isWriting = false
	p.fileLock.Unlock()
	return nil
}

func (p *pager) getPage(pageNumber uint16) *page {
	if v, hit := p.pageCache.get(pageNumber); hit {
		ap := p.allocatePage(pageNumber, v)
		if p.isWriting {
			p.dirtyPages = append(p.dirtyPages, ap)
		}
		return ap
	}
	page := make([]byte, PAGE_SIZE)
	// Page number subtracted by one since 0 is reserved as a pointer to nothing
	p.store.ReadAt(page, int64(ROOT_PAGE_START+(pageNumber-1)*PAGE_SIZE))
	ap := p.allocatePage(pageNumber, page)
	if p.isWriting {
		p.dirtyPages = append(p.dirtyPages, ap)
	}
	p.pageCache.add(pageNumber, page)
	return ap
}

func (p *pager) writePage(page *page) error {
	// Page number subtracted by one since 0 is reserved as a pointer to nothing
	_, err := p.store.WriteAt(page.content, int64(ROOT_PAGE_START+(page.getNumber()-1)*PAGE_SIZE))
	if err != nil {
		return err
	}
	return nil
}

func (p *pager) writeMaxPageNumber() {
	cmpb := make([]byte, FREE_PAGE_COUNTER_SIZE)
	binary.LittleEndian.PutUint16(cmpb, p.currentMaxPage)
	p.store.WriteAt(cmpb, FREE_PAGE_COUNTER_OFFSET)
}

func (p *pager) newPage() *page {
	p.currentMaxPage += 1
	np := p.allocatePage(p.currentMaxPage, make([]byte, PAGE_SIZE))
	if p.isWriting {
		p.dirtyPages = append(p.dirtyPages, np)
	}
	return np
}

func (p *pager) allocatePage(pageNumber uint16, content []byte) *page {
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

type pageCache interface {
	get(pageNumber uint16) ([]byte, bool)
	add(key uint16, value []byte)
	remove(key uint16)
}

// lruPageCache implements pageCache
type lruPageCache struct {
	cache *lru.Cache
}

func (c *lruPageCache) get(key uint16) (value []byte, hit bool) {
	v, ok := c.cache.Get(key)
	if !ok {
		return nil, false
	}
	vb, ok := v.([]byte)
	if !ok {
		log.Fatal("lru cache is not byte array")
	}
	return vb, true
}

func (c *lruPageCache) add(key uint16, value []byte) {
	c.cache.Add(key, value)
}

func (c *lruPageCache) remove(key uint16) {
	c.cache.Remove(key)
}

func newLruPageCache(maxSize int) *lruPageCache {
	return &lruPageCache{
		cache: lru.New(maxSize),
	}
}
