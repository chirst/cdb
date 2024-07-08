// Accessed by the kv layer. The pager provides an API for read and write access
// of pages. The pager handles caching the file operations of loading pages into
// memory. It also handles locking.
package pager

import (
	"bytes"
	"encoding/binary"
	"slices"
	"sort"
	"sync"

	"github.com/chirst/cdb/pager/cache"
)

// The pager is an abstraction over the database file. The file is formatted
// with a header and pages as follows:
// +---------+
// | HEADER  |
// +---------+
// | Page 1  |
// +---------+
// | Page 2  |
// +---------+
// | Page n  |
// +---------+
// | Page... |
// +---------+

const (
	// journalSuffix is the suffix of the filename the rollback journal uses.
	// If the database file is called cdb.db a journal will be called
	// cdb-journal.db
	journalSuffix = "-journal"
	// DefaultDBFileName is the default name of the file the database uses. The
	// file extension is .db.
	DefaultDBFileName = "cdb"
	// pageCacheSize is maximum amount of pages that can be cached in memory.
	pageCacheSize = 1000
)

// File header constants
const (
	// freePageCounterOffset is in the first position of the file header. It
	// stores the last allocated page.
	freePageCounterOffset = 0
	// freePageCounterSize is a uint32 and must match the size of the page
	// pointer size.
	freePageCounterSize = 4
	// rootPageStart marks the end of the file header.
	rootPageStart = 4
)

// Page constants
const (
	// pageSize is the byte size of a single page. This size is used to
	// calculate the offset for each block.
	//
	// The capacity of the database can be calculated with the pageSize and the
	// PAGE_POINTER_SIZE. For example 4096 * 4,294,967,295 = 1.7592186e+13 bytes
	// which is 17.5 Terabytes. This number could be much larger given the page
	// size was increased. It is eventually limited to the size of a file
	// allowed by the operating system.
	pageSize = 4096
	// pageTypeUnknown is an invalid type.
	pageTypeUnknown = 0
	// pageTypeInternal is a page representing a B tree internal node.
	pageTypeInternal = 1
	// pageTypeLeaf is a page representing a B tree leaf.
	pageTypeLeaf   = 2
	pageTypeOffset = 0
	// pageTypeSize is a uint16
	pageTypeSize = 2 // TODO could be uint8
	// pagePointerSize is a uint32 and must be consistent with the free page
	// counter.
	pagePointerSize       = 4
	parentPointerOffset   = pageTypeOffset + pageTypeSize
	leftPointerOffset     = parentPointerOffset + pagePointerSize
	rightPointerOffset    = leftPointerOffset + pagePointerSize
	pageRecordCountOffset = rightPointerOffset + pagePointerSize
	// pageRecordCountSize is a uint16 and stores the number of records in a
	// page.
	pageRecordCountSize = 2
	// pageRowOffsetsOffset marks the start of offsets that map to the tuple
	// positions on a page.
	pageRowOffsetsOffset = pageRecordCountOffset + pageRecordCountSize
	// pageRowOffsetSize is a uint16 that is the size of each offset.
	pageRowOffsetSize = 2
	// emptyParentPageNumber is a reserved number to indicate no parent.
	emptyParentPageNumber = 0
)

// pageCache defines the page caching interface.
type pageCache interface {
	Get(pageNumber int) ([]byte, bool)
	Add(key int, value []byte)
	Remove(key int)
}

// Pager is an abstraction of the database file. Pager handles efficiently
// accessing the file in a thread safe manner and atomically writing to the
// file.
type Pager struct {
	// store implements storage and is typically a file, but also can be an in
	// memory representation for testing purposes.
	store storage
	// currentMaxPage is a counter that holds the last allocated page number.
	currentMaxPage int
	// fileLock enables read and writes to be in isolation. The RWMutex allows
	// either many readers or only one writer. RWMutex also prevents writer
	// starvation by only allowing readers to acquire a lock if there is no
	// pending writer.
	//
	// TODO the fileLock lives in the database process, so it is only resilient
	// to writers in the same process. If two instances of the database are
	// sharing a file there are no guarantees. This could probably be solved
	// with some sort of lock that is put on the file itself.
	fileLock sync.RWMutex
	// isWriting is a helper flag that is true when a writer has acquired a
	// lock. This enables functions distributing pages to the kv layer to mark
	// the pages as dirty so the pages can be flushed to disk before the write
	// lock is released.
	isWriting bool
	// dirtyPages is a list of pages that need to be flushed to disk in order
	// for a write to be considered complete.
	dirtyPages []*Page
	// pageCache caches frequently used pages to reduce expensive reads from
	// the filesystem.
	pageCache pageCache
}

// New creates a new pager. The useMemory flag means the database will not
// create a file or persist changes to disk. This is useful for testing
// purposes.
func New(useMemory bool, filename string) (*Pager, error) {
	var s storage
	var err error
	if useMemory {
		s = newMemoryStorage()
	} else {
		s, err = newFileStorage(filename)
	}
	if err != nil {
		return nil, err
	}
	p := &Pager{
		store:          s,
		currentMaxPage: allocateFreePageCounter(s),
		fileLock:       sync.RWMutex{},
		dirtyPages:     []*Page{},
		pageCache:      cache.NewLRU(pageCacheSize),
	}
	return p, nil
}

// Read the free page counter from the file header.
func allocateFreePageCounter(s storage) int {
	fb := make([]byte, freePageCounterSize)
	s.ReadAt(fb, freePageCounterOffset)
	fpc := int(binary.LittleEndian.Uint32(fb))
	// If the max page is the reserved page number the free page counter has not
	// yet been set. Meaning the max page should probably be 1.
	if fpc == emptyParentPageNumber {
		fpc = 1
	}
	return fpc
}

// Write the free page counter to the file header.
func (p *Pager) writeFreePageCounter() {
	fb := make([]byte, freePageCounterSize)
	binary.LittleEndian.PutUint32(fb, uint32(p.currentMaxPage))
	p.store.WriteAt(fb, freePageCounterOffset)
}

// BeginRead starts a read transaction. Other readers will be able to access the
// database file.
func (p *Pager) BeginRead() {
	p.fileLock.RLock()
}

// BeginRead ends a read transaction.
func (p *Pager) EndRead() {
	p.fileLock.RUnlock()
}

// BeginWrite starts a write transaction. If there are active readers this will
// go into a pending state. When there is a pending writer no new readers will
// be able to acquire a lock. Once all of the readers have finished this will
// acquire exclusive access to the database file.
func (p *Pager) BeginWrite() {
	p.fileLock.Lock()
	p.isWriting = true
}

// EndWrite creates a copy of the database called a journal. EndWrite proceeds
// to write pages to disk and removes the journal after all pages have been
// written. If there is a crash while the pages are being written the journal
// will be promoted to the main database file the next time the db is started.
// This enables the database to write atomically.
func (p *Pager) EndWrite() error {
	if !p.isWriting {
		return nil
	}
	if err := p.store.CreateJournal(); err != nil {
		return err
	}
	for _, fp := range p.dirtyPages {
		p.WritePage(fp)
		p.pageCache.Remove(fp.GetNumber())
	}
	p.dirtyPages = []*Page{}
	p.writeFreePageCounter()
	if err := p.store.DeleteJournal(); err != nil {
		return err
	}
	p.isWriting = false
	p.fileLock.Unlock()
	return nil
}

// GetPage returns an allocated page. GetPage will return cached pages. GetPage
// will return dirtyPages during a write transaction.
func (p *Pager) GetPage(pageNumber int) *Page {
	// During a write pages are collected in the dirtyPages buffer. These pages
	// must be retrieved from the buffer as they are modified because the file
	// is becoming outdated.
	if p.isWriting {
		dpn := slices.IndexFunc(p.dirtyPages, func(dp *Page) bool {
			return dp.number == pageNumber
		})
		if dpn != -1 {
			return p.dirtyPages[dpn]
		}
	} else {
		if v, hit := p.pageCache.Get(pageNumber); hit {
			return p.allocatePage(pageNumber, v)
		}
	}
	page := make([]byte, pageSize)
	// Page number subtracted by 1 since 0 is reserved as a pointer to nothing.
	p.store.ReadAt(page, int64(rootPageStart+(pageNumber-1)*pageSize))
	ap := p.allocatePage(pageNumber, page)
	if p.isWriting {
		p.dirtyPages = append(p.dirtyPages, ap)
	}
	p.pageCache.Add(pageNumber, page)
	return ap
}

// Write page writes the page to storage.
func (p *Pager) WritePage(page *Page) error {
	// Page number subtracted by one since 0 is reserved as a pointer to nothing
	pn := page.GetNumber() - 1
	pns := pn * pageSize
	off := rootPageStart + pns
	_, err := p.store.WriteAt(page.content, int64(off))
	return err
}

// NewPage increases the free page counter, allocates a new page, and adds it to
// the dirtyPages list. NewPage must be called during a write transaction.
func (p *Pager) NewPage() *Page {
	if !p.isWriting {
		panic("must be a write transaction to allocate a new page")
	}
	p.currentMaxPage += 1
	np := p.allocatePage(p.currentMaxPage, make([]byte, pageSize))
	if p.isWriting {
		p.dirtyPages = append(p.dirtyPages, np)
	}
	return np
}

// allocatePage is a helper function that is capable of converting the
// underlying byte slice into a page structure.
func (p *Pager) allocatePage(pageNumber int, content []byte) *Page {
	np := &Page{
		content: content,
		number:  pageNumber,
	}
	if np.GetType() == pageTypeUnknown {
		np.SetType(pageTypeLeaf)
	}
	return np
}

// Page is structured as follows where values accumulate start to end unless
// otherwise specified:
//   - 2 bytes for the Page type. Which could be internal, leaf or overflow.
//   - 4 bytes for the parent pointer (btree).
//   - 4 bytes for the left pointer (btree).
//   - 4 bytes for the right pointer (btree).
//   - 2 bytes for the count of tuples stored on the Page.
//   - 4 bytes for the tuple offsets (2 bytes key 2 bytes value) multiplied by
//     the count of tuples previously mentioned.
//   - Variable length key and value tuples filling the remaining space. Which
//     accumulates from the end of the Page to the start.
//
// Tuple offsets are sorted and listed in order. Tuples are stored in reverse
// order starting at the end of the Page. This is so the end of each tuple can
// be calculated by the start of the previous tuple and in the case of the first
// tuple the size of the Page.
//
// Example page:
// +---------------------------------------------------------------------------+
// | Page type - Parent pointer - Left pointer - Right pointer - Tuple Count - |
// | Tuple offsets - Tuple offset 1 - Tuple offset 2 - Tuple offset 3 ---------|
// |--> grows forwards                                                         |
// |                  grows backwards <----------- Tuple 3 - Tuple 2 - Tuple 1 |
// +---------------------------------------------------------------------------+
//
// TODO could implement overflow pages in case a tuple is larger than some
// threshold (say half the Page). There would be another block that would
// contain pointers to overflow pages or be empty. The overflow pages would
// contain a pointer to the next overflow Page and content of the overflow.
type Page struct {
	content []byte
	number  int
}

// PageTuple is a variable length key value pair.
type PageTuple struct {
	Key   []byte
	Value []byte
}

func (p *Page) GetParentPageNumber() (hasParent bool, pageNumber int) {
	pn := binary.LittleEndian.Uint32(p.content[parentPointerOffset : parentPointerOffset+pagePointerSize])
	if pn == emptyParentPageNumber {
		return false, emptyParentPageNumber
	}
	return true, int(pn)
}

func (p *Page) SetParentPageNumber(pageNumber int) {
	bpn := make([]byte, pagePointerSize)
	binary.LittleEndian.PutUint32(bpn, uint32(pageNumber))
	copy(p.content[parentPointerOffset:parentPointerOffset+pagePointerSize], bpn)
}

func (p *Page) GetLeftPageNumber() (hasLeft bool, pageNumber int) {
	pn := binary.LittleEndian.Uint32(p.content[leftPointerOffset : leftPointerOffset+pagePointerSize])
	if pn == emptyParentPageNumber {
		return false, emptyParentPageNumber
	}
	return true, int(pn)
}

func (p *Page) SetLeftPageNumber(pageNumber int) {
	bpn := make([]byte, pagePointerSize)
	binary.LittleEndian.PutUint32(bpn, uint32(pageNumber))
	copy(p.content[leftPointerOffset:leftPointerOffset+pagePointerSize], bpn)
}

func (p *Page) GetRightPageNumber() (hasRight bool, pageNumber int) {
	pn := binary.LittleEndian.Uint32(p.content[rightPointerOffset : rightPointerOffset+pagePointerSize])
	if pn == emptyParentPageNumber {
		return false, emptyParentPageNumber
	}
	return true, int(pn)
}

func (p *Page) SetRightPageNumber(pageNumber int) {
	bpn := make([]byte, pagePointerSize)
	binary.LittleEndian.PutUint32(bpn, uint32(pageNumber))
	copy(p.content[rightPointerOffset:rightPointerOffset+pagePointerSize], bpn)
}

func (p *Page) GetNumber() int {
	return p.number
}

func (p *Page) GetNumberAsBytes() []byte {
	n := p.GetNumber()
	bn := make([]byte, freePageCounterSize)
	binary.LittleEndian.PutUint32(bn, uint32(n))
	return bn
}

func (p *Page) GetType() int {
	return int(binary.LittleEndian.Uint16(p.content[pageTypeOffset:pageTypeSize]))
}

func (p *Page) IsLeaf() bool {
	return p.GetType() == pageTypeLeaf
}

func (p *Page) SetType(t int) {
	bytePageType := make([]byte, pageTypeSize)
	binary.LittleEndian.PutUint16(bytePageType, uint16(t))
	copy(p.content[pageTypeOffset:pageTypeOffset+pageTypeSize], bytePageType)
}

func (p *Page) SetTypeInternal() {
	p.SetType(pageTypeInternal)
}

// GetRecordCount returns the value of the counter that tells how many tuples
// are currently stored on the page.
func (p *Page) GetRecordCount() int {
	return int(binary.LittleEndian.Uint16(p.content[pageRecordCountOffset : pageRecordCountOffset+pageRecordCountSize]))
}

func (p *Page) setRecordCount(newCount int) {
	byteRecordCount := make([]byte, pageRecordCountSize)
	binary.LittleEndian.PutUint16(byteRecordCount, uint16(newCount))
	copy(
		p.content[pageRecordCountOffset:pageRecordCountOffset+pageRecordCountSize],
		byteRecordCount,
	)
}

// CanInsertTuples returns true if the page can fit the new tuple otherwise it
// returns false.
func (p *Page) CanInsertTuple(key, value []byte) bool {
	return p.CanInsertTuples([]PageTuple{
		{
			key,
			value,
		},
	})
}

// CanInsertTuples returns true if the page can fit the new tuples otherwise it
// returns false.
func (p *Page) CanInsertTuples(pageTuples []PageTuple) bool {
	s := 0
	s += pageTypeSize
	s += pageRecordCountSize
	s += pagePointerSize // parent
	s += pagePointerSize // left
	s += pagePointerSize // right
	entries := append(pageTuples, p.GetEntries()...)
	s += len(entries) * (pageRowOffsetSize + pageRowOffsetSize)
	for _, e := range entries {
		s += len(e.Key)
		s += len(e.Value)
	}
	return pageSize >= s
}

// SetEntries sets the page tuples in sorted order.
func (p *Page) SetEntries(entries []PageTuple) {
	copy(p.content[pageRowOffsetsOffset:pageSize], make([]byte, pageSize-pageRowOffsetsOffset))
	sort.Slice(entries, func(a, b int) bool { return bytes.Compare(entries[a].Key, entries[b].Key) == -1 })
	shift := pageRowOffsetsOffset
	entryEnd := pageSize
	for _, entry := range entries {
		startKeyOffset := shift
		endKeyOffset := shift + pageRowOffsetSize
		endValueOffset := shift + pageRowOffsetSize + pageRowOffsetSize

		// set key offset
		keyOffset := entryEnd - len(entry.Key) - len(entry.Value)
		byteKeyOffset := make([]byte, pageRowOffsetSize)
		binary.LittleEndian.PutUint16(byteKeyOffset, uint16(keyOffset))
		copy(p.content[startKeyOffset:endKeyOffset], byteKeyOffset)

		// set value offset
		valueOffset := entryEnd - len(entry.Value)
		byteValueOffset := make([]byte, pageRowOffsetSize)
		binary.LittleEndian.PutUint16(byteValueOffset, uint16(valueOffset))
		copy(p.content[endKeyOffset:endValueOffset], byteValueOffset)

		// set key
		copy(p.content[keyOffset:valueOffset], entry.Key)

		// set value
		copy(p.content[valueOffset:valueOffset+len(entry.Value)], entry.Value)

		// update for next iteration
		shift = endValueOffset
		entryEnd = keyOffset
	}
	p.setRecordCount(len(entries))
}

// GetEntries returns the page tuples in sorted order.
func (p *Page) GetEntries() []PageTuple {
	entries := []PageTuple{}
	recordCount := p.GetRecordCount()
	entryEnd := pageSize
	for i := 0; i < recordCount; i += 1 {
		startKeyOffset := pageRowOffsetsOffset + (i * (pageRowOffsetSize + pageRowOffsetSize))
		endKeyOffset := pageRowOffsetsOffset + (i * (pageRowOffsetSize + pageRowOffsetSize)) + pageRowOffsetSize
		endValueOffset := pageRowOffsetsOffset + (i * (pageRowOffsetSize + pageRowOffsetSize)) + pageRowOffsetSize + pageRowOffsetSize

		keyOffset := int(binary.LittleEndian.Uint16(p.content[startKeyOffset:endKeyOffset]))
		valueOffset := int(binary.LittleEndian.Uint16(p.content[endKeyOffset:endValueOffset]))

		// These must be copied otherwise the underlying byte array is returned.
		// This causes what seems a unique value to be treated as a reference.
		byteKey := make([]byte, valueOffset-keyOffset)
		copy(byteKey, p.content[keyOffset:valueOffset])
		byteValue := make([]byte, entryEnd-valueOffset)
		copy(byteValue, p.content[valueOffset:entryEnd])
		entries = append(entries, PageTuple{
			Key:   byteKey,
			Value: byteValue,
		})
		entryEnd = keyOffset
	}
	return entries
}

// SetValue searches with GetValue and adds the value or overwrites the existing
// value.
func (p *Page) SetValue(key, value []byte) {
	_, found := p.GetValue(key)
	if found {
		withoutFound := []PageTuple{}
		e := p.GetEntries()
		for _, entry := range e {
			if !bytes.Equal(entry.Key, key) {
				withoutFound = append(withoutFound, entry)
			}
		}
		p.SetEntries(append(withoutFound, PageTuple{key, value}))
	} else {
		p.SetEntries(append(p.GetEntries(), PageTuple{key, value}))
	}
}

// GetValue searches the page and returns the value and a flag indicated if the
// value was found. If the page is leaf an exact match must be made. If the page
// is internal GetValue will search for the range the key falls in and return
// the ranges value.
func (p *Page) GetValue(key []byte) (value []byte, exists bool) {
	e := p.GetEntries()
	if p.GetType() == pageTypeLeaf {
		for _, entry := range e {
			c := bytes.Compare(entry.Key, key)
			if c == 0 { // entryKey == searchKey
				return entry.Value, true
			}
		}
		return []byte{}, false
	}
	var prevEntry *PageTuple = nil
	for _, entry := range e {
		c := bytes.Compare(entry.Key, key)
		if c == 0 { // entryKey == searchKey
			return entry.Value, true
		}
		if c == 1 { // searchKey < entryKey
			return prevEntry.Value, true
		}
		prevEntry = &entry
	}
	if prevEntry != nil {
		return prevEntry.Value, true
	}
	return []byte{}, false
}
