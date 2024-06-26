// Accessed by the kv layer. The pager provides an API for read and write access
// of pages. The pager handles caching the file operations of loading pages into
// memory. It also handles locking.
package pager

// TODO think about the similarities and differences between pageCache and
// dirtyPages. Think about why the pageCache stores raw bytes of a page and the
// dirtyPages stores a pointer to a page. Think about how caching should be
// handled during a write. For instance, newPage hits dirtyPages, but not
// pageCache.
// TODO try and remove specific integer types in favor of just int.

import (
	"bytes"
	"encoding/binary"
	"sort"
	"sync"

	"github.com/chirst/cdb/pager/cache"
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
	dirtyPages []*Page
	// pageCache caches frequently used pages to reduce expensive reads from
	// the filesystem.
	pageCache pageCache
}

func New(useMemory bool) (*Pager, error) {
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
	// If the max page is the reserved page number the free page counter has not
	// yet been set. Meaning the max page should probably be 1.
	if cmpi == EMPTY_PARENT_PAGE_NUMBER {
		cmpi = 1
	}
	p := &Pager{
		store:          s,
		currentMaxPage: cmpi,
		fileLock:       sync.RWMutex{},
		dirtyPages:     []*Page{},
		pageCache:      cache.NewLRU(PAGE_CACHE_SIZE),
	}
	p.GetPage(1)
	return p, nil
}

func (p *Pager) BeginRead() {
	p.fileLock.RLock()
}

func (p *Pager) EndRead() {
	p.fileLock.RUnlock()
}

func (p *Pager) BeginWrite() {
	p.fileLock.Lock()
	p.isWriting = true
}

// EndWrite creates a copy of the database called a journal. EndWrite proceeds
// to write pages to disk and removes the journal after all pages have been
// written. If there is a crash while the pages are being written the journal
// will be promoted to the main database file the next time the db is started.
func (p *Pager) EndWrite() error {
	if !p.isWriting {
		return nil
	}
	if err := p.store.CreateJournal(); err != nil {
		return err
	}
	for _, fp := range p.dirtyPages {
		p.WritePage(fp)
		p.pageCache.Remove(int(fp.GetNumber()))
	}
	p.dirtyPages = []*Page{}
	p.writeMaxPageNumber()
	if err := p.store.DeleteJournal(); err != nil {
		return err
	}
	p.isWriting = false
	p.fileLock.Unlock()
	return nil
}

func (p *Pager) GetPage(pageNumber uint16) *Page {
	if v, hit := p.pageCache.Get(int(pageNumber)); hit {
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
	p.pageCache.Add(int(pageNumber), page)
	return ap
}

func (p *Pager) WritePage(page *Page) error {
	// Page number subtracted by one since 0 is reserved as a pointer to nothing
	_, err := p.store.WriteAt(page.content, int64(ROOT_PAGE_START+(page.GetNumber()-1)*PAGE_SIZE))
	if err != nil {
		return err
	}
	return nil
}

func (p *Pager) writeMaxPageNumber() {
	cmpb := make([]byte, FREE_PAGE_COUNTER_SIZE)
	binary.LittleEndian.PutUint16(cmpb, p.currentMaxPage)
	p.store.WriteAt(cmpb, FREE_PAGE_COUNTER_OFFSET)
}

func (p *Pager) NewPage() *Page {
	p.currentMaxPage += 1
	np := p.allocatePage(p.currentMaxPage, make([]byte, PAGE_SIZE))
	if p.isWriting {
		p.dirtyPages = append(p.dirtyPages, np)
	}
	return np
}

func (p *Pager) allocatePage(pageNumber uint16, content []byte) *Page {
	np := &Page{
		content: content,
		number:  pageNumber,
	}
	if np.GetType() == PAGE_TYPE_UNKNOWN {
		np.SetType(PAGE_TYPE_LEAF)
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
// TODO could implement overflow pages in case a tuple is larger than some
// threshold (say half the Page). There would be another block that would
// contain pointers to overflow pages or be empty. The overflow pages would
// contain a pointer to the next overflow Page and content of the overflow.
//
// Tuple offsets are sorted and listed in order. Tuples are stored in reverse
// order starting at the end of the Page. This is so the end of each tuple can
// be calculated by the start of the previous tuple and in the case of the first
// tuple the size of the Page.
type Page struct {
	content []byte
	number  uint16
}

// PageTuple is a variable length key value pair.
type PageTuple struct {
	Key   []byte
	Value []byte
}

func (p *Page) GetParentPageNumber() (hasParent bool, pageNumber uint16) {
	pn := binary.LittleEndian.Uint16(p.content[PARENT_POINTER_OFFSET : PARENT_POINTER_OFFSET+PAGE_POINTER_SIZE])
	// An unsigned int page number has to be reserved to tell if the current
	// page is a root.
	if pn == EMPTY_PARENT_PAGE_NUMBER {
		return false, EMPTY_PARENT_PAGE_NUMBER
	}
	return true, pn
}

func (p *Page) SetParentPageNumber(pageNumber uint16) {
	bpn := make([]byte, PAGE_POINTER_SIZE)
	binary.LittleEndian.PutUint16(bpn, pageNumber)
	copy(p.content[PARENT_POINTER_OFFSET:PARENT_POINTER_OFFSET+PAGE_POINTER_SIZE], bpn)
}

// TODO consider just removing the boolean return from all these page pointers
// since 0 means the same thing
func (p *Page) GetLeftPageNumber() (hasLeft bool, pageNumber uint16) {
	pn := binary.LittleEndian.Uint16(p.content[LEFT_POINTER_OFFSET : LEFT_POINTER_OFFSET+PAGE_POINTER_SIZE])
	if pn == EMPTY_PARENT_PAGE_NUMBER {
		return false, EMPTY_PARENT_PAGE_NUMBER
	}
	return true, pn
}

func (p *Page) SetLeftPageNumber(pageNumber uint16) {
	bpn := make([]byte, PAGE_POINTER_SIZE)
	binary.LittleEndian.PutUint16(bpn, pageNumber)
	copy(p.content[LEFT_POINTER_OFFSET:LEFT_POINTER_OFFSET+PAGE_POINTER_SIZE], bpn)
}

func (p *Page) GetRightPageNumber() (hasRight bool, pageNumber uint16) {
	pn := binary.LittleEndian.Uint16(p.content[RIGHT_POINTER_OFFSET : RIGHT_POINTER_OFFSET+PAGE_POINTER_SIZE])
	if pn == EMPTY_PARENT_PAGE_NUMBER {
		return false, EMPTY_PARENT_PAGE_NUMBER
	}
	return true, pn
}

func (p *Page) SetRightPageNumber(pageNumber uint16) {
	bpn := make([]byte, PAGE_POINTER_SIZE)
	binary.LittleEndian.PutUint16(bpn, pageNumber)
	copy(p.content[RIGHT_POINTER_OFFSET:RIGHT_POINTER_OFFSET+PAGE_POINTER_SIZE], bpn)
}

func (p *Page) GetNumber() uint16 {
	return p.number
}

func (p *Page) GetNumberAsBytes() []byte {
	n := p.GetNumber()
	bn := make([]byte, FREE_PAGE_COUNTER_SIZE)
	binary.LittleEndian.PutUint16(bn, n)
	return bn
}

func (p *Page) GetType() uint16 {
	return binary.LittleEndian.Uint16(p.content[PAGE_TYPE_OFFSET:PAGE_TYPE_SIZE])
}

func (p *Page) SetType(t uint16) {
	bytePageType := make([]byte, PAGE_TYPE_SIZE)
	binary.LittleEndian.PutUint16(bytePageType, t)
	copy(p.content[PAGE_TYPE_OFFSET:PAGE_TYPE_OFFSET+PAGE_TYPE_SIZE], bytePageType)
}

func (p *Page) getRecordCount() uint16 {
	return binary.LittleEndian.Uint16(
		p.content[PAGE_RECORD_COUNT_OFFSET : PAGE_RECORD_COUNT_OFFSET+PAGE_RECORD_COUNT_SIZE],
	)
}

func (p *Page) setRecordCount(newCount uint16) {
	byteRecordCount := make([]byte, PAGE_RECORD_COUNT_SIZE)
	binary.LittleEndian.PutUint16(byteRecordCount, newCount)
	copy(
		p.content[PAGE_RECORD_COUNT_OFFSET:PAGE_RECORD_COUNT_OFFSET+PAGE_RECORD_COUNT_SIZE],
		byteRecordCount,
	)
}

func (p *Page) CanInsertTuple(key, value []byte) bool {
	return p.CanInsertTuples([]PageTuple{
		{
			key,
			value,
		},
	})
}

func (p *Page) CanInsertTuples(pageTuples []PageTuple) bool {
	s := 0
	s += PAGE_TYPE_SIZE
	s += PAGE_RECORD_COUNT_SIZE
	s += PAGE_POINTER_SIZE // parent
	s += PAGE_POINTER_SIZE // left
	s += PAGE_POINTER_SIZE // right
	entries := append(pageTuples, p.GetEntries()...)
	s += len(entries) * (PAGE_ROW_OFFSET_SIZE + PAGE_ROW_OFFSET_SIZE)
	for _, e := range entries {
		s += len(e.Key)
		s += len(e.Value)
	}
	return PAGE_SIZE >= s
}

func (p *Page) SetEntries(entries []PageTuple) {
	copy(p.content[PAGE_ROW_OFFSETS_OFFSET:PAGE_SIZE], make([]byte, PAGE_SIZE-PAGE_ROW_OFFSETS_OFFSET))
	sort.Slice(entries, func(a, b int) bool { return bytes.Compare(entries[a].Key, entries[b].Key) == -1 })
	shift := PAGE_ROW_OFFSETS_OFFSET
	entryEnd := PAGE_SIZE
	for _, entry := range entries {
		startKeyOffset := shift
		endKeyOffset := shift + PAGE_ROW_OFFSET_SIZE
		endValueOffset := shift + PAGE_ROW_OFFSET_SIZE + PAGE_ROW_OFFSET_SIZE

		// set key offset
		keyOffset := uint16(entryEnd - len(entry.Key) - len(entry.Value))
		byteKeyOffset := make([]byte, PAGE_ROW_OFFSET_SIZE)
		binary.LittleEndian.PutUint16(byteKeyOffset, keyOffset)
		copy(p.content[startKeyOffset:endKeyOffset], byteKeyOffset)

		// set value offset
		valueOffset := uint16(entryEnd - len(entry.Value))
		byteValueOffset := make([]byte, PAGE_ROW_OFFSET_SIZE)
		binary.LittleEndian.PutUint16(byteValueOffset, valueOffset)
		copy(p.content[endKeyOffset:endValueOffset], byteValueOffset)

		// set key
		copy(p.content[keyOffset:valueOffset], entry.Key)

		// set value
		copy(p.content[valueOffset:valueOffset+uint16(len(entry.Value))], entry.Value)

		// update for next iteration
		shift = endValueOffset
		entryEnd = int(keyOffset)
	}
	p.setRecordCount(uint16(len(entries)))
}

func (p *Page) GetEntries() []PageTuple {
	entries := []PageTuple{}
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
		entries = append(entries, PageTuple{
			Key:   byteKey,
			Value: byteValue,
		})
		entryEnd = int(keyOffset)
	}
	return entries
}

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

func (p *Page) GetValue(key []byte) ([]byte, bool) {
	e := p.GetEntries()
	if p.GetType() == PAGE_TYPE_LEAF {
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
