// KV provides a set of key value operations that implement data structures such
// as a b-tree to efficiently access the page cache.
package main

import (
	"bytes"
	"encoding/binary"
	"errors"
)

var errorReservedPage = errors.New("specified a reserved page number")

type kv struct {
	pager   *pager
	catalog *catalog
}

func NewKv(useMemory bool) (*kv, error) {
	pager, err := newPager(useMemory)
	if err != nil {
		return nil, err
	}
	catalog := newCatalog()
	ret := &kv{
		pager:   pager,
		catalog: catalog,
	}
	err = ret.ParseSchema()
	if err != nil {
		return nil, err
	}
	return ret, nil
}

// Get returns a byte array corresponding to the key and a bool indicating if
// the key was found. The pageNumber has to do with the root page of the
// corresponding table. The system catalog uses the page number 1.
func (kv *kv) Get(pageNumber uint16, key []byte) ([]byte, bool, error) {
	if pageNumber == EMPTY_PARENT_PAGE_NUMBER {
		return nil, false, errorReservedPage
	}
	// Step 1. Need a source page to start from. Will start from 1 if there is
	// no source page specified. This source page has to do with a table. 1 has
	// to be the system catalog.
	for {
		page := kv.pager.getPage(pageNumber)
		// Step 2. Decide whether the page is an internal node or a leaf node.
		//This can be determined by asking what the page type is.
		if page.getType() == PAGE_TYPE_LEAF {
			// 4. Find the value for the key and return.
			b1, b2 := page.getValue(key)
			return b1, b2, nil
		}
		v, found := page.getValue(key)
		if !found {
			return nil, false, nil
		}
		// Step 3. If the page is internal jump to the next page and go back to
		// 2. This process guarantees that we are on a leaf page for step 4.
		pageNumber = binary.LittleEndian.Uint16(v)
	}
}

// Set inserts or updates the value for the given key. The pageNumber has to do
// with the root page of the corresponding table. The system catalog uses the
// page number 1.
func (kv *kv) Set(pageNumber uint16, key, value []byte) error {
	if pageNumber == EMPTY_PARENT_PAGE_NUMBER {
		return errorReservedPage
	}
	leafPage := kv.getLeafPage(pageNumber, key)
	if leafPage.canInsertTuple(key, value) {
		leafPage.setValue(key, value)
		return nil
	}
	leftPage, rightPage := kv.splitPage(leafPage)
	insertIntoOne(key, value, leftPage, rightPage)
	hasParent, parentPageNumber := leafPage.getParentPageNumber()
	if hasParent {
		parentPage := kv.pager.getPage(parentPageNumber)
		kv.parentInsert(parentPage, leftPage, rightPage)
		return nil
	}
	leafPage.setType(PAGE_TYPE_INTERNAL)
	leafPage.setEntries([]pageTuple{
		{
			key:   leftPage.getEntries()[0].key,
			value: leftPage.getNumberAsBytes(),
		},
		{
			key:   rightPage.getEntries()[0].key,
			value: rightPage.getNumberAsBytes(),
		},
	})
	leftPage.setParentPageNumber(leafPage.getNumber())
	rightPage.setParentPageNumber(leafPage.getNumber())
	return nil
}

// TODO this is really messy and is a symptom of internal pages using two keys
// to represent two ranges where only one key is necessary.
func insertIntoOne(key, value []byte, p1, p2 *page) {
	p1k := p1.getEntries()[0].key
	p2k := p2.getEntries()[0].key
	if bytes.Equal(p1k, key) {
		p1.setEntries(append(p1.getEntries(), pageTuple{key, value}))
		return
	}
	if bytes.Equal(p2k, key) {
		p2.setEntries(append(p2.getEntries(), pageTuple{key, value}))
		return
	}
	if bytes.Compare(p1k, key) == -1 && bytes.Compare(key, p2k) == -1 {
		p1.setEntries(append(p1.getEntries(), pageTuple{key, value}))
		return
	}
	p2.setEntries(append(p2.getEntries(), pageTuple{key, value}))
}

func (kv *kv) getLeafPage(nextPageNumber uint16, key []byte) *page {
	p := kv.pager.getPage(nextPageNumber)
	for p.getType() != PAGE_TYPE_LEAF {
		nextPage, found := p.getValue(key)
		if !found {
			return nil
		}
		nextPageNumber = binary.LittleEndian.Uint16(nextPage)
		p = kv.pager.getPage(nextPageNumber)
	}
	return p
}

func (kv *kv) splitPage(page *page) (left, right *page) {
	entries := page.getEntries()
	leftPage := kv.pager.newPage()
	leftEntries := entries[:len(entries)/2]
	leftPage.setEntries(leftEntries)
	rightPage := kv.pager.newPage()
	rightEntries := entries[len(entries)/2:]
	rightPage.setEntries(rightEntries)
	return leftPage, rightPage
}

func (kv *kv) parentInsert(p, l, r *page) {
	k1 := l.getEntries()[0].key
	v1 := l.getNumberAsBytes()
	k2 := r.getEntries()[0].key
	v2 := r.getNumberAsBytes()
	tuples := []pageTuple{{key: k1, value: v1}, {key: k2, value: v2}}
	if p.canInsertTuples(tuples) {
		p.setValue(k1, v1)
		p.setValue(k2, v2)
		l.setParentPageNumber(p.getNumber())
		r.setParentPageNumber(p.getNumber())
		return
	}
	leftPage, rightPage := kv.splitPage(p)
	hasParent, parentPageNumber := p.getParentPageNumber()
	if hasParent {
		l.setParentPageNumber(parentPageNumber)
		r.setParentPageNumber(parentPageNumber)
		parentParent := kv.pager.getPage(parentPageNumber)
		kv.parentInsert(parentParent, leftPage, rightPage)
		return
	}
	// The root node needs to be split. It is wise to keep the root node the
	// same page number so the table catalog doesn't need to be updated every
	// time a root node splits.
	p.setType(PAGE_TYPE_INTERNAL)
	p.setEntries([]pageTuple{
		{
			key:   leftPage.getEntries()[0].key,
			value: leftPage.getNumberAsBytes(),
		},
		{
			key:   rightPage.getEntries()[0].key,
			value: rightPage.getNumberAsBytes(),
		},
	})
	leftPage.setParentPageNumber(p.getNumber())
	rightPage.setParentPageNumber(p.getNumber())
}

// NewBTree creates an empty BTree and returns the new tree's root page number.
func (kv *kv) NewBTree() int {
	np := kv.pager.newPage()
	return int(np.number)
}

func (kv *kv) BeginReadTransaction() {
	kv.pager.beginRead()
}

func (kv *kv) EndReadTransaction() {
	kv.pager.endRead()
}

func (kv *kv) BeginWriteTransaction() {
	kv.pager.beginWrite()
}

func (kv *kv) EndWriteTransaction() error {
	return kv.pager.endWrite()
}

// NewRowID returns the highest unused key in a table for the rootPageNumber.
// For a integer key it is the largest integer key plus one.
func (kv *kv) NewRowID(rootPageNumber int) (int, error) {
	// TODO could possibly cache this in the catalog or something
	candidate := kv.pager.getPage(uint16(rootPageNumber))
	if len(candidate.getEntries()) == 0 {
		return 1, nil
	}
	for candidate.getType() != PAGE_TYPE_LEAF {
		pagePointers := candidate.getEntries()
		descendingPageNum := pagePointers[len(pagePointers)-1].value
		candidate = kv.pager.getPage(binary.LittleEndian.Uint16(descendingPageNum))
	}
	k := candidate.getEntries()[len(candidate.getEntries())-1].key
	dk := DecodeKey(k)
	return dk + 1, nil
}

func (kv *kv) ParseSchema() error {
	c := kv.NewCursor(1)
	exists := c.GotoFirstRecord()
	if !exists {
		return nil
	}
	var objs []object
	for exists {
		v := c.GetValue()
		dv, err := Decode(v)
		if err != nil {
			return err
		}
		o := &object{
			objectType:     dv[0].(string),
			name:           dv[1].(string),
			tableName:      dv[2].(string),
			rootPageNumber: dv[3].(int),
			jsonSchema:     dv[4].(string),
		}
		objs = append(objs, *o)
		exists = c.GotoNext()
	}
	kv.catalog.schema.objects = objs
	return nil
}

// cursor is an abstraction that can seek and scan ranges of a btree.
type cursor struct {
	// rootPageNumber is the table this cursor operates on
	rootPageNumber     int
	currentPageEntries []pageTuple
	currentTupleIndex  int
	pager              *pager
}

func (kv *kv) NewCursor(rootPageNumber int) *cursor {
	return &cursor{
		rootPageNumber: rootPageNumber,
		pager:          kv.pager,
	}
}

// GotoFirstRecord moves the cursor to the first tuple in ascending order. It
// returns true if the table has values. It returns false if the table is empty.
func (c *cursor) GotoFirstRecord() bool {
	candidatePage := c.pager.getPage(uint16(c.rootPageNumber))
	if len(candidatePage.getEntries()) == 0 {
		return false
	}
	for candidatePage.getType() != PAGE_TYPE_LEAF {
		pagePointers := candidatePage.getEntries()
		ascendingPageNum := pagePointers[0].value
		candidatePage = c.pager.getPage(binary.LittleEndian.Uint16(ascendingPageNum))
	}
	c.currentPageEntries = candidatePage.getEntries()
	c.currentTupleIndex = 0
	return true
}

func (c *cursor) GotoLastRecord() bool {
	candidatePage := c.pager.getPage(uint16(c.rootPageNumber))
	if len(candidatePage.getEntries()) == 0 {
		return false
	}
	for candidatePage.getType() != PAGE_TYPE_LEAF {
		pagePointers := candidatePage.getEntries()
		descendingPageNum := pagePointers[len(pagePointers)-1].value
		candidatePage = c.pager.getPage(binary.LittleEndian.Uint16(descendingPageNum))
	}
	c.currentPageEntries = candidatePage.getEntries()
	c.currentTupleIndex = len(c.currentPageEntries) - 1
	return true
}

// GetKey returns the key of the current tuple.
func (c *cursor) GetKey() []byte {
	return c.currentPageEntries[c.currentTupleIndex].key
}

// GetValue returns the values
func (c *cursor) GetValue() []byte {
	return c.currentPageEntries[c.currentTupleIndex].value
}

// GotoNext moves the cursor to the next tuple in ascending order. If there is
// no next tuple this function will return false otherwise it will return true.
func (c *cursor) GotoNext() bool {
	if c.currentTupleIndex+1 <= len(c.currentPageEntries)-1 {
		c.currentTupleIndex += 1
		return true
	}
	return false
}
