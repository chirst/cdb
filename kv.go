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
	pager *pager
}

func NewKv(useMemory bool) (*kv, error) {
	pager, err := newPager(useMemory)
	if err != nil {
		return nil, err
	}
	return &kv{
		pager: pager,
	}, nil
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
func (kv *kv) NewRowID(rootPageNumber int) int {
	// TODO
	return 2
}

// cursor is an abstraction that can seek and scan ranges of a btree.
type cursor struct {
	// rootPageNumber is the table this cursor operates on
	rootPageNumber int
}

func NewCursor(rootPageNumber int) *cursor {
	return &cursor{
		rootPageNumber: rootPageNumber,
	}
}

// GotoFirstRecord moves the cursor to the first tuple in ascending order.
func (*cursor) GotoFirstRecord() {}

// GetRowID returns the serialized key of the current tuple.
func (*cursor) GetRowID() int {
	return 1
}

// GetColumn returns the serialized value of the nth column
func (*cursor) GetColumn(nth int) any {
	switch nth {
	case 1:
		return "table"
	case 2:
		return "foo"
	case 3:
		return "foo"
	case 4:
		return 1
	case 5:
		return "{columns:[{name:\"first_name\",type:\"TEXT\"}]}"
	}
	panic("no column handled")
}

// GotoNext moves the cursor to the next tuple in ascending order. If there is
// no next tuple this function will return false otherwise it will return true.
func (*cursor) GotoNext() bool {
	return false
}
