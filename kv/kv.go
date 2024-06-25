// KV provides a set of key value operations that implement data structures such
// as a b-tree to efficiently access the page cache.
package kv

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/chirst/cdb/pager"
)

var errorReservedPage = errors.New("specified a reserved page number")

type KV struct {
	pager   *pager.Pager
	catalog *catalog
}

func New(useMemory bool) (*KV, error) {
	pager, err := pager.New(useMemory)
	if err != nil {
		return nil, err
	}
	catalog := newCatalog()
	ret := &KV{
		pager:   pager,
		catalog: catalog,
	}
	err = ret.ParseSchema()
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (kv *KV) GetCatalog() *catalog {
	return kv.catalog
}

// Get returns a byte array corresponding to the key and a bool indicating if
// the key was found. The pageNumber has to do with the root page of the
// corresponding table. The system catalog uses the page number 1.
func (kv *KV) Get(pageNumber uint16, key []byte) ([]byte, bool, error) {
	if pageNumber == pager.EMPTY_PARENT_PAGE_NUMBER {
		return nil, false, errorReservedPage
	}
	// Step 1. Need a source page to start from. Will start from 1 if there is
	// no source page specified. This source page has to do with a table. 1 has
	// to be the system catalog.
	for {
		page := kv.pager.GetPage(pageNumber)
		// Step 2. Decide whether the page is an internal node or a leaf node.
		//This can be determined by asking what the page type is.
		if page.GetType() == pager.PAGE_TYPE_LEAF {
			// 4. Find the value for the key and return.
			b1, b2 := page.GetValue(key)
			return b1, b2, nil
		}
		v, found := page.GetValue(key)
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
func (kv *KV) Set(pageNumber uint16, key, value []byte) error {
	// TODO set has some issues. One being set doesn't differentiate between
	// insert and update which isn't very intentional. Two being set cannot
	// perform multiple insertions in the span of one transaction since page
	// splits pull out stale pages on subsequent inserts which causes difficult
	// bugs.
	if pageNumber == pager.EMPTY_PARENT_PAGE_NUMBER {
		return errorReservedPage
	}
	leafPage := kv.getLeafPage(pageNumber, key)
	if leafPage.CanInsertTuple(key, value) {
		leafPage.SetValue(key, value)
		return nil
	}
	leftPage, rightPage := kv.splitPage(leafPage)
	insertIntoOne(key, value, leftPage, rightPage)
	hasParent, parentPageNumber := leafPage.GetParentPageNumber()
	if hasParent {
		parentPage := kv.pager.GetPage(parentPageNumber)
		kv.parentInsert(parentPage, leftPage, rightPage)
		return nil
	}
	leafPage.SetType(pager.PAGE_TYPE_INTERNAL)
	leafPage.SetEntries([]pager.PageTuple{
		{
			Key:   leftPage.GetEntries()[0].Key,
			Value: leftPage.GetNumberAsBytes(),
		},
		{
			Key:   rightPage.GetEntries()[0].Key,
			Value: rightPage.GetNumberAsBytes(),
		},
	})
	leftPage.SetParentPageNumber(leafPage.GetNumber())
	rightPage.SetParentPageNumber(leafPage.GetNumber())
	return nil
}

// a helper function to insert into a left or right page given the left and
// right pages have space and the right page is greater than the left.
func insertIntoOne(key, value []byte, lp, rp *pager.Page) {
	rpk := rp.GetEntries()[0].Key
	comp := bytes.Compare(key, rpk)
	if comp == 0 { // key == rpk
		rp.SetEntries(append(rp.GetEntries(), pager.PageTuple{Key: key, Value: value}))
		return
	}
	if comp == -1 { // key < rpk
		lp.SetEntries(append(lp.GetEntries(), pager.PageTuple{Key: key, Value: value}))
		return
	}
	// key > rpk
	rp.SetEntries(append(rp.GetEntries(), pager.PageTuple{Key: key, Value: value}))
}

func (kv *KV) getLeafPage(nextPageNumber uint16, key []byte) *pager.Page {
	p := kv.pager.GetPage(nextPageNumber)
	for p.GetType() != pager.PAGE_TYPE_LEAF {
		nextPage, found := p.GetValue(key)
		if !found {
			return nil
		}
		nextPageNumber = binary.LittleEndian.Uint16(nextPage)
		p = kv.pager.GetPage(nextPageNumber)
	}
	return p
}

func (kv *KV) splitPage(page *pager.Page) (left, right *pager.Page) {
	entries := page.GetEntries()
	leftPage := kv.pager.NewPage()
	leftEntries := entries[:len(entries)/2]
	leftPage.SetEntries(leftEntries)
	rightPage := kv.pager.NewPage()
	rightEntries := entries[len(entries)/2:]
	rightPage.SetEntries(rightEntries)
	return leftPage, rightPage
}

func (kv *KV) parentInsert(p, l, r *pager.Page) {
	k1 := l.GetEntries()[0].Key
	v1 := l.GetNumberAsBytes()
	k2 := r.GetEntries()[0].Key
	v2 := r.GetNumberAsBytes()
	tuples := []pager.PageTuple{{Key: k1, Value: v1}, {Key: k2, Value: v2}}
	if p.CanInsertTuples(tuples) {
		p.SetValue(k1, v1)
		p.SetValue(k2, v2)
		l.SetParentPageNumber(p.GetNumber())
		r.SetParentPageNumber(p.GetNumber())
		return
	}
	leftPage, rightPage := kv.splitPage(p)
	hasParent, parentPageNumber := p.GetParentPageNumber()
	if hasParent {
		l.SetParentPageNumber(parentPageNumber)
		r.SetParentPageNumber(parentPageNumber)
		parentParent := kv.pager.GetPage(parentPageNumber)
		kv.parentInsert(parentParent, leftPage, rightPage)
		return
	}
	// The root node needs to be split. It is wise to keep the root node the
	// same page number so the table catalog doesn't need to be updated every
	// time a root node splits.
	p.SetType(pager.PAGE_TYPE_INTERNAL)
	p.SetEntries([]pager.PageTuple{
		{
			Key:   leftPage.GetEntries()[0].Key,
			Value: leftPage.GetNumberAsBytes(),
		},
		{
			Key:   rightPage.GetEntries()[0].Key,
			Value: rightPage.GetNumberAsBytes(),
		},
	})
	leftPage.SetParentPageNumber(p.GetNumber())
	rightPage.SetParentPageNumber(p.GetNumber())
}

// NewBTree creates an empty BTree and returns the new tree's root page number.
func (kv *KV) NewBTree() int {
	np := kv.pager.NewPage()
	return int(np.GetNumber())
}

func (kv *KV) BeginReadTransaction() {
	kv.pager.BeginRead()
}

func (kv *KV) EndReadTransaction() {
	kv.pager.EndRead()
}

func (kv *KV) BeginWriteTransaction() {
	kv.pager.BeginWrite()
}

func (kv *KV) EndWriteTransaction() error {
	return kv.pager.EndWrite()
}

// NewRowID returns the highest unused key in a table for the rootPageNumber.
// For a integer key it is the largest integer key plus one.
func (kv *KV) NewRowID(rootPageNumber int) (int, error) {
	// TODO could possibly cache this in the catalog or something
	candidate := kv.pager.GetPage(uint16(rootPageNumber))
	if len(candidate.GetEntries()) == 0 {
		return 1, nil
	}
	for candidate.GetType() != pager.PAGE_TYPE_LEAF {
		pagePointers := candidate.GetEntries()
		descendingPageNum := pagePointers[len(pagePointers)-1].Value
		candidate = kv.pager.GetPage(binary.LittleEndian.Uint16(descendingPageNum))
	}
	k := candidate.GetEntries()[len(candidate.GetEntries())-1].Key
	dk := DecodeKey(k)
	return dk + 1, nil
}

func (kv *KV) ParseSchema() error {
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

// Cursor is an abstraction that can seek and scan ranges of a btree.
type Cursor struct {
	// rootPageNumber is the table this cursor operates on
	rootPageNumber     int
	currentPageEntries []pager.PageTuple
	currentTupleIndex  int
	pager              *pager.Pager
}

func (kv *KV) NewCursor(rootPageNumber int) *Cursor {
	return &Cursor{
		rootPageNumber: rootPageNumber,
		pager:          kv.pager,
	}
}

// GotoFirstRecord moves the cursor to the first tuple in ascending order. It
// returns true if the table has values. It returns false if the table is empty.
func (c *Cursor) GotoFirstRecord() bool {
	candidatePage := c.pager.GetPage(uint16(c.rootPageNumber))
	if len(candidatePage.GetEntries()) == 0 {
		return false
	}
	for candidatePage.GetType() != pager.PAGE_TYPE_LEAF {
		pagePointers := candidatePage.GetEntries()
		ascendingPageNum := pagePointers[0].Value
		candidatePage = c.pager.GetPage(binary.LittleEndian.Uint16(ascendingPageNum))
	}
	c.currentPageEntries = candidatePage.GetEntries()
	c.currentTupleIndex = 0
	return true
}

func (c *Cursor) GotoLastRecord() bool {
	candidatePage := c.pager.GetPage(uint16(c.rootPageNumber))
	if len(candidatePage.GetEntries()) == 0 {
		return false
	}
	for candidatePage.GetType() != pager.PAGE_TYPE_LEAF {
		pagePointers := candidatePage.GetEntries()
		descendingPageNum := pagePointers[len(pagePointers)-1].Value
		candidatePage = c.pager.GetPage(binary.LittleEndian.Uint16(descendingPageNum))
	}
	c.currentPageEntries = candidatePage.GetEntries()
	c.currentTupleIndex = len(c.currentPageEntries) - 1
	return true
}

// GetKey returns the key of the current tuple.
func (c *Cursor) GetKey() []byte {
	return c.currentPageEntries[c.currentTupleIndex].Key
}

// GetValue returns the values
func (c *Cursor) GetValue() []byte {
	return c.currentPageEntries[c.currentTupleIndex].Value
}

// GotoNext moves the cursor to the next tuple in ascending order. If there is
// no next tuple this function will return false otherwise it will return true.
func (c *Cursor) GotoNext() bool {
	if c.currentTupleIndex+1 <= len(c.currentPageEntries)-1 {
		c.currentTupleIndex += 1
		return true
	}
	return false
}
