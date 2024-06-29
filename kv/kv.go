// KV provides a set of key value operations that implement data structures such
// as a b-tree to efficiently access the page cache. KV implements an
// abstraction called a cursor to efficiently seek and scan the btree. Each B
// tree is referenced with a page number assigned by the catalog.
package kv

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/chirst/cdb/pager"
)

type KV struct {
	pager   *pager.Pager
	catalog *catalog
}

func New(useMemory bool, filename string) (*KV, error) {
	pager, err := pager.New(useMemory, filename)
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
func (kv *KV) Get(pageNumber int, key []byte) ([]byte, bool, error) {
	if pageNumber == 0 {
		panic("pageNumber cannot be 0")
	}
	// Step 1. Need a source page to start from. Will start from 1 if there is
	// no source page specified. This source page has to do with a table. 1 has
	// to be the system catalog.
	for {
		page := kv.pager.GetPage(pageNumber)
		// Step 2. Decide whether the page is an internal node or a leaf node.
		//This can be determined by asking what the page type is.
		if page.IsLeaf() {
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
		pageNumber = int(binary.LittleEndian.Uint16(v))
	}
}

// Set inserts or updates the value for the given key. The pageNumber has to do
// with the root page of the corresponding table. The system catalog uses the
// page number 1.
func (kv *KV) Set(pageNumber int, key, value []byte) error {
	// TODO set doesn't differentiate between insert and update
	if pageNumber == 0 {
		panic("pageNumber cannot be 0")
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
	leafPage.SetTypeInternal()
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

func (kv *KV) getLeafPage(nextPageNumber int, key []byte) *pager.Page {
	p := kv.pager.GetPage(nextPageNumber)
	for !p.IsLeaf() {
		nextPage, found := p.GetValue(key)
		if !found {
			return nil
		}
		nextPageNumber16 := binary.LittleEndian.Uint16(nextPage)
		p = kv.pager.GetPage(int(nextPageNumber16))
	}
	return p
}

func (kv *KV) splitPage(page *pager.Page) (left, right *pager.Page) {
	hasParent, _ := page.GetParentPageNumber()
	_, parentLeftPageNumber := page.GetLeftPageNumber()
	_, parentRightPageNumber := page.GetRightPageNumber()
	parentType := page.GetType()
	entries := page.GetEntries()
	// If it is splitting the root page should make two new nodes so the
	// root can keep the same page number. Otherwise will only need to split
	// into one new node and also use the existing node.
	leftPage := page
	if !hasParent {
		leftPage = kv.pager.NewPage()
	}
	leftEntries := entries[:len(entries)/2]
	leftPage.SetEntries(leftEntries)
	leftPage.SetType(parentType)
	rightPage := kv.pager.NewPage()
	rightEntries := entries[len(entries)/2:]
	rightPage.SetEntries(rightEntries)
	rightPage.SetType(parentType)
	// Set relative left page's right page
	if parentLeftPageNumber != 0 {
		kv.pager.GetPage(parentLeftPageNumber).SetRightPageNumber(leftPage.GetNumber())
	}
	// Set split left's left and right
	leftPage.SetLeftPageNumber(parentLeftPageNumber)
	leftPage.SetRightPageNumber(rightPage.GetNumber())
	// Set split right's left and right
	rightPage.SetLeftPageNumber(leftPage.GetNumber())
	rightPage.SetRightPageNumber(parentRightPageNumber)
	// Set relative right page's left page
	if parentRightPageNumber != 0 {
		kv.pager.GetPage(parentRightPageNumber).SetLeftPageNumber(rightPage.GetNumber())
	}
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
	p.SetTypeInternal()
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
	return np.GetNumber()
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
	candidate := kv.pager.GetPage(rootPageNumber)
	if len(candidate.GetEntries()) == 0 {
		return 1, nil
	}
	for !candidate.IsLeaf() {
		pagePointers := candidate.GetEntries()
		descendingPageNum := pagePointers[len(pagePointers)-1].Value
		descendingPageNum16 := binary.LittleEndian.Uint16(descendingPageNum)
		candidate = kv.pager.GetPage(int(descendingPageNum16))
	}
	k := candidate.GetEntries()[len(candidate.GetEntries())-1].Key
	dk, err := DecodeKey(k)
	if err != nil {
		return 0, err
	}
	dki, ok := dk.(int)
	if !ok {
		return 0, errors.New("non integer key increment not supported")
	}
	return dki + 1, nil
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
	rootPageNumber          int
	currentPageEntries      []pager.PageTuple
	currentTupleIndex       int
	currentLeftPage         int
	currentRightPage        int
	currentPageEntriesCount int
	pager                   *pager.Pager
}

func (kv *KV) NewCursor(rootPageNumber int) *Cursor {
	return &Cursor{
		rootPageNumber: rootPageNumber,
		pager:          kv.pager,
	}
}

func (c *Cursor) moveToPage(p *pager.Page) {
	c.currentPageEntries = p.GetEntries()
	c.currentTupleIndex = 0
	_, c.currentLeftPage = p.GetLeftPageNumber()
	_, c.currentRightPage = p.GetRightPageNumber()
	c.currentPageEntriesCount = p.GetRecordCount()
}

// GotoFirstRecord moves the cursor to the first tuple in ascending order. It
// returns true if the table has values. It returns false if the table is empty.
func (c *Cursor) GotoFirstRecord() bool {
	candidatePage := c.pager.GetPage(c.rootPageNumber)
	if len(candidatePage.GetEntries()) == 0 {
		return false
	}
	for !candidatePage.IsLeaf() {
		pagePointers := candidatePage.GetEntries()
		ascendingPageNum := pagePointers[0].Value
		ascendingPageNum16 := binary.LittleEndian.Uint16(ascendingPageNum)
		candidatePage = c.pager.GetPage(int(ascendingPageNum16))
	}
	c.moveToPage(candidatePage)
	return true
}

// GotoLastRecord moves the cursor to the last tuple in the last page
// (descending ordering). It returns true if the table has values otherwise
// false.
func (c *Cursor) GotoLastRecord() bool {
	candidatePage := c.pager.GetPage(c.rootPageNumber)
	if len(candidatePage.GetEntries()) == 0 {
		return false
	}
	for !candidatePage.IsLeaf() {
		pagePointers := candidatePage.GetEntries()
		descendingPageNum := pagePointers[len(pagePointers)-1].Value
		descendingPageNum16 := binary.LittleEndian.Uint16(descendingPageNum)
		candidatePage = c.pager.GetPage(int(descendingPageNum16))
	}
	c.moveToPage(candidatePage)
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
	if c.currentRightPage != 0 {
		candidatePage := c.pager.GetPage(c.currentRightPage)
		if len(candidatePage.GetEntries()) == 0 {
			return false
		}
		c.moveToPage(candidatePage)
		return true
	}
	return false
}

// GotoNextPage advances the cursor to the next page and returns true. If there
// is no next page it will not advance and will return false
func (c *Cursor) GotoNextPage() bool {
	if c.currentRightPage == 0 {
		return false
	}
	np := c.pager.GetPage(c.currentRightPage)
	c.moveToPage(np)
	return true
}

// Count returns the count of the current b trees leaf node entries.
//
// Count does this not by scanning each individual tuple, but scanning each page
// and summing the computed counter on the page.
func (c *Cursor) Count() int {
	hasValues := c.GotoFirstRecord()
	sum := 0
	if !hasValues {
		return sum
	}
	sum += c.currentPageEntriesCount
	for c.GotoNextPage() {
		sum += c.currentPageEntriesCount
	}
	return sum
}
