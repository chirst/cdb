// KV provides a set of key value operations that implement data structures such
// as a b-tree to efficiently access the page cache. KV implements an
// abstraction called a cursor to efficiently seek and scan the btree. Each B
// tree is referenced with a page number assigned by the catalog.
package kv

import (
	"bytes"
	"encoding/binary"
	"log"

	"github.com/chirst/cdb/pager"
)

// KV is an abstraction on the pager module that provides efficient reads and
// writes through b tree indexes.
type KV struct {
	pager   *pager.Pager
	catalog *catalog
}

// New creates an instance of kv
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

// NewBTree creates an empty BTree and returns the new tree's root page number.
func (kv *KV) NewBTree() int {
	np := kv.pager.NewPage()
	return np.GetNumber()
}

// BeginReadTransaction begins a read transaction.
func (kv *KV) BeginReadTransaction() {
	kv.pager.BeginRead()
}

// EndReadTransaction ends a read transaction.
func (kv *KV) EndReadTransaction() {
	kv.pager.EndRead()
}

// BeginWriteTransaction begins a write transaction.
func (kv *KV) BeginWriteTransaction() {
	kv.pager.BeginWrite()
}

// RollbackWrite rolls back and ends a write transaction.
func (kv *KV) RollbackWrite() {
	kv.pager.RollbackWrite()
}

// EndWriteTransaction ends a write transaction.
func (kv *KV) EndWriteTransaction() error {
	return kv.pager.EndWrite()
}

// ParseSchema updates the system catalog by reading the schema table.
func (kv *KV) ParseSchema() error {
	c := kv.NewCursor(1)
	exists := c.GotoFirstRecord()
	if !exists {
		return nil
	}
	var objects []object
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
		objects = append(objects, *o)
		exists = c.GotoNext()
	}
	kv.catalog.setSchema(objects)
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

// NewCursor creates a cursor the given object's rootPageNumber.
func (kv *KV) NewCursor(rootPageNumber int) *Cursor {
	if rootPageNumber == 0 {
		panic("root page cannot be 0")
	}
	return &Cursor{
		rootPageNumber: rootPageNumber,
		pager:          kv.pager,
	}
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

func (c *Cursor) moveToPage(p *pager.Page) {
	c.currentPageEntries = p.GetEntries()
	c.currentTupleIndex = 0
	_, c.currentLeftPage = p.GetLeftPageNumber()
	_, c.currentRightPage = p.GetRightPageNumber()
	c.currentPageEntriesCount = p.GetRecordCount()
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

// Exists will probe the specified key and return true or false if the key
// exists or not.
func (c *Cursor) Exists(key []byte) bool {
	pageNumber := c.rootPageNumber
	for {
		page := c.pager.GetPage(pageNumber)
		v, found := page.GetValue(key)
		if page.IsLeaf() {
			return found
		}
		if !found {
			return false
		}
		pageNumber = int(binary.LittleEndian.Uint16(v))
	}
}

// NewRowID returns the highest unused key in a table for the rootPageNumber.
// For a integer key it is the largest integer key plus one.
func (c *Cursor) NewRowID() int {
	// TODO could possibly cache this in the catalog or on the cursor
	candidate := c.pager.GetPage(c.rootPageNumber)
	if len(candidate.GetEntries()) == 0 {
		return 1
	}
	for !candidate.IsLeaf() {
		pagePointers := candidate.GetEntries()
		descendingPageNum := pagePointers[len(pagePointers)-1].Value
		descendingPageNum16 := binary.LittleEndian.Uint16(descendingPageNum)
		candidate = c.pager.GetPage(int(descendingPageNum16))
	}
	k := candidate.GetEntries()[len(candidate.GetEntries())-1].Key
	dk, err := DecodeKey(k)
	if err != nil {
		log.Fatal(err)
	}
	dki, ok := dk.(int)
	if !ok {
		log.Fatal("non integer key increment not supported")
	}
	return dki + 1
}

// Get returns a byte array corresponding to the key and a bool indicating if
// the key was found. The pageNumber has to do with the root page of the
// corresponding table. The system catalog uses the page number 1.
func (c *Cursor) Get(key []byte) ([]byte, bool) {
	pageNumber := c.rootPageNumber
	for {
		page := c.pager.GetPage(pageNumber)
		v, found := page.GetValue(key)
		if page.IsLeaf() {
			return v, found
		}
		if !found {
			return nil, false
		}
		pageNumber = int(binary.LittleEndian.Uint16(v))
	}
}

// Set inserts or updates the value for the given key. The pageNumber has to do
// with the root page of the corresponding table. The system catalog uses the
// page number 1.
func (c *Cursor) Set(key, value []byte) {
	// TODO set doesn't differentiate between insert and update
	leafPage := c.getLeafPage(c.rootPageNumber, key)
	if leafPage.CanInsertTuple(key, value) {
		leafPage.SetValue(key, value)
		return
	}
	leftPage, rightPage := c.splitPage(leafPage)
	c.insertIntoOne(key, value, leftPage, rightPage)
	hasParent, parentPageNumber := leafPage.GetParentPageNumber()
	if hasParent {
		parentPage := c.pager.GetPage(parentPageNumber)
		c.parentInsert(parentPage, leftPage, rightPage)
		return
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
}

// insertIntoOne is a helper function to insert into a left or right page given
// the left and right pages have space and the right page is greater than the
// left.
func (c *Cursor) insertIntoOne(key, value []byte, lp, rp *pager.Page) {
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

func (c *Cursor) getLeafPage(nextPageNumber int, key []byte) *pager.Page {
	p := c.pager.GetPage(nextPageNumber)
	for !p.IsLeaf() {
		nextPage, found := p.GetValue(key)
		if !found {
			return nil
		}
		nextPageNumber16 := binary.LittleEndian.Uint16(nextPage)
		p = c.pager.GetPage(int(nextPageNumber16))
	}
	return p
}

func (c *Cursor) splitPage(page *pager.Page) (left, right *pager.Page) {
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
		leftPage = c.pager.NewPage()
	}
	leftEntries := entries[:len(entries)/2]
	leftPage.SetEntries(leftEntries)
	leftPage.SetType(parentType)
	rightPage := c.pager.NewPage()
	rightEntries := entries[len(entries)/2:]
	rightPage.SetEntries(rightEntries)
	rightPage.SetType(parentType)
	// Set relative left page's right page
	if parentLeftPageNumber != 0 {
		c.pager.GetPage(parentLeftPageNumber).SetRightPageNumber(leftPage.GetNumber())
	}
	// Set split left's left and right
	leftPage.SetLeftPageNumber(parentLeftPageNumber)
	leftPage.SetRightPageNumber(rightPage.GetNumber())
	// Set split right's left and right
	rightPage.SetLeftPageNumber(leftPage.GetNumber())
	rightPage.SetRightPageNumber(parentRightPageNumber)
	// Set relative right page's left page
	if parentRightPageNumber != 0 {
		c.pager.GetPage(parentRightPageNumber).SetLeftPageNumber(rightPage.GetNumber())
	}
	return leftPage, rightPage
}

func (c *Cursor) parentInsert(p, l, r *pager.Page) {
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
	leftPage, rightPage := c.splitPage(p)
	hasParent, parentPageNumber := p.GetParentPageNumber()
	if hasParent {
		l.SetParentPageNumber(parentPageNumber)
		r.SetParentPageNumber(parentPageNumber)
		parentParent := c.pager.GetPage(parentPageNumber)
		c.parentInsert(parentParent, leftPage, rightPage)
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
