// KV provides a set of key value operations that implement data structures such
// as a b-tree to efficiently access the page cache. KV implements an
// abstraction called a cursor to efficiently seek and scan the btree. Each B
// tree is referenced with a page number assigned by the catalog.
package kv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"slices"

	"github.com/chirst/cdb/catalog"
	"github.com/chirst/cdb/pager"
)

// KV is an abstraction on the pager module that provides efficient reads and
// writes through b tree indexes.
type KV struct {
	pager   *pager.Pager
	catalog *catalog.Catalog
}

// New creates an instance of kv
func New(useMemory bool, filename string) (*KV, error) {
	pager, err := pager.New(useMemory, filename)
	if err != nil {
		return nil, err
	}
	catalog := catalog.NewCatalog()
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

// GetCatalog returns and instance of the system catalog.
func (kv *KV) GetCatalog() *catalog.Catalog {
	return kv.catalog
}

// NewBTree creates an empty BTree and returns the new tree's root page number.
func (kv *KV) NewBTree() int {
	np := kv.pager.NewPage()
	return np.GetNumber()
}

// BeginReadTransaction begins a read transaction.
func (kv *KV) BeginReadTransaction() error {
	return kv.pager.BeginRead()
}

// EndReadTransaction ends a read transaction.
func (kv *KV) EndReadTransaction() {
	kv.pager.EndRead()
}

// BeginWriteTransaction begins a write transaction.
func (kv *KV) BeginWriteTransaction() error {
	return kv.pager.BeginWrite()
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
	var objects []catalog.Object
	for exists {
		v := c.GetValue()
		dv, err := Decode(v)
		if err != nil {
			return err
		}
		o := &catalog.Object{
			ObjectType:     dv[0].(string),
			Name:           dv[1].(string),
			TableName:      dv[2].(string),
			RootPageNumber: dv[3].(int),
			JsonSchema:     dv[4].(string),
		}
		objects = append(objects, *o)
		exists = c.GotoNext()
	}
	kv.catalog.SetSchema(objects)
	return nil
}

// nextBehavior is the state of GotoNext in relation to DeleteCurrent
type nextBehavior int

const (
	// When GotoNext is unaffected by DeleteCurrent.
	nextBehaviorNormal nextBehavior = 0
	// When GotoNext should return true as if it moved to the next tuple.
	nextBehaviorNext nextBehavior = 1
	// When GotoNext should return false as if it ran out of tuple to move to.
	nextBehaviorEmpty nextBehavior = 2
)

// Cursor is an abstraction that can seek and scan ranges of a btree.
type Cursor struct {
	// rootPageNumber is the object this cursor operates on
	rootPageNumber int
	// currentTupleKey is the current tuple being pointed to
	currentTupleKey []byte
	// currentPage is the current page the cursor is pointing to
	currentPage *pager.Page
	// pager is the cursors pager
	pager *pager.Pager
	// nextBehavior is the state of GotoNext behavior for the cursor
	nextBehavior nextBehavior
}

// NewCursor creates a cursor with the given object's rootPageNumber.
func (kv *KV) NewCursor(rootPageNumber int) *Cursor {
	if rootPageNumber == 0 {
		panic("root page cannot be 0")
	}
	return &Cursor{
		rootPageNumber: rootPageNumber,
		pager:          kv.pager,
	}
}

// getCurrentEntriesIndex gets the index of the currentKey within the pages
// current entries. Note a special value of -1 is returned in the rare case
// the current key doesn't exist.
func (c *Cursor) getCurrentEntriesIndex() int {
	return slices.IndexFunc(
		c.currentPage.GetEntries(),
		func(t pager.PageTuple) bool {
			return bytes.Equal(t.Key, c.currentTupleKey)
		},
	)
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
		ascendingPageNum32 := binary.LittleEndian.Uint32(ascendingPageNum)
		candidatePage = c.pager.GetPage(int(ascendingPageNum32))
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
		descendingPageNum32 := binary.LittleEndian.Uint32(descendingPageNum)
		candidatePage = c.pager.GetPage(int(descendingPageNum32))
	}
	c.moveToPage(candidatePage)
	entries := c.currentPage.GetEntries()
	c.currentTupleKey = entries[len(entries)-1].Key
	return true
}

func (c *Cursor) GotoKey(key []byte) bool {
	candidatePage := c.pager.GetPage(c.rootPageNumber)
	for !candidatePage.IsLeaf() {
		v, exists := candidatePage.GetValue(key)
		if !exists {
			return false
		}
		nextPageNumber := int(binary.LittleEndian.Uint32(v))
		candidatePage = c.pager.GetPage(nextPageNumber)
	}
	c.moveToPage(candidatePage)
	entries := c.currentPage.GetEntries()
	for i, e := range entries {
		if bytes.Equal(e.Key, key) {
			c.currentTupleKey = entries[i].Key
			return true
		}
	}
	return false
}

// GetKey returns the key of the current tuple.
func (c *Cursor) GetKey() []byte {
	return c.currentTupleKey
}

// GetValue returns the value of the current pointed to tuple
func (c *Cursor) GetValue() []byte {
	v, _ := c.currentPage.GetValue(c.currentTupleKey)
	return v
}

// DeleteCurrent deletes the current tuple the cursor is pointing to. This
// leaves the cursor pointing at the next tuple in which case calling GotoNext
// will be a no-op. If the next tuple is the end of the table GotoNext will also
// be aware of this. This is all to facilitate execution plans which delete in a
// loop.
func (c *Cursor) DeleteCurrent() {
	newEntries := []pager.PageTuple{}
	var nextKey []byte
	foundNextKey := false
	// Gather entries besides the deleted current from the current page. Also,
	// find the next highest key.
	for _, e := range c.currentPage.GetEntries() {
		comparison := bytes.Compare(e.Key, c.currentTupleKey)
		if comparison != 0 { // a != b
			newEntries = append(newEntries, e)
		}
		if comparison == 1 && !foundNextKey { // a > b
			foundNextKey = true
			nextKey = e.Key
		}
	}
	// Set the current page entries minus the deleted entry.
	newPage := c.pager.GetPage(c.currentPage.GetNumber())
	newPage.SetEntries(newEntries)
	// Determine what the next key is and setup flag for GotoNext.
	if !foundNextKey {
		hasRight, rightPageNumber := c.currentPage.GetRightPageNumber()
		if hasRight {
			c.nextBehavior = nextBehaviorNext
			c.moveToPage(c.pager.GetPage(rightPageNumber))
		} else {
			c.nextBehavior = nextBehaviorEmpty
		}
	} else {
		c.nextBehavior = nextBehaviorNext
		c.currentTupleKey = nextKey
	}
}

// GotoNext moves the cursor to the next tuple in ascending order. If there is
// no next tuple this function will return false otherwise it will return true.
//
// GotoNext may have special behavior if a DeleteCurrent has been previously
// invoked. GotoNext will essentially be a noop that makes the illusion it did
// the job that was already done by DeleteCurrent.
func (c *Cursor) GotoNext() bool {
	switch c.nextBehavior {
	case nextBehaviorEmpty:
		c.nextBehavior = nextBehaviorNormal
		return false
	case nextBehaviorNext:
		c.nextBehavior = nextBehaviorNormal
		return true
	case nextBehaviorNormal:
		currentIndex := c.getCurrentEntriesIndex()
		if currentIndex+1 <= len(c.currentPage.GetEntries())-1 {
			c.currentTupleKey = c.currentPage.GetEntries()[currentIndex+1].Key
			return true
		}
		if hasRight, rpn := c.currentPage.GetRightPageNumber(); hasRight {
			candidatePage := c.pager.GetPage(rpn)
			if len(candidatePage.GetEntries()) == 0 {
				return false
			}
			c.moveToPage(candidatePage)
			return true
		}
		return false
	default:
		panic(fmt.Sprintf("unexpected next behavior %d", c.nextBehavior))
	}
}

// gotoNextPage advances the cursor to the next page and returns true. If there
// is no next page it will not advance and will return false
func (c *Cursor) gotoNextPage() bool {
	hasRight, rightPageNumber := c.currentPage.GetRightPageNumber()
	if !hasRight {
		return false
	}
	np := c.pager.GetPage(rightPageNumber)
	c.moveToPage(np)
	return true
}

func (c *Cursor) moveToPage(p *pager.Page) {
	c.currentTupleKey = p.GetEntries()[0].Key
	c.currentPage = p
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
	sum += len(c.currentPage.GetEntries())
	for c.gotoNextPage() {
		sum += len(c.currentPage.GetEntries())
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
		pageNumber = int(binary.LittleEndian.Uint32(v))
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
		descendingPageNum32 := binary.LittleEndian.Uint32(descendingPageNum)
		candidate = c.pager.GetPage(int(descendingPageNum32))
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
	// TODO improve interface to move the cursor instead of a one time point
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
		pageNumber = int(binary.LittleEndian.Uint32(v))
	}
}

// Set inserts or updates the value for the given key. The pageNumber has to do
// with the root page of the corresponding table. The system catalog uses the
// page number 1.
func (c *Cursor) Set(key, value []byte) {
	// Find leaf page with key as the search param.
	leafPage := c.getLeafPage(c.rootPageNumber, key)
	// If the leaf page can hold the new tuple be done.
	if leafPage.CanInsertTuple(key, value) {
		leafPage.SetValue(key, value)
		return
	}
	// Split page when the leaf cannot hold the tuple.
	leftPage, rightPage := c.splitPage(leafPage)
	// Find which page out of the split can best hold the tuple.
	c.insertIntoOne(key, value, leftPage, rightPage)
	// Having a parent means the parent must have the new pages inserted.
	hasParent, parentPageNumber := leafPage.GetParentPageNumber()
	if hasParent {
		leftPage.SetParentPageNumber(parentPageNumber)
		rightPage.SetParentPageNumber(parentPageNumber)
		parentPage := c.pager.GetPage(parentPageNumber)
		c.parentInsert(parentPage, leftPage, rightPage)
		return
	}
	// Falling through to here means there is no parent of the split so the root
	// node has split. This is a special optimization to keep the root page
	// number the same.
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
		nextPageNumber32 := binary.LittleEndian.Uint32(nextPage)
		p = c.pager.GetPage(int(nextPageNumber32))
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

// parentInsert is new left and right pointers needing to be inserted into the
// parent. This means the parent may need to be split and inserted into its
// parent and so on.
func (c *Cursor) parentInsert(p, l, r *pager.Page) {
	// k1/v1 and k2/v2 are the new page pointers. These will go in the parent
	// node.
	k1 := l.GetEntries()[0].Key
	v1 := l.GetNumberAsBytes()
	k2 := r.GetEntries()[0].Key
	v2 := r.GetNumberAsBytes()
	tuples := []pager.PageTuple{{Key: k1, Value: v1}, {Key: k2, Value: v2}}
	// If the parent is able to insert the page pointers we are done.
	if p.CanInsertTuples(tuples) {
		p.SetValue(k1, v1)
		p.SetValue(k2, v2)
		l.SetParentPageNumber(p.GetNumber())
		r.SetParentPageNumber(p.GetNumber())
		return
	}
	// This case is the parent needing to be split. We then check if the parents
	// parent is there or not. In case it is there we can make a recursive call.
	// In case it is not we fall through.
	leftPage, rightPage := c.splitPage(p)
	c.insertIntoOne(k1, v1, leftPage, rightPage)
	c.insertIntoOne(k2, v2, leftPage, rightPage)
	hasParent, parentPageNumber := p.GetParentPageNumber()
	if hasParent {
		leftPage.SetParentPageNumber(parentPageNumber)
		rightPage.SetParentPageNumber(parentPageNumber)
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
