// KV provides a set of key value operations that implement data structures such
// as a b-tree to efficiently access the page cache.
package main

import (
	"encoding/binary"
	"log"
)

type kv struct {
	pager *pager
}

func NewKv(filename string) (*kv, error) {
	pager, err := newPager(filename)
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
func (kv *kv) Get(pageNumber uint16, key []byte) ([]byte, bool) {
	if pageNumber == EMPTY_PARENT_PAGE_NUMBER {
		// TODO likely should be returning and handling errors and not just
		// randomly failing.
		log.Fatal("specified a reserved page number")
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
			return page.getValue(key)
		}
		v, found := page.getValue(key)
		if !found {
			// TODO need to really be doing a search option when looking at an
			// internal node.
			return nil, false
		}
		// Step 3. If the page is internal jump to the next page and go back to
		// 2. This process guarantees that we are on a leaf page for step 4.
		pageNumber = binary.LittleEndian.Uint16(v)
	}
}

// Set inserts or updates the value for the given key. The pageNumber has to do
// with the root page of the corresponding table. The system catalog uses the
// page number 1.
func (kv *kv) Set(pageNumber uint16, key, value []byte) {
	if pageNumber == EMPTY_PARENT_PAGE_NUMBER {
		// TODO likely should be returning and handling errors and not just
		// randomly failing.
		log.Fatal("specified a reserved page number")
	}
	defer kv.pager.flush()
	leafPage := kv.getLeafPage(pageNumber, key)
	if leafPage.canInsertTuple(key, value) {
		leafPage.setValue(key, value)
		return
	}
	leftPage, rightPage := kv.splitPage(leafPage)
	hasParent, parentPageNumber := leafPage.getParentPageNumber()
	if hasParent {
		parentPage := kv.pager.getPage(parentPageNumber)
		kv.parentInsert(parentPage, leftPage, rightPage)
		return
	}
	newParent := kv.pager.newPage()
	newParent.setType(PAGE_TYPE_INTERNAL)
	newParent.setEntries([]pageTuple{
		{
			key:   leftPage.getEntries()[0].key,
			value: leftPage.getNumberAsBytes(),
		},
		{
			key:   rightPage.getEntries()[0].key,
			value: rightPage.getNumberAsBytes(),
		},
	})
	leftPage.setParentPageNumber(newParent.getNumber())
	rightPage.setParentPageNumber(newParent.getNumber())
}

func (kv *kv) getLeafPage(pageNumber uint16, key []byte) *page {
	leafPage := kv.pager.getPage(pageNumber)
	for leafPage.getType() != PAGE_TYPE_LEAF {
		// Increment to the next page. TODO This should likely not be
		// page.getValue(key), but should be something like page.search(key) in
		// order to get to a leaf node
		nextPage, found := leafPage.getValue(key)
		if !found {
			// TODO need to be searching not looking directly at key
			return nil
		}
		pageNumber = binary.LittleEndian.Uint16(nextPage)
		leafPage = kv.pager.getPage(pageNumber)
	}
	return leafPage
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
		// TODO need to do calculation to see what left or right is appropriate
		l.setParentPageNumber(leftPage.getNumber())
		// TODO need to do calculation to see what left or right is appropriate
		r.setParentPageNumber(rightPage.getNumber())

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
