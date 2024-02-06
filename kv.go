// KV provides a set of key value operations that implement data structures such
// as a b-tree to efficiently access the page cache.
package main

import (
	"encoding/binary"
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
// corresponding table. The system catalog uses the page number 0.
func (kv *kv) Get(pageNumber uint16, key []byte) ([]byte, bool) {
	// Step 1. Need a source page to start from. Will start from 0 if there is
	// no source page specified. This source page has to do with a table. 0 has
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
			return nil, false
		}
		// Step 3. If the page is internal jump to the next page and go back to
		// 2. This process guarantees that we are on a leaf page for step 4.
		pageNumber = binary.LittleEndian.Uint16(v)
	}
}

// Set inserts or updates the value for the given key. The pageNumber has to do
// with the root page of the corresponding table. The system catalog uses the
// page number 0.
func (kv *kv) Set(pageNumber uint16, key, value []byte) {
	leafPage := kv.getLeafPage(pageNumber, key)
	if leafPage.canInsertTuple(key, value) {
		leafPage.setValue(key, value)
		return
	}
	leftPage, rightPage := kv.splitPage(leafPage)
	hasParent, parentPageNumber := leafPage.getParentPageNumber()
	if hasParent {
		parentPage := kv.pager.getPage(parentPageNumber)
		kv.parentInsert(
			parentPage,
			leftPage.getEntries()[0].key,
			leftPage.getNumberAsBytes(),
			rightPage.getEntries()[0].key,
			rightPage.getNumberAsBytes(),
		)
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
	return nil
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

func (kv *kv) parentInsert(p *page, k1, v1, k2, v2 []byte) {
	tuples := []pageTuple{{key: k1, value: v1}, {key: k2, value: v2}}
	if p.canInsertTuples(tuples) {
		p.setValue(k1, v1)
		p.setValue(k2, v2)
		return
	}
	leftPage, rightPage := kv.splitPage(p)
	hasParent, parentPageNumber := p.getParentPageNumber()
	if hasParent {
		parentParent := kv.pager.getPage(parentPageNumber)
		kv.parentInsert(
			parentParent,
			leftPage.getEntries()[0].key,
			leftPage.getNumberAsBytes(),
			rightPage.getEntries()[0].key,
			rightPage.getNumberAsBytes(),
		)
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
}
