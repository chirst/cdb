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
// the key was found.
func (kv *kv) Get(key []byte) ([]byte, bool) {
	// Step 1. Need a source page to start from. Will start from 0 if there is
	// no source page specified. This source page has to do with a table. 0 has
	// to be the system catalog.
	var pageNumber uint16 = 0
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

// Set inserts or updates the value for the given key.
func (kv *kv) Set(key, value []byte) {
	var pageNumber uint16 = 0
	leafPage := kv.getLeafPage(pageNumber, key)
	if !leafPage.canInsertTuple(key, value) {
		leftPage, rightPage := kv.splitPage(leafPage)
		hasParent, parentPageNumber := leafPage.getParentPageNumber()
		if hasParent {
			// TODO basically need to pull internal insert out of the pager
			// because the pager shouldn't know anything about a b tree or index
			// or whatever.
			parentPage := kv.pager.getPage(parentPageNumber)
			kv.parentInsert(
				parentPage,
				leftPage.getEntries()[0].key,
				leftPage.getNumberAsBytes(),
				rightPage.getEntries()[0].key,
				rightPage.getNumberAsBytes(),
			)
		} else {
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
		return
	}
	// No splitting needed just update or insert the value
	leafPage.setValue(key, value)
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
	// allocate left and right page and return the keys
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
	if !p.canInsertTuples([]pageTuple{
		{key: k1, value: v1},
		{key: k2, value: v2},
	}) {
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
		} else {
			// The root node needs to be split. It is wise to keep the root node
			// the same page so the table catalog doesn't need to be updated
			// every time a root node splits.
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
		return
	}
	p.setValue(k1, v1)
	p.setValue(k2, v2)
}
