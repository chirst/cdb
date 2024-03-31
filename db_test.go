package main

import (
	"reflect"
	"testing"
)

func TestExecute(t *testing.T) {
	input := "EXPLAIN SELECT 1;"
	expectation := []executeResult{
		{
			text: "asdf",
		},
	}
	db := newDb()
	res := db.execute(input)
	if !reflect.DeepEqual(res, expectation) {
		t.Errorf("got %#v want %#v", res, expectation)
	}
}
