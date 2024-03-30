package main

import (
	"reflect"
	"testing"
)

type selectTestCase struct {
	input  []token
	expect stmtList
}

func TestParseSelect(t *testing.T) {
	cases := []selectTestCase{
		{
			input: []token{
				{KEYWORD, "SELECT"},
				{SPACE, " "},
				{ASTERISK, "*"},
				{SPACE, " "},
				{KEYWORD, "FROM"},
				{SPACE, " "},
				{IDENTIFIER, "foo"},
			},
			expect: stmtList{selectStmt{
				from: &tableOrSubQuery{
					tableName: "foo",
				},
				resultColumns: []resultColumn{
					{
						all: true,
					},
				},
			}},
		},
	}
	for _, c := range cases {
		p := parser{
			tokens: c.input,
		}
		ret := p.parse()
		if !reflect.DeepEqual(ret, c.expect) {
			t.Errorf("got %#v want %#v", ret, c.expect)
		}
	}
}
