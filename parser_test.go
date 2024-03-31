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
				{KEYWORD, "EXPLAIN"},
				{WHITESPACE, " "},
				{KEYWORD, "SELECT"},
				{WHITESPACE, " "},
				{PUNCTUATOR, "*"},
				{WHITESPACE, " "},
				{KEYWORD, "FROM"},
				{WHITESPACE, " "},
				{IDENTIFIER, "foo"},
			},
			expect: stmtList{&selectStmt{
				explain: true,
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
		{
			input: []token{
				{KEYWORD, "EXPLAIN"},
				{WHITESPACE, " "},
				{KEYWORD, "SELECT"},
				{WHITESPACE, " "},
				{LITERAL, "1"},
			},
			expect: stmtList{&selectStmt{
				explain: true,
				resultColumns: []resultColumn{
					{
						all: false,
						expr: &expr{
							literal: &literal{
								numericLiteral: 1,
							},
						},
					},
				},
			}},
		},
	}
	for _, c := range cases {
		p := parser{
			tokens: c.input,
		}
		ret, err := p.parse()
		if err != nil {
			t.Errorf("want no err got err %s", err.Error())
		}
		if !reflect.DeepEqual(ret, c.expect) {
			t.Errorf("got %#v want %#v", ret, c.expect)
		}
	}
}
