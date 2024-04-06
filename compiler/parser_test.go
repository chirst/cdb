package compiler

import (
	"reflect"
	"testing"
)

type selectTestCase struct {
	input  []token
	expect StmtList
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
			expect: StmtList{&SelectStmt{
				StmtBase: &StmtBase{
					Explain: true,
				},
				From: &From{
					TableName: "foo",
				},
				ResultColumns: []ResultColumn{
					{
						All: true,
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
			expect: StmtList{&SelectStmt{
				StmtBase: &StmtBase{
					Explain: true,
				},
				ResultColumns: []ResultColumn{
					{
						All: false,
						Expr: &Expr{
							Literal: &Literal{
								NumericLiteral: 1,
							},
						},
					},
				},
			}},
		},
	}
	for _, c := range cases {
		p := NewParser(c.input)
		ret, err := p.Parse()
		if err != nil {
			t.Errorf("want no err got err %s", err.Error())
		}
		if !reflect.DeepEqual(ret, c.expect) {
			t.Errorf("got %#v want %#v", ret, c.expect)
		}
	}
}
