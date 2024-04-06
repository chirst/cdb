package compiler

import (
	"reflect"
	"testing"
)

type tc struct {
	sql      string
	expected []token
}

func TestLexer(t *testing.T) {
	cases := []tc{
		{
			sql: "SELECT * FROM foo",
			expected: []token{
				{KEYWORD, "SELECT"},
				{WHITESPACE, " "},
				{PUNCTUATOR, "*"},
				{WHITESPACE, " "},
				{KEYWORD, "FROM"},
				{WHITESPACE, " "},
				{IDENTIFIER, "foo"},
			},
		},
		{
			sql: "EXPLAIN SELECT 1",
			expected: []token{
				{KEYWORD, "EXPLAIN"},
				{WHITESPACE, " "},
				{KEYWORD, "SELECT"},
				{WHITESPACE, " "},
				{LITERAL, "1"},
			},
		},
		{
			sql: "SELECT 12",
			expected: []token{
				{KEYWORD, "SELECT"},
				{WHITESPACE, " "},
				{LITERAL, "12"},
			},
		},
		{
			sql: "SELECT 1;",
			expected: []token{
				{KEYWORD, "SELECT"},
				{WHITESPACE, " "},
				{LITERAL, "1"},
				{SEPARATOR, ";"},
			},
		},
	}
	for _, c := range cases {
		l := NewLexer(c.sql)
		ret := l.Lex()
		if !reflect.DeepEqual(ret, c.expected) {
			t.Errorf("expected %#v got %#v", c.expected, ret)
		}
	}
}
