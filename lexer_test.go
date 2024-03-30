package main

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
				{SPACE, " "},
				{ASTERISK, "*"},
				{SPACE, " "},
				{KEYWORD, "FROM"},
				{SPACE, " "},
				{IDENTIFIER, "foo"},
			},
		},
		{
			sql: "SELECT 1",
			expected: []token{
				{KEYWORD, "SELECT"},
				{SPACE, " "},
				{IDENTIFIER, "1"},
			},
		},
		{
			sql: "SELECT 12",
			expected: []token{
				{KEYWORD, "SELECT"},
				{SPACE, " "},
				{IDENTIFIER, "12"},
			},
		},
		{
			sql: "SELECT 1;",
			expected: []token{
				{KEYWORD, "SELECT"},
				{SPACE, " "},
				{IDENTIFIER, "1"},
				{SEMI, ";"},
			},
		},
	}
	for _, c := range cases {
		l := &lexer{
			src: c.sql,
		}
		ret := l.Lex()
		if !reflect.DeepEqual(ret, c.expected) {
			t.Errorf("expected %#v got %#v", c.expected, ret)
		}
	}
}
