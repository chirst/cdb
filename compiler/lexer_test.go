package compiler

import (
	"reflect"
	"testing"
)

type tc struct {
	sql      string
	expected []token
}

func TestLexSelect(t *testing.T) {
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
			sql: "select * from foo",
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
			sql: `
				select *
				from foo
			`,
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
				{NUMERIC, "1"},
			},
		},
		{
			sql: "SELECT 12",
			expected: []token{
				{KEYWORD, "SELECT"},
				{WHITESPACE, " "},
				{NUMERIC, "12"},
			},
		},
		{
			sql: "SELECT 1;",
			expected: []token{
				{KEYWORD, "SELECT"},
				{WHITESPACE, " "},
				{NUMERIC, "1"},
				{SEPARATOR, ";"},
			},
		},
	}
	for _, c := range cases {
		ret := NewLexer(c.sql).Lex()
		if !reflect.DeepEqual(ret, c.expected) {
			t.Errorf("expected %#v got %#v", c.expected, ret)
		}
	}
}

func TestLexCreate(t *testing.T) {
	cases := []tc{
		{
			sql: "CREATE TABLE foo (id INTEGER, first_name TEXT, last_name TEXT)",
			expected: []token{
				{KEYWORD, "CREATE"},
				{WHITESPACE, " "},
				{KEYWORD, "TABLE"},
				{WHITESPACE, " "},
				{IDENTIFIER, "foo"},
				{WHITESPACE, " "},
				{SEPARATOR, "("},
				{IDENTIFIER, "id"},
				{WHITESPACE, " "},
				{KEYWORD, "INTEGER"},
				{SEPARATOR, ","},
				{WHITESPACE, " "},
				{IDENTIFIER, "first_name"},
				{WHITESPACE, " "},
				{KEYWORD, "TEXT"},
				{SEPARATOR, ","},
				{WHITESPACE, " "},
				{IDENTIFIER, "last_name"},
				{WHITESPACE, " "},
				{KEYWORD, "TEXT"},
				{SEPARATOR, ")"},
			},
		},
	}
	for _, c := range cases {
		ret := NewLexer(c.sql).Lex()
		if !reflect.DeepEqual(ret, c.expected) {
			t.Errorf("expected %#v got %#v", c.expected, ret)
		}
	}
}

func TestLexInsert(t *testing.T) {
	cases := []tc{
		{
			sql: "INSERT INTO foo (id, first_name, last_name) VALUES (1, 'gud', 'dude')",
			expected: []token{
				{KEYWORD, "INSERT"},
				{WHITESPACE, " "},
				{KEYWORD, "INTO"},
				{WHITESPACE, " "},
				{IDENTIFIER, "foo"},
				{WHITESPACE, " "},
				{SEPARATOR, "("},
				{IDENTIFIER, "id"},
				{SEPARATOR, ","},
				{WHITESPACE, " "},
				{IDENTIFIER, "first_name"},
				{SEPARATOR, ","},
				{WHITESPACE, " "},
				{IDENTIFIER, "last_name"},
				{SEPARATOR, ")"},
				{WHITESPACE, " "},
				{KEYWORD, "VALUES"},
				{WHITESPACE, " "},
				{SEPARATOR, "("},
				{NUMERIC, "1"},
				{SEPARATOR, ","},
				{WHITESPACE, " "},
				{LITERAL, "'gud'"},
				{SEPARATOR, ","},
				{WHITESPACE, " "},
				{LITERAL, "'dude'"},
				{SEPARATOR, ")"},
			},
		},
	}
	for _, c := range cases {
		ret := NewLexer(c.sql).Lex()
		if !reflect.DeepEqual(ret, c.expected) {
			t.Errorf("expected %#v got %#v", c.expected, ret)
		}
	}
}
