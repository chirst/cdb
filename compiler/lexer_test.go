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
				{tkKeyword, "SELECT"},
				{tkWhitespace, " "},
				{tkPunctuator, "*"},
				{tkWhitespace, " "},
				{tkKeyword, "FROM"},
				{tkWhitespace, " "},
				{tkIdentifier, "foo"},
			},
		},
		{
			sql: "SELECT COUNT(*) FROM foo",
			expected: []token{
				{tkKeyword, "SELECT"},
				{tkWhitespace, " "},
				{tkKeyword, "COUNT"},
				{tkSeparator, "("},
				{tkPunctuator, "*"},
				{tkSeparator, ")"},
				{tkWhitespace, " "},
				{tkKeyword, "FROM"},
				{tkWhitespace, " "},
				{tkIdentifier, "foo"},
			},
		},
		{
			sql: "select * from foo",
			expected: []token{
				{tkKeyword, "SELECT"},
				{tkWhitespace, " "},
				{tkPunctuator, "*"},
				{tkWhitespace, " "},
				{tkKeyword, "FROM"},
				{tkWhitespace, " "},
				{tkIdentifier, "foo"},
			},
		},
		{
			sql: `
				select *
				from foo
			`,
			expected: []token{
				{tkKeyword, "SELECT"},
				{tkWhitespace, " "},
				{tkPunctuator, "*"},
				{tkWhitespace, " "},
				{tkKeyword, "FROM"},
				{tkWhitespace, " "},
				{tkIdentifier, "foo"},
			},
		},
		{
			sql: "EXPLAIN SELECT 1",
			expected: []token{
				{tkKeyword, "EXPLAIN"},
				{tkWhitespace, " "},
				{tkKeyword, "SELECT"},
				{tkWhitespace, " "},
				{tkNumeric, "1"},
			},
		},
		{
			sql: "SELECT 12",
			expected: []token{
				{tkKeyword, "SELECT"},
				{tkWhitespace, " "},
				{tkNumeric, "12"},
			},
		},
		{
			sql: "SELECT 1;",
			expected: []token{
				{tkKeyword, "SELECT"},
				{tkWhitespace, " "},
				{tkNumeric, "1"},
				{tkSeparator, ";"},
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
			sql: "CREATE TABLE foo (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, age INTEGER)",
			expected: []token{
				{tkKeyword, "CREATE"},
				{tkWhitespace, " "},
				{tkKeyword, "TABLE"},
				{tkWhitespace, " "},
				{tkIdentifier, "foo"},
				{tkWhitespace, " "},
				{tkSeparator, "("},
				{tkIdentifier, "id"},
				{tkWhitespace, " "},
				{tkKeyword, "INTEGER"},
				{tkWhitespace, " "},
				{tkKeyword, "PRIMARY"},
				{tkWhitespace, " "},
				{tkKeyword, "KEY"},
				{tkSeparator, ","},
				{tkWhitespace, " "},
				{tkIdentifier, "first_name"},
				{tkWhitespace, " "},
				{tkKeyword, "TEXT"},
				{tkSeparator, ","},
				{tkWhitespace, " "},
				{tkIdentifier, "last_name"},
				{tkWhitespace, " "},
				{tkKeyword, "TEXT"},
				{tkSeparator, ","},
				{tkWhitespace, " "},
				{tkIdentifier, "age"},
				{tkWhitespace, " "},
				{tkKeyword, "INTEGER"},
				{tkSeparator, ")"},
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
			sql: "INSERT INTO foo (id, first_name, last_name) VALUES (1, 'gud', 'dude'), (2, 'joe', 'doe')",
			expected: []token{
				{tkKeyword, "INSERT"},
				{tkWhitespace, " "},
				{tkKeyword, "INTO"},
				{tkWhitespace, " "},
				{tkIdentifier, "foo"},
				{tkWhitespace, " "},
				{tkSeparator, "("},
				{tkIdentifier, "id"},
				{tkSeparator, ","},
				{tkWhitespace, " "},
				{tkIdentifier, "first_name"},
				{tkSeparator, ","},
				{tkWhitespace, " "},
				{tkIdentifier, "last_name"},
				{tkSeparator, ")"},
				{tkWhitespace, " "},
				{tkKeyword, "VALUES"},
				{tkWhitespace, " "},
				{tkSeparator, "("},
				{tkNumeric, "1"},
				{tkSeparator, ","},
				{tkWhitespace, " "},
				{tkLiteral, "'gud'"},
				{tkSeparator, ","},
				{tkWhitespace, " "},
				{tkLiteral, "'dude'"},
				{tkSeparator, ")"},
				{tkSeparator, ","},
				{tkWhitespace, " "},
				{tkSeparator, "("},
				{tkNumeric, "2"},
				{tkSeparator, ","},
				{tkWhitespace, " "},
				{tkLiteral, "'joe'"},
				{tkSeparator, ","},
				{tkWhitespace, " "},
				{tkLiteral, "'doe'"},
				{tkSeparator, ")"},
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
