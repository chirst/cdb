package compiler

import (
	"reflect"
	"testing"
)

type tc struct {
	sql      string
	expected []Token
}

func TestLexSelect(t *testing.T) {
	cases := []tc{
		{
			sql: "SELECT * FROM foo",
			expected: []Token{
				{tkKeyword, "SELECT"},
				{TkWhitespace, " "},
				{tkOperator, "*"},
				{TkWhitespace, " "},
				{tkKeyword, "FROM"},
				{TkWhitespace, " "},
				{tkIdentifier, "foo"},
			},
		},
		{
			sql: "SELECT COUNT(*) FROM foo",
			expected: []Token{
				{tkKeyword, "SELECT"},
				{TkWhitespace, " "},
				{tkKeyword, "COUNT"},
				{tkSeparator, "("},
				{tkOperator, "*"},
				{tkSeparator, ")"},
				{TkWhitespace, " "},
				{tkKeyword, "FROM"},
				{TkWhitespace, " "},
				{tkIdentifier, "foo"},
			},
		},
		{
			sql: "select * from foo",
			expected: []Token{
				{tkKeyword, "SELECT"},
				{TkWhitespace, " "},
				{tkOperator, "*"},
				{TkWhitespace, " "},
				{tkKeyword, "FROM"},
				{TkWhitespace, " "},
				{tkIdentifier, "foo"},
			},
		},
		{
			sql: `
				select *
				from foo
			`,
			expected: []Token{
				{tkKeyword, "SELECT"},
				{TkWhitespace, " "},
				{tkOperator, "*"},
				{TkWhitespace, " "},
				{tkKeyword, "FROM"},
				{TkWhitespace, " "},
				{tkIdentifier, "foo"},
			},
		},
		{
			sql: "EXPLAIN SELECT 1",
			expected: []Token{
				{tkKeyword, "EXPLAIN"},
				{TkWhitespace, " "},
				{tkKeyword, "SELECT"},
				{TkWhitespace, " "},
				{tkNumeric, "1"},
			},
		},
		{
			sql: "EXPLAIN QUERY PLAN SELECT 1",
			expected: []Token{
				{tkKeyword, "EXPLAIN"},
				{TkWhitespace, " "},
				{tkKeyword, "QUERY"},
				{TkWhitespace, " "},
				{tkKeyword, "PLAN"},
				{TkWhitespace, " "},
				{tkKeyword, "SELECT"},
				{TkWhitespace, " "},
				{tkNumeric, "1"},
			},
		},
		{
			sql: "SELECT 12",
			expected: []Token{
				{tkKeyword, "SELECT"},
				{TkWhitespace, " "},
				{tkNumeric, "12"},
			},
		},
		{
			sql: "SELECT 1;",
			expected: []Token{
				{tkKeyword, "SELECT"},
				{TkWhitespace, " "},
				{tkNumeric, "1"},
				{tkSeparator, ";"},
			},
		},
		{
			sql: "SELECT foo.id FROM foo",
			expected: []Token{
				{tkKeyword, "SELECT"},
				{TkWhitespace, " "},
				{tkIdentifier, "foo"},
				{tkSeparator, "."},
				{tkIdentifier, "id"},
				{TkWhitespace, " "},
				{tkKeyword, "FROM"},
				{TkWhitespace, " "},
				{tkIdentifier, "foo"},
			},
		},
		{
			sql: "SELECT foo.* FROM foo",
			expected: []Token{
				{tkKeyword, "SELECT"},
				{TkWhitespace, " "},
				{tkIdentifier, "foo"},
				{tkSeparator, "."},
				{tkOperator, "*"},
				{TkWhitespace, " "},
				{tkKeyword, "FROM"},
				{TkWhitespace, " "},
				{tkIdentifier, "foo"},
			},
		},
		{
			sql: "SELECT 1 AS bar FROM foo",
			expected: []Token{
				{tkKeyword, "SELECT"},
				{TkWhitespace, " "},
				{tkNumeric, "1"},
				{TkWhitespace, " "},
				{tkKeyword, "AS"},
				{TkWhitespace, " "},
				{tkIdentifier, "bar"},
				{TkWhitespace, " "},
				{tkKeyword, "FROM"},
				{TkWhitespace, " "},
				{tkIdentifier, "foo"},
			},
		},
		{
			sql: "SELECT 1 + 2 - 3 * 4 + 5 / 6 ^ 7 - 8 * 9",
			expected: []Token{
				{tkKeyword, "SELECT"},
				{TkWhitespace, " "},
				{tkNumeric, "1"},
				{TkWhitespace, " "},
				{tkOperator, "+"},
				{TkWhitespace, " "},
				{tkNumeric, "2"},
				{TkWhitespace, " "},
				{tkOperator, "-"},
				{TkWhitespace, " "},
				{tkNumeric, "3"},
				{TkWhitespace, " "},
				{tkOperator, "*"},
				{TkWhitespace, " "},
				{tkNumeric, "4"},
				{TkWhitespace, " "},
				{tkOperator, "+"},
				{TkWhitespace, " "},
				{tkNumeric, "5"},
				{TkWhitespace, " "},
				{tkOperator, "/"},
				{TkWhitespace, " "},
				{tkNumeric, "6"},
				{TkWhitespace, " "},
				{tkOperator, "^"},
				{TkWhitespace, " "},
				{tkNumeric, "7"},
				{TkWhitespace, " "},
				{tkOperator, "-"},
				{TkWhitespace, " "},
				{tkNumeric, "8"},
				{TkWhitespace, " "},
				{tkOperator, "*"},
				{TkWhitespace, " "},
				{tkNumeric, "9"},
			},
		},
		{
			sql: "SELECT * FROM foo WHERE id = 1",
			expected: []Token{
				{tkKeyword, "SELECT"},
				{TkWhitespace, " "},
				{tkOperator, "*"},
				{TkWhitespace, " "},
				{tkKeyword, "FROM"},
				{TkWhitespace, " "},
				{tkIdentifier, "foo"},
				{TkWhitespace, " "},
				{tkKeyword, "WHERE"},
				{TkWhitespace, " "},
				{tkIdentifier, "id"},
				{TkWhitespace, " "},
				{tkOperator, "="},
				{TkWhitespace, " "},
				{tkNumeric, "1"},
			},
		},
		{
			sql: "SELECT 1 WHERE 1 > 2",
			expected: []Token{
				{tkKeyword, "SELECT"},
				{TkWhitespace, " "},
				{tkNumeric, "1"},
				{TkWhitespace, " "},
				{tkKeyword, "WHERE"},
				{TkWhitespace, " "},
				{tkNumeric, "1"},
				{TkWhitespace, " "},
				{tkOperator, ">"},
				{TkWhitespace, " "},
				{tkNumeric, "2"},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			ret := NewLexer(c.sql).Lex()
			if !reflect.DeepEqual(ret, c.expected) {
				t.Errorf("expected %#v got %#v", c.expected, ret)
			}
		})
	}
}

func TestLexCreate(t *testing.T) {
	cases := []tc{
		{
			sql: "CREATE TABLE foo (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, age INTEGER)",
			expected: []Token{
				{tkKeyword, "CREATE"},
				{TkWhitespace, " "},
				{tkKeyword, "TABLE"},
				{TkWhitespace, " "},
				{tkIdentifier, "foo"},
				{TkWhitespace, " "},
				{tkSeparator, "("},
				{tkIdentifier, "id"},
				{TkWhitespace, " "},
				{tkKeyword, "INTEGER"},
				{TkWhitespace, " "},
				{tkKeyword, "PRIMARY"},
				{TkWhitespace, " "},
				{tkKeyword, "KEY"},
				{tkSeparator, ","},
				{TkWhitespace, " "},
				{tkIdentifier, "first_name"},
				{TkWhitespace, " "},
				{tkKeyword, "TEXT"},
				{tkSeparator, ","},
				{TkWhitespace, " "},
				{tkIdentifier, "last_name"},
				{TkWhitespace, " "},
				{tkKeyword, "TEXT"},
				{tkSeparator, ","},
				{TkWhitespace, " "},
				{tkIdentifier, "age"},
				{TkWhitespace, " "},
				{tkKeyword, "INTEGER"},
				{tkSeparator, ")"},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			ret := NewLexer(c.sql).Lex()
			if !reflect.DeepEqual(ret, c.expected) {
				t.Errorf("expected %#v got %#v", c.expected, ret)
			}
		})
	}
}

func TestLexInsert(t *testing.T) {
	cases := []tc{
		{
			sql: "INSERT INTO foo (id, first_name, last_name) VALUES (1, 'gud', 'dude'), (2, 'joe', 'doe')",
			expected: []Token{
				{tkKeyword, "INSERT"},
				{TkWhitespace, " "},
				{tkKeyword, "INTO"},
				{TkWhitespace, " "},
				{tkIdentifier, "foo"},
				{TkWhitespace, " "},
				{tkSeparator, "("},
				{tkIdentifier, "id"},
				{tkSeparator, ","},
				{TkWhitespace, " "},
				{tkIdentifier, "first_name"},
				{tkSeparator, ","},
				{TkWhitespace, " "},
				{tkIdentifier, "last_name"},
				{tkSeparator, ")"},
				{TkWhitespace, " "},
				{tkKeyword, "VALUES"},
				{TkWhitespace, " "},
				{tkSeparator, "("},
				{tkNumeric, "1"},
				{tkSeparator, ","},
				{TkWhitespace, " "},
				{tkLiteral, "'gud'"},
				{tkSeparator, ","},
				{TkWhitespace, " "},
				{tkLiteral, "'dude'"},
				{tkSeparator, ")"},
				{tkSeparator, ","},
				{TkWhitespace, " "},
				{tkSeparator, "("},
				{tkNumeric, "2"},
				{tkSeparator, ","},
				{TkWhitespace, " "},
				{tkLiteral, "'joe'"},
				{tkSeparator, ","},
				{TkWhitespace, " "},
				{tkLiteral, "'doe'"},
				{tkSeparator, ")"},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			ret := NewLexer(c.sql).Lex()
			if !reflect.DeepEqual(ret, c.expected) {
				t.Errorf("expected %#v got %#v", c.expected, ret)
			}
		})
	}
}
