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
				{tkOperator, "*"},
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
				{tkOperator, "*"},
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
				{tkOperator, "*"},
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
				{tkOperator, "*"},
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
			sql: "EXPLAIN QUERY PLAN SELECT 1",
			expected: []token{
				{tkKeyword, "EXPLAIN"},
				{tkWhitespace, " "},
				{tkKeyword, "QUERY"},
				{tkWhitespace, " "},
				{tkKeyword, "PLAN"},
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
		{
			sql: "SELECT foo.id FROM foo",
			expected: []token{
				{tkKeyword, "SELECT"},
				{tkWhitespace, " "},
				{tkIdentifier, "foo"},
				{tkSeparator, "."},
				{tkIdentifier, "id"},
				{tkWhitespace, " "},
				{tkKeyword, "FROM"},
				{tkWhitespace, " "},
				{tkIdentifier, "foo"},
			},
		},
		{
			sql: "SELECT foo.* FROM foo",
			expected: []token{
				{tkKeyword, "SELECT"},
				{tkWhitespace, " "},
				{tkIdentifier, "foo"},
				{tkSeparator, "."},
				{tkOperator, "*"},
				{tkWhitespace, " "},
				{tkKeyword, "FROM"},
				{tkWhitespace, " "},
				{tkIdentifier, "foo"},
			},
		},
		{
			sql: "SELECT 1 AS bar FROM foo",
			expected: []token{
				{tkKeyword, "SELECT"},
				{tkWhitespace, " "},
				{tkNumeric, "1"},
				{tkWhitespace, " "},
				{tkKeyword, "AS"},
				{tkWhitespace, " "},
				{tkIdentifier, "bar"},
				{tkWhitespace, " "},
				{tkKeyword, "FROM"},
				{tkWhitespace, " "},
				{tkIdentifier, "foo"},
			},
		},
		{
			sql: "SELECT 1 + 2 - 3 * 4 + 5 / 6 ^ 7 - 8 * 9",
			expected: []token{
				{tkKeyword, "SELECT"},
				{tkWhitespace, " "},
				{tkNumeric, "1"},
				{tkWhitespace, " "},
				{tkOperator, "+"},
				{tkWhitespace, " "},
				{tkNumeric, "2"},
				{tkWhitespace, " "},
				{tkOperator, "-"},
				{tkWhitespace, " "},
				{tkNumeric, "3"},
				{tkWhitespace, " "},
				{tkOperator, "*"},
				{tkWhitespace, " "},
				{tkNumeric, "4"},
				{tkWhitespace, " "},
				{tkOperator, "+"},
				{tkWhitespace, " "},
				{tkNumeric, "5"},
				{tkWhitespace, " "},
				{tkOperator, "/"},
				{tkWhitespace, " "},
				{tkNumeric, "6"},
				{tkWhitespace, " "},
				{tkOperator, "^"},
				{tkWhitespace, " "},
				{tkNumeric, "7"},
				{tkWhitespace, " "},
				{tkOperator, "-"},
				{tkWhitespace, " "},
				{tkNumeric, "8"},
				{tkWhitespace, " "},
				{tkOperator, "*"},
				{tkWhitespace, " "},
				{tkNumeric, "9"},
			},
		},
		{
			sql: "SELECT * FROM foo WHERE id = 1",
			expected: []token{
				{tkKeyword, "SELECT"},
				{tkWhitespace, " "},
				{tkOperator, "*"},
				{tkWhitespace, " "},
				{tkKeyword, "FROM"},
				{tkWhitespace, " "},
				{tkIdentifier, "foo"},
				{tkWhitespace, " "},
				{tkKeyword, "WHERE"},
				{tkWhitespace, " "},
				{tkIdentifier, "id"},
				{tkWhitespace, " "},
				{tkOperator, "="},
				{tkWhitespace, " "},
				{tkNumeric, "1"},
			},
		},
		{
			sql: "SELECT 1 WHERE 1 > 2",
			expected: []token{
				{tkKeyword, "SELECT"},
				{tkWhitespace, " "},
				{tkNumeric, "1"},
				{tkWhitespace, " "},
				{tkKeyword, "WHERE"},
				{tkWhitespace, " "},
				{tkNumeric, "1"},
				{tkWhitespace, " "},
				{tkOperator, ">"},
				{tkWhitespace, " "},
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
		{
			sql: "CREATE TABLE IF NOT EXISTS bar (id INTEGER);",
			expected: []token{
				{tkKeyword, "CREATE"},
				{tkWhitespace, " "},
				{tkKeyword, "TABLE"},
				{tkWhitespace, " "},
				{tkKeyword, "IF"},
				{tkWhitespace, " "},
				{tkKeyword, "NOT"},
				{tkWhitespace, " "},
				{tkKeyword, "EXISTS"},
				{tkWhitespace, " "},
				{tkIdentifier, "bar"},
				{tkWhitespace, " "},
				{tkSeparator, "("},
				{tkIdentifier, "id"},
				{tkWhitespace, " "},
				{tkKeyword, "INTEGER"},
				{tkSeparator, ")"},
				{tkSeparator, ";"},
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
		t.Run(c.sql, func(t *testing.T) {
			ret := NewLexer(c.sql).Lex()
			if !reflect.DeepEqual(ret, c.expected) {
				t.Errorf("expected %#v got %#v", c.expected, ret)
			}
		})
	}
}

func TestToStatements(t *testing.T) {
	type testCase struct {
		src         string
		expectedLen int
	}
	testCases := []testCase{
		{
			src:         "SELECT 1",
			expectedLen: 1,
		},
		{
			src:         "SELECT 1;",
			expectedLen: 1,
		},
		{
			src:         "SELECT 1;  ",
			expectedLen: 1,
		},
		{
			src:         "SELECT 1;  SELECT 1",
			expectedLen: 2,
		},
		{
			src:         "SELECT 1;  SELECT 1; ",
			expectedLen: 2,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.src, func(t *testing.T) {
			statements := NewLexer(tc.src).ToStatements()
			if gotLen := len(statements); gotLen != tc.expectedLen {
				t.Fatalf("expected %d statements but got %d", tc.expectedLen, gotLen)
			}
		})
	}
}

func TestIsTerminated(t *testing.T) {
	type testCase struct {
		src  string
		want bool
	}
	testCases := []testCase{
		{
			src:  "",
			want: false,
		},
		{
			src:  "SELECT 1",
			want: false,
		},
		{
			src:  "SELECT 1;",
			want: true,
		},
		{
			src:  "SELECT 1; ",
			want: true,
		},
		{
			src:  "SELECT 1;  ",
			want: true,
		},
		{
			src:  "SELECT 1;  SELECT",
			want: false,
		},
		{
			src:  "SELECT 1;  SELECT 1;",
			want: true,
		},
		{
			src:  "SELECT 1;  SELECT 1; ",
			want: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.src, func(t *testing.T) {
			statements := NewLexer(tc.src).ToStatements()
			if got := IsTerminated(statements); got != tc.want {
				t.Fatalf("want %t got %t", tc.want, got)
			}
		})
	}
}
