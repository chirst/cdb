package compiler

import (
	"reflect"
	"testing"
)

type selectTestCase struct {
	tokens []token
	expect Stmt
}

func TestParseSelect(t *testing.T) {
	cases := []selectTestCase{
		{
			tokens: []token{
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
			expect: &SelectStmt{
				StmtBase: &StmtBase{
					Explain: true,
				},
				From: &From{
					TableName: "foo",
				},
				ResultColumn: ResultColumn{
					All: true,
				},
			},
		},
	}
	for _, c := range cases {
		ret, err := NewParser(c.tokens).Parse()
		if err != nil {
			t.Errorf("want no err got err %s", err.Error())
		}
		if !reflect.DeepEqual(ret, c.expect) {
			t.Errorf("got %#v want %#v", ret, c.expect)
		}
	}
}

type createTestCase struct {
	tokens   []token
	expected Stmt
}

func TestParseCreate(t *testing.T) {
	cases := []createTestCase{
		{
			tokens: []token{
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
			expected: &CreateStmt{
				StmtBase: &StmtBase{
					Explain: false,
				},
				TableName: "foo",
				ColDefs: []ColDef{
					{
						ColName: "id",
						ColType: "INTEGER",
					},
					{
						ColName: "first_name",
						ColType: "TEXT",
					},
					{
						ColName: "last_name",
						ColType: "TEXT",
					},
				},
			},
		},
	}
	for _, c := range cases {
		ret, err := NewParser(c.tokens).Parse()
		if err != nil {
			t.Errorf("expected no err got err %s", err.Error())
		}
		if !reflect.DeepEqual(ret, c.expected) {
			t.Errorf("expected %#v got %#v", c.expected, ret)
		}
	}
}

type insertTestCase struct {
	tokens   []token
	expected Stmt
}

func TestParseInsert(t *testing.T) {
	cases := []insertTestCase{
		{
			tokens: []token{
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
			expected: &InsertStmt{
				StmtBase: &StmtBase{
					Explain: false,
				},
				TableName: "foo",
				ColNames: []string{
					"id",
					"first_name",
					"last_name",
				},
				ColValues: []string{
					"1",
					"'gud'",
					"'dude'",
				},
			},
		},
	}
	for _, c := range cases {
		ret, err := NewParser(c.tokens).Parse()
		if err != nil {
			t.Errorf("expected no err got err %s", err.Error())
		}
		if !reflect.DeepEqual(ret, c.expected) {
			t.Errorf("expected %#v got %#v", c.expected, ret)
		}
	}
}
