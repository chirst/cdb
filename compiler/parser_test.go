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
				{tkKeyword, "EXPLAIN"},
				{tkWhitespace, " "},
				{tkKeyword, "SELECT"},
				{tkWhitespace, " "},
				{tkPunctuator, "*"},
				{tkWhitespace, " "},
				{tkKeyword, "FROM"},
				{tkWhitespace, " "},
				{tkIdentifier, "foo"},
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
		{
			tokens: []token{
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
			expect: &SelectStmt{
				StmtBase: &StmtBase{
					Explain: false,
				},
				From: &From{
					TableName: "foo",
				},
				ResultColumn: ResultColumn{
					Count: true,
					All:   false,
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
				{tkSeparator, ")"},
			},
			expected: &CreateStmt{
				StmtBase: &StmtBase{
					Explain: false,
				},
				TableName: "foo",
				ColDefs: []ColDef{
					{
						ColName:    "id",
						ColType:    "INTEGER",
						PrimaryKey: true,
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
				{tkSeparator, ","},
				{tkWhitespace, " "},
				{tkSeparator, "("},
				{tkNumeric, "3"},
				{tkSeparator, ","},
				{tkWhitespace, " "},
				{tkLiteral, "'jan'"},
				{tkSeparator, ","},
				{tkWhitespace, " "},
				{tkLiteral, "'ice'"},
				{tkSeparator, ")"},
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
					"gud",
					"dude",
					"2",
					"joe",
					"doe",
					"3",
					"jan",
					"ice",
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
