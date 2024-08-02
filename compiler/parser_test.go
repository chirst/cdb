package compiler

import (
	"reflect"
	"testing"
)

type selectTestCase struct {
	name   string
	tokens []token
	expect Stmt
}

func TestParseSelect(t *testing.T) {
	cases := []selectTestCase{
		{
			name: "with explain",
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
				ResultColumns: []ResultColumn{
					{
						All: true,
					},
				},
			},
		},
		{
			name: "with explain query plan",
			tokens: []token{
				{tkKeyword, "EXPLAIN"},
				{tkWhitespace, " "},
				{tkKeyword, "QUERY"},
				{tkWhitespace, " "},
				{tkKeyword, "PLAN"},
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
					Explain:          false,
					ExplainQueryPlan: true,
				},
				From: &From{
					TableName: "foo",
				},
				ResultColumns: []ResultColumn{
					{
						All: true,
					},
				},
			},
		},
		{
			name: "with count",
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
				ResultColumns: []ResultColumn{
					{
						Count: true,
						All:   false,
					},
				},
			},
		},
		{
			name: "select table column",
			tokens: []token{
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
			expect: &SelectStmt{
				StmtBase: &StmtBase{},
				From: &From{
					TableName: "foo",
				},
				ResultColumns: []ResultColumn{
					{
						Expression: &ColumnRef{
							Table:  "foo",
							Column: "id",
						},
					},
				},
			},
		},
		{
			name: "select table all",
			tokens: []token{
				{tkKeyword, "SELECT"},
				{tkWhitespace, " "},
				{tkIdentifier, "foo"},
				{tkSeparator, "."},
				{tkPunctuator, "*"},
				{tkWhitespace, " "},
				{tkKeyword, "FROM"},
				{tkWhitespace, " "},
				{tkIdentifier, "foo"},
			},
			expect: &SelectStmt{
				StmtBase: &StmtBase{},
				From: &From{
					TableName: "foo",
				},
				ResultColumns: []ResultColumn{
					{
						AllTable: "foo",
					},
				},
			},
		},
		{
			name: "select literal expression with alias",
			tokens: []token{
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
			expect: &SelectStmt{
				StmtBase: &StmtBase{},
				From: &From{
					TableName: "foo",
				},
				ResultColumns: []ResultColumn{
					{
						Expression: &IntLit{
							Value: 1,
						},
						Alias: "bar",
					},
				},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ret, err := NewParser(c.tokens).Parse()
			if err != nil {
				t.Errorf("want no err got err %s", err)
			}
			if !reflect.DeepEqual(ret, c.expect) {
				t.Errorf("got %#v want %#v", ret, c.expect)
			}
		})
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
			t.Errorf("expected no err got err %s", err)
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
				ColValues: [][]string{
					{
						"1",
						"gud",
						"dude",
					},
					{
						"2",
						"joe",
						"doe",
					},
					{
						"3",
						"jan",
						"ice",
					},
				},
			},
		},
	}
	for _, c := range cases {
		ret, err := NewParser(c.tokens).Parse()
		if err != nil {
			t.Errorf("expected no err got err %s", err)
		}
		if !reflect.DeepEqual(ret, c.expected) {
			t.Errorf("expected %#v got %#v", c.expected, ret)
		}
	}
}
