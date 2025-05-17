package compiler

import (
	"reflect"
	"slices"
	"testing"
)

type selectTestCase struct {
	name   string
	tokens []Token
	expect Stmt
}

func TestParseSelect(t *testing.T) {
	cases := []selectTestCase{
		{
			name: "with explain",
			tokens: []Token{
				{tkKeyword, "EXPLAIN"},
				{TkWhitespace, " "},
				{tkKeyword, "SELECT"},
				{TkWhitespace, " "},
				{tkOperator, "*"},
				{TkWhitespace, " "},
				{tkKeyword, "FROM"},
				{TkWhitespace, " "},
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
			tokens: []Token{
				{tkKeyword, "EXPLAIN"},
				{TkWhitespace, " "},
				{tkKeyword, "QUERY"},
				{TkWhitespace, " "},
				{tkKeyword, "PLAN"},
				{TkWhitespace, " "},
				{tkKeyword, "SELECT"},
				{TkWhitespace, " "},
				{tkOperator, "*"},
				{TkWhitespace, " "},
				{tkKeyword, "FROM"},
				{TkWhitespace, " "},
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
			name: "with where clause",
			tokens: []Token{
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
			expect: &SelectStmt{
				StmtBase: &StmtBase{},
				From: &From{
					TableName: "foo",
				},
				ResultColumns: []ResultColumn{
					{All: true},
				},
				Where: &BinaryExpr{
					Left:     &ColumnRef{Column: "id"},
					Right:    &IntLit{Value: 1},
					Operator: OpEq,
				},
			},
		},
		{
			name: "constant with where clause",
			tokens: []Token{
				{tkKeyword, "SELECT"},
				{TkWhitespace, " "},
				{tkNumeric, "1"},
				{TkWhitespace, " "},
				{tkKeyword, "WHERE"},
				{TkWhitespace, " "},
				{tkNumeric, "1"},
			},
			expect: &SelectStmt{
				StmtBase: &StmtBase{},
				ResultColumns: []ResultColumn{
					{Expression: &IntLit{Value: 1}},
				},
				Where: &IntLit{Value: 1},
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
	tokens   []Token
	expected Stmt
}

func TestParseCreate(t *testing.T) {
	cases := []createTestCase{
		{
			tokens: []Token{
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
	tokens   []Token
	expected Stmt
}

func TestParseInsert(t *testing.T) {
	cases := []insertTestCase{
		{
			tokens: []Token{
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
				{tkSeparator, ","},
				{TkWhitespace, " "},
				{tkSeparator, "("},
				{tkNumeric, "3"},
				{tkSeparator, ","},
				{TkWhitespace, " "},
				{tkLiteral, "'jan'"},
				{tkSeparator, ","},
				{TkWhitespace, " "},
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

type resultColumnTestCase struct {
	name   string
	tokens []Token
	expect []ResultColumn
}

func TestParseResultColumn(t *testing.T) {
	template := []Token{
		{tkKeyword, "SELECT"},
		{TkWhitespace, " "},
		{TkWhitespace, " "},
		{tkKeyword, "FROM"},
		{TkWhitespace, " "},
		{tkIdentifier, "foo"},
	}
	cases := []resultColumnTestCase{
		{
			name: "*",
			tokens: []Token{
				{tkOperator, "*"},
			},
			expect: []ResultColumn{
				{
					All: true,
				},
			},
		},
		{
			name: "foo.*",
			tokens: []Token{
				{tkIdentifier, "foo"},
				{tkOperator, "."},
				{tkOperator, "*"},
			},
			expect: []ResultColumn{
				{
					AllTable: "foo",
				},
			},
		},
		{
			name: "COUNT(*)",
			tokens: []Token{
				{tkKeyword, "COUNT"},
				{tkSeparator, "("},
				{tkOperator, "*"},
				{tkSeparator, ")"},
			},
			expect: []ResultColumn{
				{
					Expression: &FunctionExpr{FnType: FnCount},
				},
			},
		},
		{
			name: "COUNT(*) + 1",
			tokens: []Token{
				{tkKeyword, "COUNT"},
				{tkSeparator, "("},
				{tkOperator, "*"},
				{tkSeparator, ")"},
				{TkWhitespace, " "},
				{tkOperator, "+"},
				{TkWhitespace, " "},
				{tkNumeric, "1"},
			},
			expect: []ResultColumn{
				{
					Expression: &BinaryExpr{
						Left:     &FunctionExpr{FnType: FnCount},
						Operator: "+",
						Right:    &IntLit{Value: 1},
					},
				},
			},
		},
		{
			name: "(1 + 2 - (3 * 4) + (5 / (6 ^ 7)) - (8 * 9))",
			tokens: []Token{
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
			expect: []ResultColumn{
				{
					Expression: &BinaryExpr{
						Left: &BinaryExpr{
							Left: &BinaryExpr{
								Left: &BinaryExpr{
									Left:     &IntLit{Value: 1},
									Operator: OpAdd,
									Right:    &IntLit{Value: 2},
								},
								Operator: OpSub,
								Right: &BinaryExpr{
									Left:     &IntLit{Value: 3},
									Operator: OpMul,
									Right:    &IntLit{Value: 4},
								},
							},
							Operator: OpAdd,
							Right: &BinaryExpr{
								Left:     &IntLit{Value: 5},
								Operator: OpDiv,
								Right: &BinaryExpr{
									Left:     &IntLit{Value: 6},
									Operator: OpExp,
									Right:    &IntLit{Value: 7},
								},
							},
						},
						Operator: OpSub,
						Right: &BinaryExpr{
							Left:     &IntLit{Value: 8},
							Operator: OpMul,
							Right:    &IntLit{Value: 9},
						},
					},
				},
			},
		},
		{
			name: "foo.id AS bar",
			tokens: []Token{
				{tkIdentifier, "foo"},
				{tkSeparator, "."},
				{tkIdentifier, "id"},
				{TkWhitespace, " "},
				{tkKeyword, "AS"},
				{TkWhitespace, " "},
				{tkIdentifier, "bar"},
			},
			expect: []ResultColumn{
				{
					Expression: &ColumnRef{
						Table:  "foo",
						Column: "id",
					},
					Alias: "bar",
				},
			},
		},
		{
			name: "1 + 2 AS foo, id, id2 AS id1",
			tokens: []Token{
				{tkNumeric, "1"},
				{TkWhitespace, " "},
				{tkOperator, "+"},
				{TkWhitespace, " "},
				{tkNumeric, "2"},
				{TkWhitespace, " "},
				{tkKeyword, "AS"},
				{TkWhitespace, " "},
				{tkIdentifier, "foo"},
				{tkSeparator, ","},
				{TkWhitespace, " "},
				{tkIdentifier, "id"},
				{tkSeparator, ","},
				{TkWhitespace, " "},
				{tkIdentifier, "id2"},
				{TkWhitespace, " "},
				{tkKeyword, "AS"},
				{TkWhitespace, " "},
				{tkIdentifier, "id1"},
			},
			expect: []ResultColumn{
				{
					Expression: &BinaryExpr{
						Left:     &IntLit{Value: 1},
						Operator: "+",
						Right:    &IntLit{Value: 2},
					},
					Alias: "foo",
				},
				{
					Expression: &ColumnRef{Column: "id"},
				},
				{
					Expression: &ColumnRef{Column: "id2"},
					Alias:      "id1",
				},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tks := slices.Insert(template, 2, c.tokens...)
			ret, err := NewParser(tks).Parse()
			if err != nil {
				t.Errorf("want no err got err %s", err)
			}
			retSelect, _ := ret.(*SelectStmt)
			if !reflect.DeepEqual(retSelect.ResultColumns, c.expect) {
				t.Errorf("got %#v want %#v", retSelect.ResultColumns, c.expect)
			}
		})
	}
}
