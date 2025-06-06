package compiler

import (
	"reflect"
	"slices"
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
				{tkOperator, "*"},
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
				{tkOperator, "*"},
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
			name: "with where clause",
			tokens: []token{
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
			tokens: []token{
				{tkKeyword, "SELECT"},
				{tkWhitespace, " "},
				{tkNumeric, "1"},
				{tkWhitespace, " "},
				{tkKeyword, "WHERE"},
				{tkWhitespace, " "},
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
		{
			name: "leading and trailing space",
			tokens: []token{
				{tkWhitespace, " "},
				{tkKeyword, "SELECT"},
				{tkWhitespace, " "},
				{tkNumeric, "1"},
				{tkWhitespace, " "},
			},
			expect: &SelectStmt{
				StmtBase: &StmtBase{},
				ResultColumns: []ResultColumn{
					{Expression: &IntLit{Value: 1}},
				},
			},
		},
		{
			name: "query with parameters order forwards",
			tokens: []token{
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
				{tkParam, "?"},
				{tkWhitespace, " "},
				{tkOperator, "="},
				{tkWhitespace, " "},
				{tkParam, "?"},
				{tkWhitespace, " "},
				{tkOperator, "+"},
				{tkWhitespace, " "},
				{tkParam, "?"},
			},
			expect: &SelectStmt{
				StmtBase: &StmtBase{},
				ResultColumns: []ResultColumn{
					{All: true},
				},
				From: &From{TableName: "foo"},
				Where: &BinaryExpr{
					Left:     &Variable{Position: 0},
					Operator: OpEq,
					Right: &BinaryExpr{
						Left:     &Variable{Position: 1},
						Operator: OpAdd,
						Right:    &Variable{Position: 2},
					},
				},
			},
		},
		{
			name: "query with parameters order reverse",
			tokens: []token{
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
				{tkParam, "?"},
				{tkWhitespace, " "},
				{tkOperator, "+"},
				{tkWhitespace, " "},
				{tkParam, "?"},
				{tkWhitespace, " "},
				{tkOperator, "="},
				{tkWhitespace, " "},
				{tkParam, "?"},
			},
			expect: &SelectStmt{
				StmtBase: &StmtBase{},
				ResultColumns: []ResultColumn{
					{All: true},
				},
				From: &From{TableName: "foo"},
				Where: &BinaryExpr{
					Left: &BinaryExpr{
						Left:     &Variable{Position: 0},
						Operator: OpAdd,
						Right:    &Variable{Position: 1},
					},
					Operator: OpEq,
					Right:    &Variable{Position: 2},
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

func TestParseCreate(t *testing.T) {
	type createTestCase struct {
		name     string
		tokens   []token
		expected Stmt
	}
	cases := []createTestCase{
		{
			name: "basic create",
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
		{
			name: "create with if not exists",
			tokens: []token{
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
			expected: &CreateStmt{
				StmtBase: &StmtBase{
					Explain: false,
				},
				IfNotExists: true,
				TableName:   "bar",
				ColDefs: []ColDef{
					{
						ColName: "id",
						ColType: "INTEGER",
					},
				},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ret, err := NewParser(c.tokens).Parse()
			if err != nil {
				t.Errorf("expected no err got err %s", err)
			}
			if !reflect.DeepEqual(ret, c.expected) {
				t.Errorf("expected %#v got %#v", c.expected, ret)
			}
		})
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
				{tkSeparator, ","},
				{tkWhitespace, " "},
				{tkSeparator, "("},
				{tkNumeric, "4"},
				{tkSeparator, ","},
				{tkWhitespace, " "},
				{tkParam, "?"},
				{tkSeparator, ","},
				{tkWhitespace, " "},
				{tkParam, "?"},
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
				ColValues: [][]Expr{
					{
						&IntLit{Value: 1},
						&StringLit{Value: "gud"},
						&StringLit{Value: "dude"},
					},
					{
						&IntLit{Value: 2},
						&StringLit{Value: "joe"},
						&StringLit{Value: "doe"},
					},
					{
						&IntLit{Value: 3},
						&StringLit{Value: "jan"},
						&StringLit{Value: "ice"},
					},
					{
						&IntLit{Value: 4},
						&Variable{Position: 0},
						&Variable{Position: 1},
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
	tokens []token
	expect []ResultColumn
}

func TestParseResultColumn(t *testing.T) {
	template := []token{
		{tkKeyword, "SELECT"},
		{tkWhitespace, " "},
		{tkWhitespace, " "},
		{tkKeyword, "FROM"},
		{tkWhitespace, " "},
		{tkIdentifier, "foo"},
	}
	cases := []resultColumnTestCase{
		{
			name: "*",
			tokens: []token{
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
			tokens: []token{
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
			tokens: []token{
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
			tokens: []token{
				{tkKeyword, "COUNT"},
				{tkSeparator, "("},
				{tkOperator, "*"},
				{tkSeparator, ")"},
				{tkWhitespace, " "},
				{tkOperator, "+"},
				{tkWhitespace, " "},
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
			tokens: []token{
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
			tokens: []token{
				{tkIdentifier, "foo"},
				{tkSeparator, "."},
				{tkIdentifier, "id"},
				{tkWhitespace, " "},
				{tkKeyword, "AS"},
				{tkWhitespace, " "},
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
			tokens: []token{
				{tkNumeric, "1"},
				{tkWhitespace, " "},
				{tkOperator, "+"},
				{tkWhitespace, " "},
				{tkNumeric, "2"},
				{tkWhitespace, " "},
				{tkKeyword, "AS"},
				{tkWhitespace, " "},
				{tkIdentifier, "foo"},
				{tkSeparator, ","},
				{tkWhitespace, " "},
				{tkIdentifier, "id"},
				{tkSeparator, ","},
				{tkWhitespace, " "},
				{tkIdentifier, "id2"},
				{tkWhitespace, " "},
				{tkKeyword, "AS"},
				{tkWhitespace, " "},
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
