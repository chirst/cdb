package planner

import (
	"errors"
	"testing"

	"github.com/chirst/cdb/catalog"
	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/vm"
)

type mockSelectCatalog struct {
	columns              []string
	columnTypes          []catalog.CdbType
	primaryKeyColumnName string
}

func (m *mockSelectCatalog) GetColumns(s string) ([]string, error) {
	if len(m.columns) == 0 {
		return []string{"id", "name"}, nil
	}
	return m.columns, nil
}

func (m *mockSelectCatalog) GetRootPageNumber(s string) (int, error) {
	if s == "foo" {
		return 2, nil
	}
	return 0, errors.New("mock cannot get root")
}

func (*mockSelectCatalog) GetVersion() string {
	return "v"
}

func (m *mockSelectCatalog) GetPrimaryKeyColumn(tableName string) (string, error) {
	return m.primaryKeyColumnName, nil
}

func (m *mockSelectCatalog) GetColumnType(tableName string, columnName string) (catalog.CdbType, error) {
	if len(m.columnTypes) == 0 {
		if columnName == "id" {
			return catalog.CdbType{ID: catalog.CTInt}, nil
		}
		return catalog.CdbType{ID: catalog.CTStr}, nil
	}
	return catalog.CdbType{ID: catalog.CTUnknown}, nil
}

func TestSelectPlan(t *testing.T) {
	type selectCase struct {
		description      string
		ast              *compiler.SelectStmt
		expectedCommands []vm.Command
		mockCatalogSetup func(m *mockSelectCatalog) *mockSelectCatalog
	}
	cases := []selectCase{
		{
			description: "StarWithPrimaryKey",
			expectedCommands: []vm.Command{
				&vm.InitCmd{P2: 8},
				&vm.OpenReadCmd{P1: 1, P2: 2},
				&vm.RewindCmd{P1: 1, P2: 7},
				&vm.RowIdCmd{P1: 1, P2: 1},
				&vm.ColumnCmd{P1: 1, P2: 0, P3: 2},
				&vm.ResultRowCmd{P1: 1, P2: 2},
				&vm.NextCmd{P1: 1, P2: 3},
				&vm.HaltCmd{},
				&vm.TransactionCmd{P1: 0},
				&vm.GotoCmd{P2: 1},
			},
			ast: &compiler.SelectStmt{
				StmtBase: &compiler.StmtBase{},
				From: &compiler.From{
					TableName: "foo",
				},
				ResultColumns: []compiler.ResultColumn{
					{
						All: true,
					},
				},
			},
			mockCatalogSetup: func(m *mockSelectCatalog) *mockSelectCatalog {
				m.primaryKeyColumnName = "id"
				return m
			},
		},
		{
			description: "StarWithoutPrimaryKey",
			expectedCommands: []vm.Command{
				&vm.InitCmd{P2: 8},
				&vm.OpenReadCmd{P1: 1, P2: 2},
				&vm.RewindCmd{P1: 1, P2: 7},
				&vm.ColumnCmd{P1: 1, P2: 0, P3: 1},
				&vm.ColumnCmd{P1: 1, P2: 1, P3: 2},
				&vm.ResultRowCmd{P1: 1, P2: 2},
				&vm.NextCmd{P1: 1, P2: 3},
				&vm.HaltCmd{},
				&vm.TransactionCmd{P1: 0},
				&vm.GotoCmd{P2: 1},
			},
			ast: &compiler.SelectStmt{
				StmtBase: &compiler.StmtBase{},
				From: &compiler.From{
					TableName: "foo",
				},
				ResultColumns: []compiler.ResultColumn{
					{
						All: true,
					},
				},
			},
		},
		{
			description: "StarPrimaryKeyMiddleOrdinal",
			ast: &compiler.SelectStmt{
				StmtBase: &compiler.StmtBase{},
				From: &compiler.From{
					TableName: "foo",
				},
				ResultColumns: []compiler.ResultColumn{
					{
						All: true,
					},
				},
			},
			expectedCommands: []vm.Command{
				&vm.InitCmd{P2: 9},
				&vm.OpenReadCmd{P1: 1, P2: 2},
				&vm.RewindCmd{P1: 1, P2: 8},
				&vm.ColumnCmd{P1: 1, P2: 0, P3: 1},
				&vm.RowIdCmd{P1: 1, P2: 2},
				&vm.ColumnCmd{P1: 1, P2: 1, P3: 3},
				&vm.ResultRowCmd{P1: 1, P2: 3},
				&vm.NextCmd{P1: 1, P2: 3},
				&vm.HaltCmd{},
				&vm.TransactionCmd{P1: 0},
				&vm.GotoCmd{P2: 1},
			},
			mockCatalogSetup: func(m *mockSelectCatalog) *mockSelectCatalog {
				m.primaryKeyColumnName = "id"
				m.columnTypes = []catalog.CdbType{
					{ID: catalog.CTStr},
					{ID: catalog.CTInt},
					{ID: catalog.CTInt},
				}
				m.columns = []string{"name", "id", "age"}
				return m
			},
		},
		{
			description: "PrimaryKeyInExpression",
			ast: &compiler.SelectStmt{
				StmtBase: &compiler.StmtBase{},
				From: &compiler.From{
					TableName: "foo",
				},
				ResultColumns: []compiler.ResultColumn{
					{
						Expression: &compiler.BinaryExpr{
							Left: &compiler.ColumnRef{
								Table:  "foo",
								Column: "id",
							},
							Operator: compiler.OpAdd,
							Right:    &compiler.IntLit{Value: 10},
						},
					},
				},
			},
			expectedCommands: []vm.Command{
				&vm.InitCmd{P2: 8},
				&vm.OpenReadCmd{P1: 1, P2: 2},
				&vm.RewindCmd{P1: 1, P2: 7},
				&vm.RowIdCmd{P1: 1, P2: 2},
				&vm.AddCmd{P1: 2, P2: 3, P3: 1},
				&vm.ResultRowCmd{P1: 1, P2: 1},
				&vm.NextCmd{P1: 1, P2: 3},
				&vm.HaltCmd{},
				&vm.TransactionCmd{P1: 0},
				&vm.IntegerCmd{P1: 10, P2: 3},
				&vm.GotoCmd{P2: 1},
			},
			mockCatalogSetup: func(m *mockSelectCatalog) *mockSelectCatalog {
				m.primaryKeyColumnName = "id"
				m.columnTypes = []catalog.CdbType{{ID: catalog.CTInt}}
				m.columns = []string{"id"}
				return m
			},
		},
		{
			description: "AllTable",
			expectedCommands: []vm.Command{
				&vm.InitCmd{P2: 8},
				&vm.OpenReadCmd{P1: 1, P2: 2},
				&vm.RewindCmd{P1: 1, P2: 7},
				&vm.RowIdCmd{P1: 1, P2: 1},
				&vm.ColumnCmd{P1: 1, P2: 0, P3: 2},
				&vm.ResultRowCmd{P1: 1, P2: 2},
				&vm.NextCmd{P1: 1, P2: 3},
				&vm.HaltCmd{},
				&vm.TransactionCmd{P1: 0},
				&vm.GotoCmd{P2: 1},
			},
			ast: &compiler.SelectStmt{
				StmtBase: &compiler.StmtBase{},
				From: &compiler.From{
					TableName: "foo",
				},
				ResultColumns: []compiler.ResultColumn{
					{
						AllTable: "foo",
					},
				},
			},
			mockCatalogSetup: func(m *mockSelectCatalog) *mockSelectCatalog {
				m.primaryKeyColumnName = "id"
				m.columnTypes = []catalog.CdbType{
					{ID: catalog.CTInt},
					{ID: catalog.CTStr},
				}
				m.columns = []string{"id", "name"}
				return m
			},
		},
		{
			description: "SpecificColumnPrimaryKeyMiddleOrdinal",
			expectedCommands: []vm.Command{
				&vm.InitCmd{P2: 7},
				&vm.OpenReadCmd{P1: 1, P2: 2},
				&vm.RewindCmd{P1: 1, P2: 6},
				&vm.RowIdCmd{P1: 1, P2: 1},
				&vm.ResultRowCmd{P1: 1, P2: 1},
				&vm.NextCmd{P1: 1, P2: 3},
				&vm.HaltCmd{},
				&vm.TransactionCmd{P1: 0},
				&vm.GotoCmd{P2: 1},
			},
			ast: &compiler.SelectStmt{
				StmtBase: &compiler.StmtBase{},
				From: &compiler.From{
					TableName: "foo",
				},
				ResultColumns: []compiler.ResultColumn{
					{
						Expression: &compiler.ColumnRef{
							Column: "id",
						},
					},
				},
			},
			mockCatalogSetup: func(m *mockSelectCatalog) *mockSelectCatalog {
				m.primaryKeyColumnName = "id"
				m.columnTypes = []catalog.CdbType{
					{ID: catalog.CTStr},
					{ID: catalog.CTInt},
					{ID: catalog.CTInt},
				}
				m.columns = []string{"name", "id", "age"}
				return m
			},
		},
		{
			description: "SpecificColumns",
			expectedCommands: []vm.Command{
				&vm.InitCmd{P2: 8},
				&vm.OpenReadCmd{P1: 1, P2: 2},
				&vm.RewindCmd{P1: 1, P2: 7},
				&vm.RowIdCmd{P1: 1, P2: 1},
				&vm.ColumnCmd{P1: 1, P2: 1, P3: 2},
				&vm.ResultRowCmd{P1: 1, P2: 2},
				&vm.NextCmd{P1: 1, P2: 3},
				&vm.HaltCmd{},
				&vm.TransactionCmd{P1: 0},
				&vm.GotoCmd{P2: 1},
			},
			ast: &compiler.SelectStmt{
				StmtBase: &compiler.StmtBase{},
				From: &compiler.From{
					TableName: "foo",
				},
				ResultColumns: []compiler.ResultColumn{
					{
						Expression: &compiler.ColumnRef{
							Column: "id",
						},
					},
					{
						Expression: &compiler.ColumnRef{
							Column: "age",
						},
					},
				},
			},
			mockCatalogSetup: func(m *mockSelectCatalog) *mockSelectCatalog {
				m.primaryKeyColumnName = "id"
				m.columnTypes = []catalog.CdbType{
					{ID: catalog.CTStr},
					{ID: catalog.CTInt},
					{ID: catalog.CTInt},
				}
				m.columns = []string{"name", "id", "age"}
				return m
			},
		},
		{
			description: "JustCountAggregate",
			expectedCommands: []vm.Command{
				&vm.InitCmd{P2: 5},
				&vm.OpenReadCmd{P1: 1, P2: 2},
				&vm.CountCmd{P1: 1, P2: 1},
				&vm.ResultRowCmd{P1: 1, P2: 1},
				&vm.HaltCmd{},
				&vm.TransactionCmd{P1: 0},
				&vm.GotoCmd{P2: 1},
			},
			ast: &compiler.SelectStmt{
				StmtBase: &compiler.StmtBase{},
				From: &compiler.From{
					TableName: "foo",
				},
				ResultColumns: []compiler.ResultColumn{
					{
						Expression: &compiler.FunctionExpr{FnType: compiler.FnCount},
					},
				},
			},
		},
		{
			description: "Operators",
			expectedCommands: []vm.Command{
				&vm.InitCmd{P2: 8},
				&vm.CopyCmd{P1: 6, P2: 1},
				&vm.CopyCmd{P1: 7, P2: 2},
				&vm.CopyCmd{P1: 8, P2: 3},
				&vm.CopyCmd{P1: 9, P2: 4},
				&vm.CopyCmd{P1: 10, P2: 5},
				&vm.ResultRowCmd{P1: 1, P2: 5},
				&vm.HaltCmd{},
				&vm.IntegerCmd{P1: 1, P2: 6},
				&vm.IntegerCmd{P1: 18, P2: 7},
				&vm.IntegerCmd{P1: 387420489, P2: 8},
				&vm.IntegerCmd{P1: 81, P2: 9},
				&vm.IntegerCmd{P1: 0, P2: 10},
				&vm.GotoCmd{P2: 1},
			},
			ast: &compiler.SelectStmt{
				StmtBase: &compiler.StmtBase{},
				From:     nil,
				ResultColumns: []compiler.ResultColumn{
					{
						Expression: &compiler.BinaryExpr{
							Left:     &compiler.IntLit{Value: 9},
							Right:    &compiler.IntLit{Value: 9},
							Operator: compiler.OpDiv,
						},
					},
					{
						Expression: &compiler.BinaryExpr{
							Left:     &compiler.IntLit{Value: 9},
							Right:    &compiler.IntLit{Value: 9},
							Operator: compiler.OpAdd,
						},
					},
					{
						Expression: &compiler.BinaryExpr{
							Left:     &compiler.IntLit{Value: 9},
							Right:    &compiler.IntLit{Value: 9},
							Operator: compiler.OpExp,
						},
					},
					{
						Expression: &compiler.BinaryExpr{
							Left:     &compiler.IntLit{Value: 9},
							Right:    &compiler.IntLit{Value: 9},
							Operator: compiler.OpMul,
						},
					},
					{
						Expression: &compiler.BinaryExpr{
							Left:     &compiler.IntLit{Value: 9},
							Right:    &compiler.IntLit{Value: 9},
							Operator: compiler.OpSub,
						},
					},
				},
			},
		},
		{
			description: "WithWhereClause",
			expectedCommands: []vm.Command{
				&vm.InitCmd{P2: 7},
				&vm.OpenReadCmd{P1: 1, P2: 2},
				&vm.CopyCmd{P1: 2, P2: 1},
				&vm.SeekRowId{P1: 1, P2: 6, P3: 1},
				&vm.RowIdCmd{P1: 1, P2: 3},
				&vm.ResultRowCmd{P1: 3, P2: 1},
				&vm.HaltCmd{},
				&vm.TransactionCmd{P1: 0},
				&vm.IntegerCmd{P1: 1, P2: 2},
				&vm.GotoCmd{P2: 1},
			},
			ast: &compiler.SelectStmt{
				StmtBase: &compiler.StmtBase{},
				From: &compiler.From{
					TableName: "foo",
				},
				ResultColumns: []compiler.ResultColumn{
					{
						Expression: &compiler.ColumnRef{
							Column: "id",
						},
					},
				},
				Where: &compiler.BinaryExpr{
					Left:     &compiler.ColumnRef{Column: "id"},
					Right:    &compiler.IntLit{Value: 1},
					Operator: compiler.OpEq,
				},
			},
			mockCatalogSetup: func(m *mockSelectCatalog) *mockSelectCatalog {
				m.primaryKeyColumnName = "id"
				return m
			},
		},
		{
			description: "ConstantString",
			expectedCommands: []vm.Command{
				&vm.InitCmd{P2: 4},
				&vm.CopyCmd{P1: 2, P2: 1},
				&vm.ResultRowCmd{P1: 1, P2: 1},
				&vm.HaltCmd{},
				&vm.StringCmd{P1: 2, P4: "foo"},
				&vm.GotoCmd{P2: 1},
			},
			ast: &compiler.SelectStmt{
				StmtBase: &compiler.StmtBase{},
				ResultColumns: []compiler.ResultColumn{
					{
						Expression: &compiler.StringLit{
							Value: "foo",
						},
					},
				},
			},
		},
	}
	for _, c := range cases {
		if c.description == "" {
			t.Fatal("cases must have description")
		}
		t.Run(c.description, func(t *testing.T) {
			if c.mockCatalogSetup == nil {
				c.mockCatalogSetup = func(m *mockSelectCatalog) *mockSelectCatalog {
					return m
				}
			}
			mockCatalog := c.mockCatalogSetup(&mockSelectCatalog{})
			plan, err := NewSelect(mockCatalog, c.ast).ExecutionPlan()
			if err != nil {
				t.Errorf("expected no err got err %s", err)
			}
			if err := assertCommandsMatch(plan.Commands, c.expectedCommands); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestSelectTableDoesNotExist(t *testing.T) {
	ast := &compiler.SelectStmt{
		StmtBase: &compiler.StmtBase{},
		From: &compiler.From{
			TableName: "does_not_exist",
		},
		ResultColumns: []compiler.ResultColumn{
			{
				All: true,
			},
		},
	}
	mockCatalog := &mockSelectCatalog{}
	_, err := NewSelect(mockCatalog, ast).ExecutionPlan()
	if expectErr := errTableNotExist; !errors.Is(err, expectErr) {
		t.Fatalf("expected err: %s but got: %s", expectErr, err)
	}
}

func TestUsePrimaryKeyIndex(t *testing.T) {
	ast := &compiler.SelectStmt{
		StmtBase: &compiler.StmtBase{},
		From: &compiler.From{
			TableName: "foo",
		},
		ResultColumns: []compiler.ResultColumn{
			{
				All: true,
			},
		},
		Where: &compiler.BinaryExpr{
			Left:     &compiler.ColumnRef{Column: "id"},
			Right:    &compiler.IntLit{Value: 1},
			Operator: compiler.OpEq,
		},
	}
	mockCatalog := &mockSelectCatalog{
		primaryKeyColumnName: "id",
	}
	qp, err := NewSelect(mockCatalog, ast).QueryPlan()
	if err != nil {
		t.Errorf("expected no err got err %s", err)
	}
	if pn, ok := qp.root.(*projectNode); ok {
		if seekN, ok := pn.child.(*seekNode); ok {
			if seekN.parent != pn {
				t.Error("expected parent to be pn")
			}
		} else {
			t.Errorf("expected seek node but got %#v", pn.child)
		}
	} else {
		t.Errorf("expected project node but got %#v", qp.root)
	}
}
