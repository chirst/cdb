package planner

import (
	"reflect"
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

func (*mockSelectCatalog) GetRootPageNumber(s string) (int, error) {
	return 2, nil
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
				&vm.InitCmd{P2: 1},
				&vm.TransactionCmd{P1: 0},
				&vm.OpenReadCmd{P1: 1, P2: 2},
				&vm.RewindCmd{P1: 1, P2: 8},
				&vm.RowIdCmd{P1: 1, P2: 1},
				&vm.ColumnCmd{P1: 1, P2: 0, P3: 2},
				&vm.ResultRowCmd{P1: 1, P2: 2},
				&vm.NextCmd{P1: 1, P2: 4},
				&vm.HaltCmd{},
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
				&vm.InitCmd{P2: 1},
				&vm.TransactionCmd{P1: 0},
				&vm.OpenReadCmd{P1: 1, P2: 2},
				&vm.RewindCmd{P1: 1, P2: 8},
				&vm.ColumnCmd{P1: 1, P2: 0, P3: 1},
				&vm.ColumnCmd{P1: 1, P2: 1, P3: 2},
				&vm.ResultRowCmd{P1: 1, P2: 2},
				&vm.NextCmd{P1: 1, P2: 4},
				&vm.HaltCmd{},
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
				&vm.InitCmd{P2: 1},
				&vm.TransactionCmd{P1: 0},
				&vm.OpenReadCmd{P1: 1, P2: 2},
				&vm.RewindCmd{P1: 1, P2: 9},
				&vm.ColumnCmd{P1: 1, P2: 0, P3: 1},
				&vm.RowIdCmd{P1: 1, P2: 2},
				&vm.ColumnCmd{P1: 1, P2: 1, P3: 3},
				&vm.ResultRowCmd{P1: 1, P2: 3},
				&vm.NextCmd{P1: 1, P2: 4},
				&vm.HaltCmd{},
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
				&vm.InitCmd{P2: 1},
				&vm.TransactionCmd{P1: 0},
				&vm.IntegerCmd{P1: 10, P2: 1},
				&vm.OpenReadCmd{P1: 1, P2: 2},
				&vm.RewindCmd{P1: 1, P2: 9},
				&vm.RowIdCmd{P1: 1, P2: 3},
				&vm.AddCmd{P1: 3, P2: 1, P3: 2},
				&vm.ResultRowCmd{P1: 2, P2: 1},
				&vm.NextCmd{P1: 1, P2: 5},
				&vm.HaltCmd{},
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
				&vm.InitCmd{P2: 1},
				&vm.TransactionCmd{P1: 0},
				&vm.OpenReadCmd{P1: 1, P2: 2},
				&vm.RewindCmd{P1: 1, P2: 8},
				&vm.RowIdCmd{P1: 1, P2: 1},
				&vm.ColumnCmd{P1: 1, P2: 0, P3: 2},
				&vm.ResultRowCmd{P1: 1, P2: 2},
				&vm.NextCmd{P1: 1, P2: 4},
				&vm.HaltCmd{},
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
				&vm.InitCmd{P2: 1},
				&vm.TransactionCmd{P1: 0},
				&vm.OpenReadCmd{P1: 1, P2: 2},
				&vm.RewindCmd{P1: 1, P2: 7},
				&vm.RowIdCmd{P1: 1, P2: 1},
				&vm.ResultRowCmd{P1: 1, P2: 1},
				&vm.NextCmd{P1: 1, P2: 4},
				&vm.HaltCmd{},
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
				&vm.InitCmd{P2: 1},
				&vm.TransactionCmd{P1: 0},
				&vm.OpenReadCmd{P1: 1, P2: 2},
				&vm.RewindCmd{P1: 1, P2: 8},
				&vm.RowIdCmd{P1: 1, P2: 1},
				&vm.ColumnCmd{P1: 1, P2: 1, P3: 2},
				&vm.ResultRowCmd{P1: 1, P2: 2},
				&vm.NextCmd{P1: 1, P2: 4},
				&vm.HaltCmd{},
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
				&vm.InitCmd{P2: 1},
				&vm.TransactionCmd{P1: 0},
				&vm.OpenReadCmd{P1: 1, P2: 2},
				&vm.CountCmd{P1: 1, P2: 1},
				&vm.ResultRowCmd{P1: 1, P2: 1},
				&vm.HaltCmd{},
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
				&vm.InitCmd{P2: 1},
				&vm.IntegerCmd{P1: 1, P2: 1},
				&vm.IntegerCmd{P1: 18, P2: 2},
				&vm.IntegerCmd{P1: 387420489, P2: 3},
				&vm.IntegerCmd{P1: 81, P2: 4},
				&vm.IntegerCmd{P1: 0, P2: 5},
				&vm.CopyCmd{P1: 1, P2: 6},
				&vm.CopyCmd{P1: 2, P2: 7},
				&vm.CopyCmd{P1: 3, P2: 8},
				&vm.CopyCmd{P1: 4, P2: 9},
				&vm.CopyCmd{P1: 5, P2: 10},
				&vm.ResultRowCmd{P1: 6, P2: 5},
				&vm.HaltCmd{},
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
			description: "with where clause",
			expectedCommands: []vm.Command{
				&vm.InitCmd{P2: 1},
				&vm.TransactionCmd{P1: 0},
				&vm.IntegerCmd{P1: 1, P2: 1},
				&vm.OpenReadCmd{P1: 1, P2: 2},
				&vm.RewindCmd{P1: 1, P2: 9},
				&vm.RowIdCmd{P1: 1, P2: 2},
				&vm.NotEqualCmd{P1: 2, P2: 8, P3: 1},
				&vm.ResultRowCmd{P1: 2, P2: 1},
				&vm.NextCmd{P1: 1, P2: 5},
				&vm.HaltCmd{},
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
			for i, c := range c.expectedCommands {
				if !reflect.DeepEqual(c, plan.Commands[i]) {
					t.Errorf("got %#v want %#v", plan.Commands[i], c)
				}
			}
		})
	}
}
