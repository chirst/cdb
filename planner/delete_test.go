package planner

import (
	"errors"
	"testing"

	"github.com/chirst/cdb/catalog"
	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/vm"
)

type mockDeleteCatalog struct{}

func (*mockDeleteCatalog) GetVersion() string {
	return "mock"
}

func (*mockDeleteCatalog) GetRootPageNumber(tableName string) (int, error) {
	if tableName == "foo" {
		return 2, nil
	}
	return -1, errors.New("err mock catalog root page")
}

func (*mockDeleteCatalog) GetColumns(tableName string) ([]string, error) {
	if tableName == "foo" {
		return []string{
			"id",
			"age",
		}, nil
	}
	return nil, errors.New("err mock catalog columns")
}

func (*mockDeleteCatalog) GetPrimaryKeyColumn(tableName string) (string, error) {
	if tableName == "foo" {
		return "id", nil
	}
	return "", errors.New("err mock catalog pk")
}

func (mockDeleteCatalog) GetColumnType(tableName string, columnName string) (catalog.CdbType, error) {
	return catalog.CdbType{ID: catalog.CTInt}, nil
}

func TestDelete(t *testing.T) {
	type deleteTestCase struct {
		expectation      string
		ast              *compiler.DeleteStmt
		expectedCommands []vm.Command
	}
	tcs := []deleteTestCase{
		{
			expectation: "DeleteWithNoPredicate",
			ast: &compiler.DeleteStmt{
				StmtBase:  &compiler.StmtBase{},
				TableName: "foo",
			},
			expectedCommands: []vm.Command{
				&vm.InitCmd{P2: 6},
				&vm.OpenWriteCmd{P1: 1, P2: 2},
				&vm.RewindCmd{P1: 1, P2: 5},
				&vm.DeleteCmd{P1: 1},
				&vm.NextCmd{P1: 1, P2: 3},
				&vm.HaltCmd{},
				&vm.TransactionCmd{P2: 1},
				&vm.GotoCmd{P2: 1},
			},
		},
		{
			expectation: "DeleteWithPredicate",
			ast: &compiler.DeleteStmt{
				StmtBase:  &compiler.StmtBase{},
				TableName: "foo",
				Predicate: &compiler.BinaryExpr{
					Operator: compiler.OpEq,
					Left: &compiler.ColumnRef{
						Column: "id",
					},
					Right: &compiler.IntLit{Value: 1},
				},
			},
			expectedCommands: []vm.Command{
				&vm.InitCmd{P2: 8},
				&vm.OpenWriteCmd{P1: 1, P2: 2},
				&vm.RewindCmd{P1: 1, P2: 7},
				&vm.RowIdCmd{P1: 1, P2: 1},
				&vm.NotEqualCmd{P1: 1, P2: 6, P3: 2},
				&vm.DeleteCmd{P1: 1},
				&vm.NextCmd{P1: 1, P2: 3},
				&vm.HaltCmd{},
				&vm.TransactionCmd{P2: 1},
				&vm.IntegerCmd{P1: 1, P2: 2},
				&vm.GotoCmd{P2: 1},
			},
		},
	}
	for _, tc := range tcs {
		t.Run(tc.expectation, func(t *testing.T) {
			mockCatalog := &mockDeleteCatalog{}
			plan, err := NewDelete(mockCatalog, tc.ast).ExecutionPlan()
			if err != nil {
				t.Errorf("expected no err got err %s", err)
			}
			if err := assertCommandsMatch(plan.Commands, tc.expectedCommands); err != nil {
				t.Error(err)
			}
		})
	}
}
