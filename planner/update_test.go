package planner

import (
	"errors"
	"testing"

	"github.com/chirst/cdb/catalog"
	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/vm"
)

type mockUpdateCatalog struct{}

func (*mockUpdateCatalog) GetVersion() string {
	return "mock"
}

func (*mockUpdateCatalog) GetRootPageNumber(tableName string) (int, error) {
	if tableName == "foo" {
		return 2, nil
	}
	return -1, errors.New("err mock catalog root page")
}

func (*mockUpdateCatalog) GetColumns(tableName string) ([]string, error) {
	if tableName == "foo" {
		return []string{
			"id",
			"age",
			"lucky_number",
		}, nil
	}
	return nil, errors.New("err mock catalog columns")
}

func (*mockUpdateCatalog) GetPrimaryKeyColumn(tableName string) (string, error) {
	if tableName == "foo" {
		return "id", nil
	}
	return "", errors.New("err mock catalog pk")
}

func (mockUpdateCatalog) GetColumnType(tableName string, columnName string) (catalog.CdbType, error) {
	return catalog.CdbType{ID: catalog.CTInt}, nil
}

func TestUpdate(t *testing.T) {
	ast := &compiler.UpdateStmt{
		StmtBase:  &compiler.StmtBase{},
		TableName: "foo",
		SetList: map[string]compiler.Expr{
			"lucky_number": &compiler.IntLit{
				Value: 1,
			},
		},
	}
	expectedCommands := []vm.Command{
		&vm.InitCmd{P2: 10},
		&vm.RewindCmd{P1: 1, P2: 9},
		&vm.RowIdCmd{P1: 1, P2: 1},
		&vm.ColumnCmd{P1: 1, P2: 0, P3: 2},
		&vm.CopyCmd{P1: 4, P2: 3},
		&vm.MakeRecordCmd{P1: 2, P2: 2, P3: 5},
		&vm.DeleteCmd{P1: 1},
		&vm.InsertCmd{P1: 1, P2: 5, P3: 1},
		&vm.NextCmd{P1: 1, P2: 2},
		&vm.HaltCmd{},
		&vm.TransactionCmd{P2: 1},
		&vm.OpenWriteCmd{P1: 1, P2: 2},
		&vm.IntegerCmd{P1: 1, P2: 4},
		&vm.GotoCmd{P2: 1},
	}
	mockCatalog := &mockUpdateCatalog{}
	plan, err := NewUpdate(mockCatalog, ast).ExecutionPlan()
	if err != nil {
		t.Errorf("expected no err got err %s", err)
	}
	if err := assertCommandsMatch(plan.Commands, expectedCommands); err != nil {
		t.Error(err)
	}
}

func TestUpdateWithWhere(t *testing.T) {
	ast := &compiler.UpdateStmt{
		StmtBase:  &compiler.StmtBase{},
		TableName: "foo",
		SetList: map[string]compiler.Expr{
			"lucky_number": &compiler.IntLit{
				Value: 1,
			},
		},
		Predicate: &compiler.BinaryExpr{
			Left: &compiler.ColumnRef{
				Column:       "id",
				IsPrimaryKey: true,
			},
			Operator: compiler.OpEq,
			Right: &compiler.IntLit{
				Value: 1,
			},
		},
	}
	expectedCommands := []vm.Command{
		&vm.InitCmd{P2: 12},
		&vm.RewindCmd{P1: 1, P2: 11},
		&vm.RowIdCmd{P1: 1, P2: 1},
		&vm.NotEqualCmd{P1: 1, P2: 10, P3: 2},
		&vm.RowIdCmd{P1: 1, P2: 4},
		&vm.ColumnCmd{P1: 1, P2: 0, P3: 5},
		&vm.CopyCmd{P1: 2, P2: 6},
		&vm.MakeRecordCmd{P1: 5, P2: 2, P3: 7},
		&vm.DeleteCmd{P1: 1},
		&vm.InsertCmd{P1: 1, P2: 7, P3: 4},
		&vm.NextCmd{P1: 1, P2: 2},
		&vm.HaltCmd{},
		&vm.TransactionCmd{P2: 1},
		&vm.OpenWriteCmd{P1: 1, P2: 2},
		&vm.IntegerCmd{P1: 1, P2: 2},
		&vm.GotoCmd{P2: 1},
	}
	mockCatalog := &mockUpdateCatalog{}
	plan, err := NewUpdate(mockCatalog, ast).ExecutionPlan()
	if err != nil {
		t.Errorf("expected no err got err %s", err)
	}
	if err := assertCommandsMatch(plan.Commands, expectedCommands); err != nil {
		t.Error(err)
	}
}
