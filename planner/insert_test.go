package planner

import (
	"errors"
	"testing"

	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/vm"
)

type mockInsertCatalog struct {
	columnsReturn []string
	pkColumnName  string
}

func (c *mockInsertCatalog) GetColumns(s string) ([]string, error) {
	if len(c.columnsReturn) != 0 {
		return c.columnsReturn, nil
	}
	return []string{"id", "first", "last"}, nil
}

func (*mockInsertCatalog) GetRootPageNumber(s string) (int, error) {
	if s == "foo" {
		return 2, nil
	}
	return 0, errors.New("mock error")
}

func (*mockInsertCatalog) GetVersion() string {
	return "v"
}

func (m *mockInsertCatalog) GetPrimaryKeyColumn(tableName string) (string, error) {
	return m.pkColumnName, nil
}

func TestInsertWithoutPrimaryKey(t *testing.T) {
	expectedCommands := []vm.Command{
		&vm.InitCmd{P2: 17},
		&vm.NewRowIdCmd{P1: 1, P2: 1},
		&vm.CopyCmd{P1: 4, P2: 2},
		&vm.CopyCmd{P1: 5, P2: 3},
		&vm.MakeRecordCmd{P1: 2, P2: 2, P3: 6},
		&vm.InsertCmd{P1: 1, P2: 6, P3: 1},
		&vm.NewRowIdCmd{P1: 1, P2: 7},
		&vm.CopyCmd{P1: 10, P2: 8},
		&vm.CopyCmd{P1: 11, P2: 9},
		&vm.MakeRecordCmd{P1: 8, P2: 2, P3: 12},
		&vm.InsertCmd{P1: 1, P2: 12, P3: 7},
		&vm.NewRowIdCmd{P1: 1, P2: 13},
		&vm.CopyCmd{P1: 16, P2: 14},
		&vm.CopyCmd{P1: 17, P2: 15},
		&vm.MakeRecordCmd{P1: 14, P2: 2, P3: 18},
		&vm.InsertCmd{P1: 1, P2: 18, P3: 13},
		&vm.HaltCmd{},
		&vm.TransactionCmd{P2: 1},
		&vm.OpenWriteCmd{P1: 1, P2: 2},
		&vm.StringCmd{P1: 4, P4: "gud"},
		&vm.StringCmd{P1: 5, P4: "dude"},
		&vm.StringCmd{P1: 10, P4: "joe"},
		&vm.StringCmd{P1: 11, P4: "doe"},
		&vm.StringCmd{P1: 16, P4: "jan"},
		&vm.StringCmd{P1: 17, P4: "ice"},
		&vm.GotoCmd{P2: 1},
	}

	ast := &compiler.InsertStmt{
		StmtBase:  &compiler.StmtBase{},
		TableName: "foo",
		ColNames: []string{
			"first",
			"last",
		},
		ColValues: [][]compiler.Expr{
			{
				&compiler.StringLit{Value: "gud"},
				&compiler.StringLit{Value: "dude"},
			},
			{
				&compiler.StringLit{Value: "joe"},
				&compiler.StringLit{Value: "doe"},
			},
			{
				&compiler.StringLit{Value: "jan"},
				&compiler.StringLit{Value: "ice"},
			},
		},
	}
	mockCatalog := &mockInsertCatalog{}
	mockCatalog.columnsReturn = []string{"first", "last"}
	plan, err := NewInsert(mockCatalog, ast).ExecutionPlan()
	if err != nil {
		t.Errorf("expected no err got err %s", err)
	}
	if err := assertCommandsMatch(plan.Commands, expectedCommands); err != nil {
		t.Error(err)
	}
}

func TestInsertWithPrimaryKey(t *testing.T) {
	expectedCommands := []vm.Command{
		&vm.InitCmd{P2: 9},
		&vm.CopyCmd{P1: 2, P2: 1},
		&vm.MustBeIntCmd{P1: 1},
		&vm.NotExistsCmd{P1: 1, P2: 5, P3: 1},
		&vm.HaltCmd{P1: 1, P4: "pk unique constraint violated"},
		&vm.CopyCmd{P1: 4, P2: 3},
		&vm.MakeRecordCmd{P1: 3, P2: 1, P3: 5},
		&vm.InsertCmd{P1: 1, P2: 5, P3: 1},
		&vm.HaltCmd{},
		&vm.TransactionCmd{P2: 1},
		&vm.OpenWriteCmd{P1: 1, P2: 2},
		&vm.IntegerCmd{P1: 22, P2: 2},
		&vm.StringCmd{P1: 4, P4: "gud"},
		&vm.GotoCmd{P2: 1},
	}
	ast := &compiler.InsertStmt{
		StmtBase:  &compiler.StmtBase{},
		TableName: "foo",
		ColNames: []string{
			"id",
			"first",
		},
		ColValues: [][]compiler.Expr{
			{
				&compiler.IntLit{Value: 22},
				&compiler.StringLit{Value: "gud"},
			},
		},
	}
	mockCatalog := &mockInsertCatalog{
		columnsReturn: []string{"id", "first"},
		pkColumnName:  "id",
	}
	plan, err := NewInsert(mockCatalog, ast).ExecutionPlan()
	if err != nil {
		t.Errorf("expected no err got err %s", err)
	}
	if err := assertCommandsMatch(plan.Commands, expectedCommands); err != nil {
		t.Error(err)
	}
}

func TestInsertWithPrimaryKeyMiddleOrder(t *testing.T) {
	expectedCommands := []vm.Command{
		&vm.InitCmd{P2: 9},
		&vm.CopyCmd{P1: 2, P2: 1},
		&vm.MustBeIntCmd{P1: 1},
		&vm.NotExistsCmd{P1: 1, P2: 5, P3: 1},
		&vm.HaltCmd{P1: 1, P4: "pk unique constraint violated"},
		&vm.CopyCmd{P1: 4, P2: 3},
		&vm.MakeRecordCmd{P1: 3, P2: 1, P3: 5},
		&vm.InsertCmd{P1: 1, P2: 5, P3: 1},
		&vm.HaltCmd{},
		&vm.TransactionCmd{P2: 1},
		&vm.OpenWriteCmd{P1: 1, P2: 2},
		&vm.IntegerCmd{P1: 12, P2: 2},
		&vm.StringCmd{P1: 4, P4: "feller"},
		&vm.GotoCmd{P2: 1},
	}
	ast := &compiler.InsertStmt{
		StmtBase:  &compiler.StmtBase{},
		TableName: "foo",
		ColNames: []string{
			"first",
			"id",
		},
		ColValues: [][]compiler.Expr{
			{
				&compiler.StringLit{Value: "feller"},
				&compiler.IntLit{Value: 12},
			},
		},
	}
	mockCatalog := &mockInsertCatalog{
		columnsReturn: []string{"id", "first"},
		pkColumnName:  "id",
	}
	plan, err := NewInsert(mockCatalog, ast).ExecutionPlan()
	if err != nil {
		t.Errorf("expected no err got err %s", err)
	}
	if err := assertCommandsMatch(plan.Commands, expectedCommands); err != nil {
		t.Error(err)
	}
}

func TestInsertWithPrimaryKeyParameter(t *testing.T) {
	expectedCommands := []vm.Command{
		&vm.InitCmd{P2: 9},
		&vm.CopyCmd{P1: 2, P2: 1},
		&vm.MustBeIntCmd{P1: 1},
		&vm.NotExistsCmd{P1: 1, P2: 5, P3: 1},
		&vm.HaltCmd{P1: 1, P4: "pk unique constraint violated"},
		&vm.CopyCmd{P1: 4, P2: 3},
		&vm.MakeRecordCmd{P1: 3, P2: 1, P3: 5},
		&vm.InsertCmd{P1: 1, P2: 5, P3: 1},
		&vm.HaltCmd{},
		&vm.TransactionCmd{P2: 1},
		&vm.OpenWriteCmd{P1: 1, P2: 2},
		&vm.StringCmd{P1: 4, P4: "feller"},
		&vm.VariableCmd{P1: 0, P2: 2},
		&vm.GotoCmd{P2: 1},
	}
	ast := &compiler.InsertStmt{
		StmtBase:  &compiler.StmtBase{},
		TableName: "foo",
		ColNames: []string{
			"first",
			"id",
		},
		ColValues: [][]compiler.Expr{
			{
				&compiler.StringLit{Value: "feller"},
				&compiler.Variable{Position: 0},
			},
		},
	}
	mockCatalog := &mockInsertCatalog{
		columnsReturn: []string{"id", "first"},
		pkColumnName:  "id",
	}
	plan, err := NewInsert(mockCatalog, ast).ExecutionPlan()
	if err != nil {
		t.Errorf("expected no err got err %s", err)
	}
	if err := assertCommandsMatch(plan.Commands, expectedCommands); err != nil {
		t.Error(err)
	}
}

func TestInsertWithParameter(t *testing.T) {
	expectedCommands := []vm.Command{
		&vm.InitCmd{P2: 9},
		&vm.CopyCmd{P1: 2, P2: 1},
		&vm.MustBeIntCmd{P1: 1},
		&vm.NotExistsCmd{P1: 1, P2: 5, P3: 1},
		&vm.HaltCmd{P1: 1, P4: "pk unique constraint violated"},
		&vm.CopyCmd{P1: 4, P2: 3},
		&vm.MakeRecordCmd{P1: 3, P2: 1, P3: 5},
		&vm.InsertCmd{P1: 1, P2: 5, P3: 1},
		&vm.HaltCmd{},
		&vm.TransactionCmd{P2: 1},
		&vm.OpenWriteCmd{P1: 1, P2: 2},
		&vm.VariableCmd{P1: 0, P2: 2},
		&vm.VariableCmd{P1: 1, P2: 4},
		&vm.GotoCmd{P2: 1},
	}
	ast := &compiler.InsertStmt{
		StmtBase:  &compiler.StmtBase{},
		TableName: "foo",
		ColNames: []string{
			"first",
			"id",
		},
		ColValues: [][]compiler.Expr{
			{
				&compiler.Variable{Position: 1},
				&compiler.Variable{Position: 0},
			},
		},
	}
	mockCatalog := &mockInsertCatalog{
		columnsReturn: []string{"id", "first"},
		pkColumnName:  "id",
	}
	plan, err := NewInsert(mockCatalog, ast).ExecutionPlan()
	if err != nil {
		t.Errorf("expected no err got err %s", err)
	}
	if err := assertCommandsMatch(plan.Commands, expectedCommands); err != nil {
		t.Error(err)
	}
}

func TestInsertIntoNonExistingTable(t *testing.T) {
	ast := &compiler.InsertStmt{
		StmtBase:  &compiler.StmtBase{},
		TableName: "NotExistTable",
		ColNames:  []string{},
		ColValues: [][]compiler.Expr{},
	}
	mockCatalog := &mockInsertCatalog{}
	_, err := NewInsert(mockCatalog, ast).ExecutionPlan()
	if !errors.Is(err, errTableNotExist) {
		t.Fatalf("expected err %s got err %s", errTableNotExist, err)
	}
}

func TestInsertValuesNotMatchingColumnsLess(t *testing.T) {
	ast := &compiler.InsertStmt{
		StmtBase:  &compiler.StmtBase{},
		TableName: "foo",
		ColNames: []string{
			"id",
			"first",
		},
		ColValues: [][]compiler.Expr{
			{
				&compiler.IntLit{Value: 1},
				&compiler.StringLit{Value: "gud"},
			},
			{
				&compiler.IntLit{Value: 3},
			},
		},
	}
	mockCatalog := &mockInsertCatalog{}
	_, err := NewInsert(mockCatalog, ast).ExecutionPlan()
	if !errors.Is(err, errValuesNotMatch) {
		t.Fatalf("expected err %s got err %s", errValuesNotMatch, err)
	}
}

func TestInsertValuesNotMatchingColumnsGreater(t *testing.T) {
	ast := &compiler.InsertStmt{
		StmtBase:  &compiler.StmtBase{},
		TableName: "foo",
		ColNames: []string{
			"id",
			"first",
		},
		ColValues: [][]compiler.Expr{
			{
				&compiler.IntLit{Value: 1},
				&compiler.StringLit{Value: "gud"},
			},
			{
				&compiler.IntLit{Value: 3},
				&compiler.StringLit{Value: "gud"},
				&compiler.StringLit{Value: "gud"},
			},
		},
	}
	mockCatalog := &mockInsertCatalog{}
	_, err := NewInsert(mockCatalog, ast).ExecutionPlan()
	if !errors.Is(err, errValuesNotMatch) {
		t.Fatalf("expected err %s got err %s", errValuesNotMatch, err)
	}
}

func TestInsertIntoNonExistingColumn(t *testing.T) {
	ast := &compiler.InsertStmt{
		StmtBase:  &compiler.StmtBase{},
		TableName: "foo",
		ColNames: []string{
			"NonExistColumnName",
		},
		ColValues: [][]compiler.Expr{
			{
				&compiler.StringLit{Value: "gud"},
			},
		},
	}
	mockCatalog := &mockInsertCatalog{}
	_, err := NewInsert(mockCatalog, ast).ExecutionPlan()
	if !errors.Is(err, errMissingColumnName) {
		t.Fatalf("expected err %s got err %s", errMissingColumnName, err)
	}
}
