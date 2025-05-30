package planner

import (
	"errors"
	"reflect"
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
		&vm.InitCmd{P2: 1},
		&vm.TransactionCmd{P2: 1},
		&vm.OpenWriteCmd{P1: 1, P2: 2},
		&vm.NewRowIdCmd{P1: 1, P2: 1},
		&vm.StringCmd{P1: 2, P4: "gud"},
		&vm.StringCmd{P1: 3, P4: "dude"},
		&vm.MakeRecordCmd{P1: 2, P2: 2, P3: 4},
		&vm.InsertCmd{P1: 1, P2: 4, P3: 1},
		&vm.NewRowIdCmd{P1: 1, P2: 1},
		&vm.StringCmd{P1: 2, P4: "joe"},
		&vm.StringCmd{P1: 3, P4: "doe"},
		&vm.MakeRecordCmd{P1: 2, P2: 2, P3: 4},
		&vm.InsertCmd{P1: 1, P2: 4, P3: 1},
		&vm.NewRowIdCmd{P1: 1, P2: 1},
		&vm.StringCmd{P1: 2, P4: "jan"},
		&vm.StringCmd{P1: 3, P4: "ice"},
		&vm.MakeRecordCmd{P1: 2, P2: 2, P3: 4},
		&vm.InsertCmd{P1: 1, P2: 4, P3: 1},
		&vm.HaltCmd{},
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
	for i, c := range expectedCommands {
		if !reflect.DeepEqual(c, plan.Commands[i]) {
			t.Errorf("got %#v want %#v", plan.Commands[i], c)
		}
	}
}

func TestInsertWithPrimaryKey(t *testing.T) {
	expectedCommands := []vm.Command{
		&vm.InitCmd{P2: 1},
		&vm.TransactionCmd{P2: 1},
		&vm.OpenWriteCmd{P1: 1, P2: 2},
		&vm.IntegerCmd{P1: 22, P2: 1},
		&vm.NotExistsCmd{P1: 1, P2: 6, P3: 1},
		&vm.HaltCmd{P1: 1, P4: "pk unique constraint violated"},
		&vm.StringCmd{P1: 2, P4: "gud"},
		&vm.MakeRecordCmd{P1: 2, P2: 1, P3: 3},
		&vm.InsertCmd{P1: 1, P2: 3, P3: 1},
		&vm.HaltCmd{},
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
	for i, c := range expectedCommands {
		if !reflect.DeepEqual(c, plan.Commands[i]) {
			t.Errorf("got %#v want %#v", plan.Commands[i], c)
		}
	}
}

func TestInsertWithPrimaryKeyMiddleOrder(t *testing.T) {
	expectedCommands := []vm.Command{
		&vm.InitCmd{P2: 1},
		&vm.TransactionCmd{P2: 1},
		&vm.OpenWriteCmd{P1: 1, P2: 2},
		&vm.IntegerCmd{P1: 12, P2: 1},
		&vm.NotExistsCmd{P1: 1, P2: 6, P3: 1},
		&vm.HaltCmd{P1: 1, P4: "pk unique constraint violated"},
		&vm.StringCmd{P1: 2, P4: "feller"},
		&vm.MakeRecordCmd{P1: 2, P2: 1, P3: 3},
		&vm.InsertCmd{P1: 1, P2: 3, P3: 1},
		&vm.HaltCmd{},
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
	for i, c := range expectedCommands {
		if !reflect.DeepEqual(c, plan.Commands[i]) {
			t.Errorf("got %#v want %#v", plan.Commands[i], c)
		}
	}
}

func TestInsertWithPrimaryKeyParameter(t *testing.T) {
	expectedCommands := []vm.Command{
		&vm.InitCmd{P2: 1},
		&vm.TransactionCmd{P2: 1},
		&vm.OpenWriteCmd{P1: 1, P2: 2},
		&vm.VariableCmd{P1: 0, P2: 1},
		&vm.MustBeIntCmd{P1: 1},
		&vm.NotExistsCmd{P1: 1, P2: 7, P3: 1},
		&vm.HaltCmd{P1: 1, P4: "pk unique constraint violated"},
		&vm.StringCmd{P1: 2, P4: "feller"},
		&vm.MakeRecordCmd{P1: 2, P2: 1, P3: 3},
		&vm.InsertCmd{P1: 1, P2: 3, P3: 1},
		&vm.HaltCmd{},
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
	for i, c := range expectedCommands {
		if !reflect.DeepEqual(c, plan.Commands[i]) {
			t.Errorf("got %#v want %#v", plan.Commands[i], c)
		}
	}
}

func TestInsertWithParameter(t *testing.T) {
	expectedCommands := []vm.Command{
		&vm.InitCmd{P2: 1},
		&vm.TransactionCmd{P2: 1},
		&vm.OpenWriteCmd{P1: 1, P2: 2},
		&vm.VariableCmd{P1: 0, P2: 1},
		&vm.MustBeIntCmd{P1: 1},
		&vm.NotExistsCmd{P1: 1, P2: 7, P3: 1},
		&vm.HaltCmd{P1: 1, P4: "pk unique constraint violated"},
		&vm.VariableCmd{P1: 1, P2: 2},
		&vm.MakeRecordCmd{P1: 2, P2: 1, P3: 3},
		&vm.InsertCmd{P1: 1, P2: 3, P3: 1},
		&vm.HaltCmd{},
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
	for i, c := range expectedCommands {
		if !reflect.DeepEqual(c, plan.Commands[i]) {
			t.Errorf("got %#v want %#v", plan.Commands[i], c)
		}
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
