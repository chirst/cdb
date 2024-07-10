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

func TestInsertWithoutPrimaryKey(t *testing.T) {
	expectedCommands := []vm.Command{
		&vm.InitCmd{P2: 1},
		&vm.TransactionCmd{P2: 1},
		&vm.OpenWriteCmd{P1: 1, P2: 2},
		&vm.NewRowIdCmd{P1: 2, P2: 1},
		&vm.StringCmd{P1: 2, P4: "gud"},
		&vm.StringCmd{P1: 3, P4: "dude"},
		&vm.MakeRecordCmd{P1: 2, P2: 2, P3: 4},
		&vm.InsertCmd{P1: 2, P2: 4, P3: 1},
		&vm.NewRowIdCmd{P1: 2, P2: 1},
		&vm.StringCmd{P1: 2, P4: "joe"},
		&vm.StringCmd{P1: 3, P4: "doe"},
		&vm.MakeRecordCmd{P1: 2, P2: 2, P3: 4},
		&vm.InsertCmd{P1: 2, P2: 4, P3: 1},
		&vm.NewRowIdCmd{P1: 2, P2: 1},
		&vm.StringCmd{P1: 2, P4: "jan"},
		&vm.StringCmd{P1: 3, P4: "ice"},
		&vm.MakeRecordCmd{P1: 2, P2: 2, P3: 4},
		&vm.InsertCmd{P1: 2, P2: 4, P3: 1},
		&vm.HaltCmd{},
	}

	ast := &compiler.InsertStmt{
		StmtBase:  &compiler.StmtBase{},
		TableName: "foo",
		ColNames: []string{
			"first",
			"last",
		},
		ColValues: []string{
			"gud",
			"dude",
			"joe",
			"doe",
			"jan",
			"ice",
		},
	}
	mockCatalog := &mockInsertCatalog{}
	plan, err := NewInsert(mockCatalog).GetPlan(ast)
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
		&vm.NotExistsCmd{P1: 2, P2: 5, P3: 22},
		&vm.HaltCmd{P1: 1, P4: "pk unique constraint violated"},
		&vm.IntegerCmd{P1: 22, P2: 1},
		&vm.StringCmd{P1: 2, P4: "gud"},
		&vm.MakeRecordCmd{P1: 2, P2: 1, P3: 3},
		&vm.InsertCmd{P1: 2, P2: 3, P3: 1},
		&vm.HaltCmd{},
	}
	ast := &compiler.InsertStmt{
		StmtBase:  &compiler.StmtBase{},
		TableName: "foo",
		ColNames: []string{
			"id",
			"first",
		},
		ColValues: []string{
			"22",
			"gud",
		},
	}
	mockCatalog := &mockInsertCatalog{
		columnsReturn: []string{"id", "first"},
	}
	plan, err := NewInsert(mockCatalog).GetPlan(ast)
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
		ColValues: []string{},
	}
	mockCatalog := &mockInsertCatalog{}
	_, err := NewInsert(mockCatalog).GetPlan(ast)
	if !errors.Is(err, errTableNotExist) {
		t.Fatalf("expected err %s got err %s", errTableNotExist, err)
	}
}

func TestInsertValuesNotMatchingColumns(t *testing.T) {
	ast := &compiler.InsertStmt{
		StmtBase:  &compiler.StmtBase{},
		TableName: "foo",
		ColNames: []string{
			"id",
			"first",
		},
		ColValues: []string{
			"1",
			"gud",
			"3",
		},
	}
	mockCatalog := &mockInsertCatalog{}
	_, err := NewInsert(mockCatalog).GetPlan(ast)
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
		ColValues: []string{
			"gud",
		},
	}
	mockCatalog := &mockInsertCatalog{}
	_, err := NewInsert(mockCatalog).GetPlan(ast)
	if !errors.Is(err, errMissingColumnName) {
		t.Fatalf("expected err %s got err %s", errMissingColumnName, err)
	}
}
