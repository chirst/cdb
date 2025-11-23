package planner

import (
	"errors"
	"reflect"
	"testing"

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
	return -1, errors.New("err root page")
}

func TestUpdate(t *testing.T) {
	ast := &compiler.UpdateStmt{
		StmtBase:  &compiler.StmtBase{},
		TableName: "foo",
	}
	expectedCommands := []vm.Command{
		&vm.InitCmd{P2: 1},
		&vm.TransactionCmd{P2: 1},
		&vm.OpenWriteCmd{P1: 1, P2: 2},
		&vm.RewindCmd{P1: 1, P2: 11},
		&vm.RowIdCmd{P1: 1, P2: 1},
		&vm.ColumnCmd{P1: 1, P2: 0, P3: 2},
		&vm.IntegerCmd{P1: 1, P2: 3},
		&vm.MakeRecordCmd{P1: 2, P2: 2, P3: 4},
		&vm.DeleteCmd{P1: 1},
		&vm.InsertCmd{P1: 1, P2: 4, P3: 1},
		&vm.NextCmd{P1: 1, P2: 4},
		&vm.HaltCmd{},
	}
	mockCatalog := &mockUpdateCatalog{}
	plan, err := NewUpdate(mockCatalog, ast).ExecutionPlan()
	if err != nil {
		t.Errorf("expected no err got err %s", err)
	}
	for i, c := range expectedCommands {
		if !reflect.DeepEqual(c, plan.Commands[i]) {
			t.Errorf("got %#v want %#v", plan.Commands[i], c)
		}
	}
}
