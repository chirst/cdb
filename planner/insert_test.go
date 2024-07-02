package planner

import (
	"reflect"
	"testing"

	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/vm"
)

type mockInsertCatalog struct{}

func (*mockInsertCatalog) GetColumns(s string) ([]string, error) {
	return []string{"id", "first", "last"}, nil
}

func (*mockInsertCatalog) GetRootPageNumber(s string) (int, error) {
	return 2, nil
}

func TestInsert(t *testing.T) {
	expectedCommands := []vm.Command{
		&vm.InitCmd{P2: 1},
		&vm.TransactionCmd{P2: 1},
		&vm.OpenWriteCmd{P1: 1, P2: 2},
		&vm.NewRowIdCmd{P1: 2, P2: 1},
		&vm.StringCmd{P1: 2, P4: "gud"},
		&vm.StringCmd{P1: 3, P4: "dude"},
		&vm.MakeRecordCmd{P1: 2, P2: 3, P3: 4},
		&vm.InsertCmd{P1: 2, P2: 4, P3: 1},
		&vm.NewRowIdCmd{P1: 2, P2: 1},
		&vm.StringCmd{P1: 2, P4: "joe"},
		&vm.StringCmd{P1: 3, P4: "doe"},
		&vm.MakeRecordCmd{P1: 2, P2: 3, P3: 4},
		&vm.InsertCmd{P1: 2, P2: 4, P3: 1},
		&vm.NewRowIdCmd{P1: 2, P2: 1},
		&vm.StringCmd{P1: 2, P4: "jan"},
		&vm.StringCmd{P1: 3, P4: "ice"},
		&vm.MakeRecordCmd{P1: 2, P2: 3, P3: 4},
		&vm.InsertCmd{P1: 2, P2: 4, P3: 1},
		&vm.HaltCmd{},
	}

	ast := &compiler.InsertStmt{
		StmtBase: &compiler.StmtBase{
			Explain: false,
		},
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
		t.Errorf("expected no err got err %s", err.Error())
	}
	for i, c := range expectedCommands {
		if !reflect.DeepEqual(c, plan.Commands[i]) {
			t.Errorf("got %#v want %#v", plan.Commands[i], c)
		}
	}
}
