package planner

import (
	"reflect"
	"testing"

	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/vm"
)

type mockSelectCatalog struct{}

func (*mockSelectCatalog) GetColumns(s string) ([]string, error) {
	return []string{"id", "name"}, nil
}

func (*mockSelectCatalog) GetRootPageNumber(s string) (int, error) {
	return 2, nil
}

func TestGetPlan(t *testing.T) {
	expectedCommands := []vm.Command{
		&vm.InitCmd{P2: 1},
		&vm.TransactionCmd{P1: 0},
		&vm.OpenReadCmd{P1: 1, P2: 2},
		&vm.RewindCmd{P1: 1, P2: 8},
		&vm.RowIdCmd{P1: 1, P2: 1},
		&vm.ColumnCmd{P1: 1, P2: 0, P3: 2},
		&vm.ResultRowCmd{P1: 1, P2: 2},
		&vm.NextCmd{P1: 1, P2: 4},
		&vm.HaltCmd{},
	}

	ast := &compiler.SelectStmt{
		StmtBase: &compiler.StmtBase{
			Explain: false,
		},
		From: &compiler.From{
			TableName: "foo",
		},
		ResultColumn: compiler.ResultColumn{
			All: true,
		},
	}
	mockCatalog := &mockSelectCatalog{}
	plan, err := NewSelect(mockCatalog).GetPlan(ast)
	if err != nil {
		t.Errorf("expected no err got err %s", err.Error())
	}
	for i, c := range expectedCommands {
		if !reflect.DeepEqual(c, plan.Commands[i]) {
			t.Errorf("got %#v want %#v", plan.Commands[i], c)
		}
	}
}
