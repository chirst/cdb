package main

import (
	"reflect"
	"testing"

	"github.com/chirst/cdb/compiler"
)

type mockSelectCatalog struct{}

func (*mockSelectCatalog) getColumns(s string) ([]string, error) {
	return []string{"id", "name"}, nil
}

func (*mockSelectCatalog) getRootPageNumber(s string) (int, error) {
	return 2, nil
}

func TestGetPlan(t *testing.T) {
	expectedCommands := []command{
		&initCmd{p2: 1},
		&transactionCmd{p1: 0},
		&openReadCmd{p1: 1, p2: 2},
		&rewindCmd{p1: 1, p2: 8},
		&rowIdCmd{p1: 1, p2: 1},
		&columnCmd{p1: 1, p2: 0, p3: 2},
		&resultRowCmd{p1: 1, p2: 2},
		&nextCmd{p1: 1, p2: 4},
		&haltCmd{},
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
	plan, err := newSelectPlanner(mockCatalog).getPlan(ast)
	if err != nil {
		t.Errorf("expected no err got err %s", err.Error())
	}
	for i, c := range expectedCommands {
		if !reflect.DeepEqual(c, plan.commands[i]) {
			t.Errorf("got %#v want %#v", plan.commands[i], c)
		}
	}
}
