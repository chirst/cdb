package main

import (
	"reflect"
	"testing"

	"github.com/chirst/cdb/compiler"
)

func TestLogicalPlan(t *testing.T) {
	ast := &compiler.SelectStmt{
		StmtBase: &compiler.StmtBase{
			Explain: true,
		},
		From: &compiler.From{
			TableName: "foo",
		},
		ResultColumn: compiler.ResultColumn{
			All: true,
		},
	}
	logicalPlan, err := newSelectPlanner(newCatalog()).getLogicalPlan(ast)
	if err != nil {
		t.Errorf("expected no err got err %s", err.Error())
	}
	expectFields := []string{"id", "name"}
	for i, ep := range expectFields {
		if logicalPlan.fields[i] != ep {
			t.Errorf("expected %s, got %s", ep, logicalPlan.fields[i])
		}
	}
	expectRootPage := 2
	if logicalPlan.childSet.rootPage != expectRootPage {
		t.Errorf("expected %d got %d", expectRootPage, logicalPlan.childSet.rootPage)
	}
}

func TestPhysicalPlan(t *testing.T) {
	expectedCommands := map[int]command{
		1: &initCmd{p2: 2},
		2: &transactionCmd{},
		3: &openReadCmd{p2: 2},
		4: &rewindCmd{p2: 9},
		5: &rowIdCmd{p2: 1},
		6: &columnCmd{p2: 1, p3: 2},
		7: &resultRowCmd{p1: 1, p2: 2},
		8: &nextCmd{p2: 5},
		9: &haltCmd{},
	}

	lp := &projection{
		fields: []string{"id", "name"},
		childSet: set{
			rootPage: 2,
		},
	}
	physicalPlan, err := newSelectPlanner(newCatalog()).getPhysicalPlan(lp, false)
	if err != nil {
		t.Errorf("expected no err got err %s", err.Error())
	}
	for i, c := range expectedCommands {
		if !reflect.DeepEqual(c, physicalPlan.commands[i]) {
			t.Errorf("got %#v want %#v", physicalPlan.commands[i], c)
		}
	}
}
