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
	lp := newLogicalPlanner()
	logicalPlan := lp.forSelect(ast)
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
		1:  &initCmd{p2: 2},
		2:  &transactionCmd{},
		3:  &openReadCmd{p2: 2},
		4:  &rewindCmd{},
		5:  &rowIdCmd{},
		6:  &columnCmd{},
		7:  &columnCmd{},
		8:  &resultRowCmd{},
		9:  &nextCmd{},
		10: &haltCmd{},
	}
	p := newPhysicalPlanner()
	lp := &projection{
		fields: []string{"id", "name"},
		childSet: set{
			rootPage: 2,
		},
	}
	physicalPlan := p.forSelect(lp, false)
	for i, c := range expectedCommands {
		if !reflect.DeepEqual(c, physicalPlan.commands[i]) {
			t.Errorf("got %#v want %#v", physicalPlan.commands[i], c)
		}
	}
}
