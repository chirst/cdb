package main

import "testing"

func TestExec(t *testing.T) {
	vm := newVm()
	ep := &executionPlan{
		commands: map[int]command{
			1: &initCmd{p2: 2},
			2: &transactionCmd{},
			3: &openReadCmd{p2: 2},
			4: &rewindCmd{p2: 9},
			5: &rowIdCmd{p2: 1},
			6: &columnCmd{p2: 1, p3: 2},
			7: &resultRowCmd{p1: 1, p2: 2},
			8: &nextCmd{p2: 5},
			9: &haltCmd{},
		},
		explain: false,
	}
	vm.execute(ep)
}
