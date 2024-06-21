package main

import (
	"log"
	"testing"
)

func TestExec(t *testing.T) {
	kv, err := NewKv(true)
	if err != nil {
		log.Fatal(err.Error())
	}
	vm := newVm(kv)
	ep := &executionPlan{
		commands: []command{
			&initCmd{p2: 1},
			&transactionCmd{},
			&openReadCmd{p1: 1, p2: 2},
			&rewindCmd{p1: 1, p2: 8},
			&rowIdCmd{p1: 1, p2: 1},
			&columnCmd{p1: 1, p2: 1, p3: 2},
			&resultRowCmd{p1: 1, p2: 2},
			&nextCmd{p1: 1, p2: 4},
			&haltCmd{},
		},
		explain: false,
	}
	res := vm.execute(ep)
	if res.err != nil {
		t.Errorf("expected no err got %s", res.err.Error())
	}
}
