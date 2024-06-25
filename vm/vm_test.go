package vm

import (
	"log"
	"testing"

	"github.com/chirst/cdb/kv"
)

func TestExec(t *testing.T) {
	kv, err := kv.New(true)
	if err != nil {
		log.Fatal(err.Error())
	}
	vm := New(kv)
	ep := &ExecutionPlan{
		Commands: []Command{
			&InitCmd{P2: 1},
			&TransactionCmd{},
			&OpenReadCmd{P1: 1, P2: 2},
			&RewindCmd{P1: 1, P2: 8},
			&RowIdCmd{P1: 1, P2: 1},
			&ColumnCmd{P1: 1, P2: 1, P3: 2},
			&ResultRowCmd{P1: 1, P2: 2},
			&NextCmd{P1: 1, P2: 4},
			&HaltCmd{},
		},
		Explain: false,
	}
	res := vm.Execute(ep)
	if res.Err != nil {
		t.Errorf("expected no err got %s", res.Err.Error())
	}
}
