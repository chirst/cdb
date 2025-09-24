package vm

import (
	"errors"
	"log"
	"testing"

	"github.com/chirst/cdb/kv"
)

func TestExec(t *testing.T) {
	kv, err := kv.New(true, "")
	if err != nil {
		log.Fatal(err)
	}
	vm := New(kv)
	ep := NewExecutionPlan(kv.GetCatalog().GetVersion(), false)
	ep.Commands = []Command{
		&InitCmd{P2: 1},
		&TransactionCmd{},
		&OpenReadCmd{P1: 1, P2: 2},
		&RewindCmd{P1: 1, P2: 8},
		&RowIdCmd{P1: 1, P2: 1},
		&ColumnCmd{P1: 1, P2: 1, P3: 2},
		&ResultRowCmd{P1: 1, P2: 2},
		&NextCmd{P1: 1, P2: 4},
		&HaltCmd{},
	}
	res := vm.Execute(ep, []any{})
	if res.Err != nil {
		t.Errorf("expected no err got %s", res.Err)
	}
}

func TestExecReturnsVersionErr(t *testing.T) {
	kv, err := kv.New(true, "")
	if err != nil {
		log.Fatal(err)
	}
	vm := New(kv)

	t.Run("for read", func(t *testing.T) {
		ep := NewExecutionPlan("FakeVersion", false)
		ep.Commands = []Command{
			&InitCmd{P2: 1},
			&TransactionCmd{P2: 0},
			&IntegerCmd{P1: 1, P2: 1},
			&HaltCmd{},
		}
		res := vm.Execute(ep, []any{})
		if !errors.Is(res.Err, ErrVersionChanged) {
			t.Errorf("expected version change err")
		}
	})

	t.Run("for write", func(t *testing.T) {
		ep := NewExecutionPlan("FakeVersion", false)
		ep.Commands = []Command{
			&InitCmd{P2: 1},
			&TransactionCmd{P2: 1},
			&IntegerCmd{P1: 1, P2: 1},
			&HaltCmd{},
		}
		res := vm.Execute(ep, []any{})
		if !errors.Is(res.Err, ErrVersionChanged) {
			t.Errorf("expected version change err")
		}
	})
}

// TestAddAffinity is not representative of a real program, but is a realistic
// fixture around the add command. In summary, this fixture allows the tester to
// specify the left and right operand by declaring commands for filling the 1st
// and 2nd registers in the leftRegister and rightRegister.
func TestAddAffinity(t *testing.T) {
	type addCase struct {
		description   string
		leftRegister  Command
		rightRegister Command
		expect        string
	}
	cases := []addCase{
		{
			description:   "add 3 + 4",
			leftRegister:  &IntegerCmd{P1: 3, P2: 1},
			rightRegister: &IntegerCmd{P1: 4, P2: 2},
			expect:        "7",
		},
		{
			description:   "add 3 + '4'",
			leftRegister:  &IntegerCmd{P1: 3, P2: 1},
			rightRegister: &StringCmd{P1: 2, P4: "4"},
			expect:        "7",
		},
		{
			description:   "add 3 + 'foo'",
			leftRegister:  &IntegerCmd{P1: 3, P2: 1},
			rightRegister: &StringCmd{P1: 2, P4: "foo"},
			expect:        "3",
		},
		{
			description:   "add 3 + 'foo5'",
			leftRegister:  &IntegerCmd{P1: 3, P2: 1},
			rightRegister: &StringCmd{P1: 2, P4: "foo5"},
			expect:        "8",
		},
		{
			description:   "add 'foo' + 'foo'",
			leftRegister:  &StringCmd{P1: 1, P4: "foo"},
			rightRegister: &StringCmd{P1: 2, P4: "foo"},
			expect:        "0",
		},
	}
	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			kv, err := kv.New(true, "")
			if err != nil {
				log.Fatal(err)
			}
			vm := New(kv)
			ep := NewExecutionPlan(kv.GetCatalog().GetVersion(), false)
			ep.Commands = []Command{
				&InitCmd{P2: 1},
				c.leftRegister,
				c.rightRegister,
				&AddCmd{P1: 1, P2: 2, P3: 3},
				&ResultRowCmd{P1: 3, P2: 1},
				&HaltCmd{},
			}
			res := vm.Execute(ep, []any{})
			if res.Err != nil {
				t.Fatalf("expected no err got %s", res.Err)
			}
			if got := *res.ResultRows[0][0]; got != c.expect {
				t.Fatalf("expected %s got %s", c.expect, got)
			}
		})
	}
}

func TestNeAffinity(t *testing.T) {
	type neCase struct {
		description   string
		leftRegister  Command
		rightRegister Command
		expect        string
	}
	cases := []neCase{
		{
			description:   "2 != 3",
			leftRegister:  &IntegerCmd{P1: 2, P2: 1},
			rightRegister: &IntegerCmd{P1: 3, P2: 2},
			expect:        "1",
		},
		{
			description:   "2 != 2",
			leftRegister:  &IntegerCmd{P1: 2, P2: 1},
			rightRegister: &IntegerCmd{P1: 2, P2: 2},
			expect:        "0",
		},
		{
			description:   "2 != '2'",
			leftRegister:  &IntegerCmd{P1: 2, P2: 1},
			rightRegister: &StringCmd{P1: 2, P4: "2"},
			expect:        "0",
		},
		{
			description:   "2 != 'foo2'",
			leftRegister:  &IntegerCmd{P1: 2, P2: 1},
			rightRegister: &StringCmd{P1: 2, P4: "foo2"},
			expect:        "1",
		},
	}
	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			kv, err := kv.New(true, "")
			if err != nil {
				log.Fatal(err)
			}
			vm := New(kv)
			ep := NewExecutionPlan(kv.GetCatalog().GetVersion(), false)
			ep.Commands = []Command{
				&InitCmd{P2: 1},
				c.leftRegister,
				c.rightRegister,
				&NotEqualCmd{P1: 1, P2: 7, P3: 2},
				&IntegerCmd{P1: 0, P2: 3},
				&ResultRowCmd{P1: 3, P2: 1},
				&HaltCmd{},
				&IntegerCmd{P1: 1, P2: 3},
				&ResultRowCmd{P1: 3, P2: 1},
				&HaltCmd{},
			}
			res := vm.Execute(ep, []any{})
			if res.Err != nil {
				t.Fatalf("expected no err got %s", res.Err)
			}
			if got := *res.ResultRows[0][0]; got != c.expect {
				t.Fatalf("expected %s got %s", c.expect, got)
			}
		})
	}
}

func TestGteAffinity(t *testing.T) {
	type gteCase struct {
		description   string
		leftRegister  Command
		rightRegister Command
		expect        string
	}
	cases := []gteCase{
		{
			description:   "3 >= 2",
			leftRegister:  &IntegerCmd{P1: 3, P2: 1},
			rightRegister: &IntegerCmd{P1: 2, P2: 2},
			expect:        "1",
		},
		{
			description:   "2 >= 2",
			leftRegister:  &IntegerCmd{P1: 2, P2: 1},
			rightRegister: &IntegerCmd{P1: 2, P2: 2},
			expect:        "1",
		},
		{
			description:   "1 >= 2",
			leftRegister:  &IntegerCmd{P1: 1, P2: 1},
			rightRegister: &IntegerCmd{P1: 2, P2: 2},
			expect:        "0",
		},
		{
			description:   "1 >= 'a'",
			leftRegister:  &IntegerCmd{P1: 1, P2: 1},
			rightRegister: &StringCmd{P1: 2, P4: "a"},
			expect:        "0",
		},
		{
			description:   "'a' >= 'a'",
			leftRegister:  &StringCmd{P1: 1, P4: "a"},
			rightRegister: &StringCmd{P1: 2, P4: "a"},
			expect:        "1",
		},
		{
			description:   "'b' >= 'a'",
			leftRegister:  &StringCmd{P1: 1, P4: "b"},
			rightRegister: &StringCmd{P1: 2, P4: "a"},
			expect:        "1",
		},
	}
	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			kv, err := kv.New(true, "")
			if err != nil {
				log.Fatal(err)
			}
			vm := New(kv)
			ep := NewExecutionPlan(kv.GetCatalog().GetVersion(), false)
			ep.Commands = []Command{
				&InitCmd{P2: 1},
				c.leftRegister,
				c.rightRegister,
				&GteCmd{P1: 1, P2: 7, P3: 2},
				&IntegerCmd{P1: 0, P2: 3},
				&ResultRowCmd{P1: 3, P2: 1},
				&HaltCmd{},
				&IntegerCmd{P1: 1, P2: 3},
				&ResultRowCmd{P1: 3, P2: 1},
				&HaltCmd{},
			}
			res := vm.Execute(ep, []any{})
			if res.Err != nil {
				t.Fatalf("expected no err got %s", res.Err)
			}
			if got := *res.ResultRows[0][0]; got != c.expect {
				t.Fatalf("expected %s got %s", c.expect, got)
			}
		})
	}
}
