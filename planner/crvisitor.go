package planner

import (
	"slices"

	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/vm"
)

// constantRegisterVisitor fills constantRegisters with constants in the visited
// node.
type constantRegisterVisitor struct {
	// nextOpenRegister counts upwards reserving registers as needed.
	nextOpenRegister int
	// constantRegisters is a mapping of register value to register index.
	constantRegisters map[int]int
}

func (c *constantRegisterVisitor) Init(openRegister int) {
	c.constantRegisters = make(map[int]int)
	c.nextOpenRegister = openRegister
}

// GetRegisterCommands returns commands to fill the current constantRegister
// map.
func (c *constantRegisterVisitor) GetRegisterCommands() []vm.Command {
	// Maps are unordered so there is some extra work to keep commands in order.
	unordered := []*vm.IntegerCmd{}
	for k := range c.constantRegisters {
		unordered = append(unordered, &vm.IntegerCmd{P1: k, P2: c.constantRegisters[k]})
	}
	slices.SortFunc(unordered, func(a, b *vm.IntegerCmd) int {
		return a.P2 - b.P2
	})
	ret := []vm.Command{}
	for _, s := range unordered {
		ret = append(ret, vm.Command(s))
	}
	return ret
}

func (c *constantRegisterVisitor) VisitIntLit(e *compiler.IntLit) {
	c.fillRegisterIfNeeded(e.Value)
}

func (c *constantRegisterVisitor) fillRegisterIfNeeded(v int) {
	found := false
	for k := range c.constantRegisters {
		if k == v {
			found = true
		}
	}
	if !found {
		c.constantRegisters[v] = c.nextOpenRegister
		c.nextOpenRegister += 1
	}
}

func (c *constantRegisterVisitor) VisitBinaryExpr(e *compiler.BinaryExpr)     {}
func (c *constantRegisterVisitor) VisitUnaryExpr(e *compiler.UnaryExpr)       {}
func (c *constantRegisterVisitor) VisitColumnRefExpr(e *compiler.ColumnRef)   {}
func (c *constantRegisterVisitor) VisitStringLit(e *compiler.StringLit)       {}
func (c *constantRegisterVisitor) VisitFunctionExpr(e *compiler.FunctionExpr) {}
