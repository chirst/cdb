package planner

import (
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
	ret := []vm.Command{}
	for k := range c.constantRegisters {
		ret = append(ret, &vm.IntegerCmd{P1: k, P2: c.constantRegisters[k]})
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
