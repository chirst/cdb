package planner

import (
	"slices"
	"strings"

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
	// variableRegisters is a mapping of variable indices to registers.
	variableRegisters map[int]int
	// stringRegisters is a mapping of string constants to registers
	stringRegisters map[string]int
}

func (c *constantRegisterVisitor) Init(openRegister int) {
	c.constantRegisters = make(map[int]int)
	c.variableRegisters = make(map[int]int)
	c.stringRegisters = make(map[string]int)
	c.nextOpenRegister = openRegister
}

// GetRegisterCommands returns commands to fill the current constantRegister
// map.
func (c *constantRegisterVisitor) GetRegisterCommands() []vm.Command {
	// Maps are unordered so there is some extra work to keep commands in order.
	lc := c.getOrderedLitCommands()
	vc := c.getOrderedVariableCommands()
	sc := c.getOrderedStringCommands()
	return append(lc, append(vc, sc...)...)
}

func (c *constantRegisterVisitor) getOrderedLitCommands() []vm.Command {
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

func (c *constantRegisterVisitor) getOrderedVariableCommands() []vm.Command {
	unordered := []*vm.VariableCmd{}
	for k := range c.variableRegisters {
		unordered = append(unordered, &vm.VariableCmd{P1: k, P2: c.variableRegisters[k]})
	}
	slices.SortFunc(unordered, func(a, b *vm.VariableCmd) int {
		return a.P2 - b.P2
	})
	ret := []vm.Command{}
	for _, s := range unordered {
		ret = append(ret, vm.Command(s))
	}
	return ret
}

func (c *constantRegisterVisitor) getOrderedStringCommands() []vm.Command {
	unordered := []*vm.StringCmd{}
	for k := range c.stringRegisters {
		unordered = append(unordered, &vm.StringCmd{P1: c.stringRegisters[k], P4: k})
	}
	slices.SortFunc(unordered, func(a, b *vm.StringCmd) int {
		return strings.Compare(a.P4, b.P4)
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

func (c *constantRegisterVisitor) VisitVariable(v *compiler.Variable) {
	c.variableRegisters[v.Position] = c.nextOpenRegister
	c.nextOpenRegister += 1
}

func (c *constantRegisterVisitor) VisitStringLit(e *compiler.StringLit) {
	found := false
	for k := range c.stringRegisters {
		if k == e.Value {
			found = true
		}
	}
	if !found {
		c.stringRegisters[e.Value] = c.nextOpenRegister
		c.nextOpenRegister += 1
	}
}

func (c *constantRegisterVisitor) VisitBinaryExpr(e *compiler.BinaryExpr)     {}
func (c *constantRegisterVisitor) VisitUnaryExpr(e *compiler.UnaryExpr)       {}
func (c *constantRegisterVisitor) VisitColumnRefExpr(e *compiler.ColumnRef)   {}
func (c *constantRegisterVisitor) VisitFunctionExpr(e *compiler.FunctionExpr) {}
