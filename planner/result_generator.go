package planner

import (
	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/vm"
)

// generateExpressionTo takes the context of the plan and generates commands
// that land the result of the given expr in the toRegister.
func generateExpressionTo(plan *planV2, expr compiler.Expr, toRegister int) {
	rg := &resultExprGenerator{}
	rg.plan = plan
	rg.outputRegister = toRegister
	rg.commandOffset = len(rg.plan.commands)
	rg.build(expr, 0)
}

// resultExprGenerator builds commands for the given expression.
type resultExprGenerator struct {
	plan *planV2
	// outputRegister is the target register for the result of the expression.
	outputRegister int
	// commandOffset is the amount of commands prior to calling this routine.
	// Useful for calculating jump instructions.
	commandOffset int
}

func (e *resultExprGenerator) build(root compiler.Expr, level int) int {
	switch n := root.(type) {
	case *compiler.BinaryExpr:
		ol := e.build(n.Left, level+1)
		or := e.build(n.Right, level+1)
		r := e.getNextRegister(level)
		switch n.Operator {
		case compiler.OpAdd:
			e.plan.commands = append(e.plan.commands, &vm.AddCmd{P1: ol, P2: or, P3: r})
		case compiler.OpDiv:
			e.plan.commands = append(e.plan.commands, &vm.DivideCmd{P1: ol, P2: or, P3: r})
		case compiler.OpMul:
			e.plan.commands = append(e.plan.commands, &vm.MultiplyCmd{P1: ol, P2: or, P3: r})
		case compiler.OpExp:
			e.plan.commands = append(e.plan.commands, &vm.ExponentCmd{P1: ol, P2: or, P3: r})
		case compiler.OpSub:
			e.plan.commands = append(e.plan.commands, &vm.SubtractCmd{P1: ol, P2: or, P3: r})
		case compiler.OpEq:
			e.plan.commands = append(e.plan.commands, &vm.IntegerCmd{P1: 0, P2: r})
			jumpOverCount := 2
			jumpAddress := len(e.plan.commands) + jumpOverCount + e.commandOffset
			e.plan.commands = append(
				e.plan.commands,
				&vm.NotEqualCmd{P1: ol, P2: jumpAddress, P3: or},
			)
			e.plan.commands = append(e.plan.commands, &vm.IntegerCmd{P1: 1, P2: r})
		case compiler.OpLt:
			e.plan.commands = append(e.plan.commands, &vm.IntegerCmd{P1: 0, P2: r})
			jumpOverCount := 2
			jumpAddress := len(e.plan.commands) + jumpOverCount + e.commandOffset
			e.plan.commands = append(
				e.plan.commands,
				&vm.GteCmd{P1: ol, P2: jumpAddress, P3: or},
			)
			e.plan.commands = append(e.plan.commands, &vm.IntegerCmd{P1: 1, P2: r})
		case compiler.OpGt:
			e.plan.commands = append(e.plan.commands, &vm.IntegerCmd{P1: 0, P2: r})
			jumpOverCount := 2
			jumpAddress := len(e.plan.commands) + jumpOverCount + e.commandOffset
			e.plan.commands = append(
				e.plan.commands,
				&vm.LteCmd{P1: ol, P2: jumpAddress, P3: or},
			)
			e.plan.commands = append(e.plan.commands, &vm.IntegerCmd{P1: 1, P2: r})
		default:
			panic("no vm command for operator")
		}
		return r
	case *compiler.ColumnRef:
		r := e.getNextRegister(level)
		if n.IsPrimaryKey {
			e.plan.commands = append(e.plan.commands, &vm.RowIdCmd{P1: e.plan.cursorId, P2: r})
		} else {
			e.plan.commands = append(
				e.plan.commands,
				&vm.ColumnCmd{P1: e.plan.cursorId, P2: n.ColIdx, P3: r},
			)
		}
		return r
	case *compiler.IntLit:
		cir := e.plan.declareConstInt(n.Value)
		if level == 0 {
			e.plan.commands = append(
				e.plan.commands,
				&vm.CopyCmd{P1: cir, P2: e.outputRegister},
			)
		}
		return cir
	case *compiler.StringLit:
		csr := e.plan.declareConstString(n.Value)
		if level == 0 {
			e.plan.commands = append(
				e.plan.commands,
				&vm.CopyCmd{P1: csr, P2: e.outputRegister},
			)
		}
		return csr
	case *compiler.Variable:
		cvr := e.plan.declareConstVar(n.Position)
		if level == 0 {
			e.plan.commands = append(
				e.plan.commands,
				&vm.CopyCmd{P1: cvr, P2: e.outputRegister},
			)
		}
		return cvr
	}
	panic("unhandled expression in expr command builder")
}

func (e *resultExprGenerator) getNextRegister(level int) int {
	if level == 0 {
		return e.outputRegister
	}
	r := e.plan.freeRegister
	e.plan.freeRegister += 1
	return r
}
