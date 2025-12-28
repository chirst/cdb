package planner

import (
	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/vm"
)

// generatePredicate generates code to make a boolean jump for the given
// expression within the plan context. The function returns the jump command to
// lazily set the jump address.
func generatePredicate(plan *QueryPlan, expression compiler.Expr) vm.JumpCommand {
	pg := &predicateGenerator{}
	pg.plan = plan
	pg.build(expression, 0)
	return pg.jumpCommand
}

// predicateGenerator builds commands to calculate the boolean result of an
// expression.
type predicateGenerator struct {
	plan *QueryPlan
	// jumpCommand is the command used to make the jump. The command can be
	// accessed to defer setting the jump address.
	jumpCommand vm.JumpCommand
}

func (p *predicateGenerator) build(e compiler.Expr, level int) (int, error) {
	switch ce := e.(type) {
	case *compiler.BinaryExpr:
		ol, err := p.build(ce.Left, level+1)
		if err != nil {
			return 0, err
		}
		or, err := p.build(ce.Right, level+1)
		if err != nil {
			return 0, err
		}
		r := p.getNextRegister()
		switch ce.Operator {
		case compiler.OpAdd:
			p.plan.commands = append(
				p.plan.commands,
				&vm.AddCmd{P1: ol, P2: or, P3: r},
			)
			if level == 0 {
				jc := &vm.IfNotCmd{P1: r}
				p.jumpCommand = jc
				p.plan.commands = append(p.plan.commands, jc)
			}
			return r, nil
		case compiler.OpDiv:
			p.plan.commands = append(
				p.plan.commands,
				&vm.DivideCmd{P1: ol, P2: or, P3: r},
			)
			if level == 0 {
				jc := &vm.IfNotCmd{P1: r}
				p.jumpCommand = jc
				p.plan.commands = append(p.plan.commands, jc)
			}
			return r, nil
		case compiler.OpMul:
			p.plan.commands = append(
				p.plan.commands,
				&vm.MultiplyCmd{P1: ol, P2: or, P3: r},
			)
			if level == 0 {
				jc := &vm.IfNotCmd{P1: r}
				p.jumpCommand = jc
				p.plan.commands = append(p.plan.commands, jc)
			}
			return r, nil
		case compiler.OpExp:
			p.plan.commands = append(
				p.plan.commands,
				&vm.ExponentCmd{P1: ol, P2: or, P3: r},
			)
			if level == 0 {
				jc := &vm.IfNotCmd{P1: r}
				p.jumpCommand = jc
				p.plan.commands = append(p.plan.commands, jc)
			}
			return r, nil
		case compiler.OpSub:
			p.plan.commands = append(
				p.plan.commands,
				&vm.SubtractCmd{P1: ol, P2: or, P3: r},
			)
			if level == 0 {
				jc := &vm.IfNotCmd{P1: r}
				p.jumpCommand = jc
				p.plan.commands = append(p.plan.commands, jc)
			}
			return r, nil
		case compiler.OpEq:
			if level == 0 {
				jc := &vm.NotEqualCmd{P1: ol, P3: or}
				p.jumpCommand = jc
				p.plan.commands = append(p.plan.commands, jc)
				return 0, nil
			}
			p.plan.commands = append(p.plan.commands, &vm.IntegerCmd{P1: 0, P2: r})
			jumpOverCount := 2
			jumpAddress := len(p.plan.commands) + jumpOverCount
			p.plan.commands = append(
				p.plan.commands,
				&vm.NotEqualCmd{P1: ol, P2: jumpAddress, P3: or},
			)
			p.plan.commands = append(p.plan.commands, &vm.IntegerCmd{P1: 1, P2: r})
			return r, nil
		case compiler.OpLt:
			if level == 0 {
				jc := &vm.LteCmd{P1: or, P3: ol}
				p.jumpCommand = jc
				p.plan.commands = append(p.plan.commands, jc)
				return 0, nil
			}
			p.plan.commands = append(p.plan.commands, &vm.IntegerCmd{P1: 0, P2: r})
			jumpOverCount := 2
			jumpAddress := len(p.plan.commands) + jumpOverCount
			p.plan.commands = append(
				p.plan.commands,
				&vm.GteCmd{P1: ol, P2: jumpAddress, P3: or},
			)
			p.plan.commands = append(p.plan.commands, &vm.IntegerCmd{P1: 1, P2: r})
			return r, nil
		case compiler.OpGt:
			if level == 0 {
				jc := &vm.GteCmd{P1: or, P3: ol}
				p.jumpCommand = jc
				p.plan.commands = append(p.plan.commands, jc)
				return 0, nil
			}
			p.plan.commands = append(p.plan.commands, &vm.IntegerCmd{P1: 0, P2: r})
			jumpOverCount := 2
			jumpAddress := len(p.plan.commands) + jumpOverCount
			p.plan.commands = append(
				p.plan.commands,
				&vm.LteCmd{P1: ol, P2: jumpAddress, P3: or},
			)
			p.plan.commands = append(p.plan.commands, &vm.IntegerCmd{P1: 1, P2: r})
			return r, nil
		default:
			panic("no vm command for operator")
		}
	case *compiler.ColumnRef:
		colRefReg := p.valueRegisterFor(ce)
		if level == 0 {
			jc := &vm.IfNotCmd{P1: colRefReg}
			p.jumpCommand = jc
			p.plan.commands = append(p.plan.commands, jc)
		}
		return colRefReg, nil
	case *compiler.IntLit:
		cir := p.plan.declareConstInt(ce.Value)
		if level == 0 {
			jc := &vm.IfNotCmd{P1: cir}
			p.jumpCommand = jc
			p.plan.commands = append(p.plan.commands, jc)
		}
		return cir, nil
	case *compiler.StringLit:
		csr := p.plan.declareConstString(ce.Value)
		if level == 0 {
			jc := &vm.IfNotCmd{P1: csr}
			p.jumpCommand = jc
			p.plan.commands = append(p.plan.commands, jc)
		}
		return csr, nil
	case *compiler.Variable:
		cvr := p.plan.declareConstVar(ce.Position)
		if level == 0 {
			jc := &vm.IfNotCmd{P1: cvr}
			p.jumpCommand = jc
			p.plan.commands = append(p.plan.commands, jc)
		}
		return cvr, nil
	}
	panic("unhandled expression in predicate builder")
}

func (p *predicateGenerator) valueRegisterFor(ce *compiler.ColumnRef) int {
	if ce.IsPrimaryKey {
		r := p.getNextRegister()
		p.plan.commands = append(p.plan.commands, &vm.RowIdCmd{
			P1: p.plan.cursorId,
			P2: r,
		})
		return r
	}
	r := p.getNextRegister()
	p.plan.commands = append(p.plan.commands, &vm.ColumnCmd{
		P1: p.plan.cursorId,
		P2: ce.ColIdx, P3: r,
	})
	return r
}

func (p *predicateGenerator) getNextRegister() int {
	r := p.plan.freeRegister
	p.plan.freeRegister += 1
	return r
}
