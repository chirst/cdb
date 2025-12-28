package planner

import (
	"slices"

	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/vm"
)

// transactionType defines possible transactions for a query plan.
type transactionType int

const (
	transactionTypeNone  transactionType = 0
	transactionTypeRead  transactionType = 1
	transactionTypeWrite transactionType = 2
)

// declareConstInt gets or sets a register with the const value and returns the
// register. It is guaranteed the value will be in the register for the duration
// of the plan.
func (p *QueryPlan) declareConstInt(i int) int {
	_, ok := p.constInts[i]
	if !ok {
		p.constInts[i] = p.freeRegister
		p.freeRegister += 1
	}
	return p.constInts[i]
}

// declareConstString gets or sets a register with the const value and returns
// the register. It is guaranteed the value will be in the register for the
// duration of the plan.
func (p *QueryPlan) declareConstString(s string) int {
	_, ok := p.constStrings[s]
	if !ok {
		p.constStrings[s] = p.freeRegister
		p.freeRegister += 1
	}
	return p.constStrings[s]
}

// declareConstVar gets or sets a register with the const value and returns
// the register. It is guaranteed the value will be in the register for the
// duration of the plan.
func (p *QueryPlan) declareConstVar(position int) int {
	_, ok := p.constVars[position]
	if !ok {
		p.constVars[position] = p.freeRegister
		p.freeRegister += 1
	}
	return p.constVars[position]
}

func (p *QueryPlan) compile() {
	initCmd := &vm.InitCmd{}
	p.commands = append(p.commands, initCmd)
	p.root.produce()
	p.commands = append(p.commands, &vm.HaltCmd{})
	initCmd.P2 = len(p.commands)
	p.pushTransaction()
	p.pushConstants()
	p.commands = append(p.commands, &vm.GotoCmd{P2: 1})
}

func (p *QueryPlan) pushTransaction() {
	switch p.transactionType {
	case transactionTypeNone:
		return
	case transactionTypeRead:
		p.commands = append(
			p.commands,
			&vm.TransactionCmd{P2: 0},
		)
		p.commands = append(
			p.commands,
			&vm.OpenReadCmd{P1: p.cursorId, P2: p.rootPageNumber},
		)
	case transactionTypeWrite:
		p.commands = append(
			p.commands,
			&vm.TransactionCmd{P2: 1},
		)
		p.commands = append(
			p.commands,
			&vm.OpenWriteCmd{P1: p.cursorId, P2: p.rootPageNumber},
		)
	default:
		panic("unexpected transaction type")
	}
}

func (p *QueryPlan) pushConstants() {
	// these constants are pushed ordered since maps are unordered making it
	// difficult to assert that a sequence of instructions appears.
	p.pushConstantInts()
	p.pushConstantStrings()
	p.pushConstantVars()
}

func (p *QueryPlan) pushConstantInts() {
	temp := []*vm.IntegerCmd{}
	for k := range p.constInts {
		temp = append(temp, &vm.IntegerCmd{P1: k, P2: p.constInts[k]})
	}
	slices.SortFunc(temp, func(a, b *vm.IntegerCmd) int {
		return a.P2 - b.P2
	})
	for i := range temp {
		p.commands = append(p.commands, temp[i])
	}
}

func (p *QueryPlan) pushConstantStrings() {
	temp := []*vm.StringCmd{}
	for v := range p.constStrings {
		p.commands = append(p.commands, &vm.StringCmd{P1: p.constStrings[v], P4: v})
	}
	slices.SortFunc(temp, func(a, b *vm.StringCmd) int {
		return a.P2 - b.P2
	})
	for i := range temp {
		p.commands = append(p.commands, temp[i])
	}
}

func (p *QueryPlan) pushConstantVars() {
	temp := []*vm.VariableCmd{}
	for v := range p.constVars {
		p.commands = append(p.commands, &vm.VariableCmd{P1: v, P2: p.constVars[v]})
	}
	slices.SortFunc(temp, func(a, b *vm.VariableCmd) int {
		return a.P2 - b.P2
	})
	for i := range temp {
		p.commands = append(p.commands, temp[i])
	}
}

type updateNode struct {
	child logicalNode
	plan  *QueryPlan
	// updateExprs is formed from the update statement AST. The idea is to
	// provide an expression for each column where the expression is either a
	// columnRef or the complex expression from the right hand side of the SET
	// keyword. Note it is important to provide the expressions in their correct
	// ordinal position as the generator will not try to order them correctly.
	//
	// The row id is not allowed to be updated at the moment because it could
	// cause infinite loops due to it changing the physical location of the
	// record. The query plan will have to use a temporary storage to update
	// primary keys.
	updateExprs []compiler.Expr
}

func (u *updateNode) produce() {
	u.child.produce()
}

func (u *updateNode) consume() {
	// RowID
	u.plan.commands = append(u.plan.commands, &vm.RowIdCmd{
		P1: u.plan.cursorId,
		P2: u.plan.freeRegister,
	})
	rowIdRegister := u.plan.freeRegister
	u.plan.freeRegister += 1

	// Reserve a contiguous block of free registers for the columns. This block
	// will be used in makeRecord.
	startRecordRegister := u.plan.freeRegister
	u.plan.freeRegister += len(u.updateExprs)
	recordRegisterCount := len(u.updateExprs)
	for i, e := range u.updateExprs {
		generateExpressionTo(u.plan, e, startRecordRegister+i)
	}

	// Make the record for inserting
	u.plan.commands = append(u.plan.commands, &vm.MakeRecordCmd{
		P1: startRecordRegister,
		P2: recordRegisterCount,
		P3: u.plan.freeRegister,
	})
	recordRegister := u.plan.freeRegister
	u.plan.freeRegister += 1

	// Update by deleting then inserting
	u.plan.commands = append(u.plan.commands, &vm.DeleteCmd{
		P1: u.plan.cursorId,
	})
	u.plan.commands = append(u.plan.commands, &vm.InsertCmd{
		P1: u.plan.cursorId,
		P2: recordRegister,
		P3: rowIdRegister,
	})
}

type filterNode struct {
	child     logicalNode
	parent    logicalNode
	plan      *QueryPlan
	predicate compiler.Expr
}

func (f *filterNode) produce() {
	f.child.produce()
}

func (f *filterNode) consume() {
	jumpCommand := generatePredicate(f.plan, f.predicate)
	f.parent.consume()
	jumpCommand.SetJumpAddress(len(f.plan.commands))
}

type scanNode struct {
	parent logicalNode
	plan   *QueryPlan
}

func (s *scanNode) produce() {
	s.consume()
}

func (s *scanNode) consume() {
	rewindCmd := &vm.RewindCmd{P1: s.plan.cursorId}
	s.plan.commands = append(s.plan.commands, rewindCmd)
	loopBeginAddress := len(s.plan.commands)
	s.parent.consume()
	s.plan.commands = append(s.plan.commands, &vm.NextCmd{
		P1: s.plan.cursorId,
		P2: loopBeginAddress,
	})
	rewindCmd.P2 = len(s.plan.commands)
}

type projection struct {
	expr compiler.Expr
	// alias is the alias of the projection or no alias for the zero value.
	alias string
}

type projectNode struct {
	child       logicalNode
	plan        *QueryPlan
	projections []projection
}

func (p *projectNode) produce() {
	p.child.produce()
}

func (p *projectNode) consume() {
	startRegister := p.plan.freeRegister
	reservedRegisters := len(p.projections)
	p.plan.freeRegister += reservedRegisters
	for i, projection := range p.projections {
		generateExpressionTo(p.plan, projection.expr, startRegister+i)
	}
	p.plan.commands = append(p.plan.commands, &vm.ResultRowCmd{
		P1: startRegister,
		P2: reservedRegisters,
	})
}

type constantNode struct {
	parent logicalNode
	plan   *QueryPlan
}

func (c *constantNode) produce() {
	c.consume()
}

func (c *constantNode) consume() {
	c.parent.consume()
}

type countNode struct {
	plan       *QueryPlan
	projection projection
}

func (c *countNode) produce() {
	c.consume()
}

func (c *countNode) consume() {
	c.plan.commands = append(c.plan.commands, &vm.CountCmd{
		P1: c.plan.cursorId,
		P2: c.plan.freeRegister,
	})
	countRegister := c.plan.freeRegister
	countResults := 1
	c.plan.freeRegister += 1
	c.plan.commands = append(c.plan.commands, &vm.ResultRowCmd{
		P1: countRegister,
		P2: countResults,
	})
}

func (c *createNode) produce() {
	c.consume()
}

func (c *createNode) consume() {
	if c.noop {
		return
	}
	c.plan.commands = append(c.plan.commands, &vm.CreateBTreeCmd{P2: 1})
	c.plan.commands = append(c.plan.commands, &vm.NewRowIdCmd{P1: c.plan.cursorId, P2: 2})
	c.plan.commands = append(c.plan.commands, &vm.StringCmd{P1: 3, P4: c.objectType})
	c.plan.commands = append(c.plan.commands, &vm.StringCmd{P1: 4, P4: c.objectName})
	c.plan.commands = append(c.plan.commands, &vm.StringCmd{P1: 5, P4: c.tableName})
	c.plan.commands = append(c.plan.commands, &vm.CopyCmd{P1: 1, P2: 6})
	c.plan.commands = append(c.plan.commands, &vm.StringCmd{P1: 7, P4: string(c.schema)})
	c.plan.commands = append(c.plan.commands, &vm.MakeRecordCmd{P1: 3, P2: 5, P3: 8})
	c.plan.commands = append(c.plan.commands, &vm.InsertCmd{P1: c.plan.cursorId, P2: 8, P3: 2})
	c.plan.commands = append(c.plan.commands, &vm.ParseSchemaCmd{})
}

func (n *insertNode) produce() {
}

func (n *insertNode) consume() {
}

func (n *joinNode) produce() {
}

func (n *joinNode) consume() {
}
