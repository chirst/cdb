package planner

import (
	"fmt"

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

// planV2 holds the necessary data and receivers for generating a plan as well
// as the final commands that define the execution plan.
type planV2 struct {
	// root is the root node of the plan tree.
	root nodeV2
	// commands is a list of commands that define the plan.
	commands []vm.Command
	// constInts is a mapping of constant integer values to the registers that
	// contain the value.
	constInts map[int]int
	// constStrings is a mapping of constant string values to the registers that
	// contain the value.
	constStrings map[string]int
	// constVars is a mapping of a variable's position to the registers that
	// holds the variable's value.
	constVars map[int]int
	// freeRegister is a counter containing the next free register in the plan.
	freeRegister int
	// transactionType defines what kind of transaction the plan will need.
	transactionType transactionType
	// cursorId is the id of the cursor the plan is using. Note plans will
	// eventually need to use more than one cursor, but for now it is convenient
	// to pull the id from here.
	cursorId int
	// rootPageNumber is the root page number of the table cursorId is
	// associated with. This should be a map at some point when multiple tables
	// can be queried in one plan.
	rootPageNumber int
}

func generateUpdate() {
	logicalPlan := &planV2{
		commands:        []vm.Command{},
		constInts:       make(map[int]int),
		constStrings:    make(map[string]int),
		freeRegister:    1,
		transactionType: transactionTypeWrite,
		cursorId:        1,
	}
	un := &updateNodeV2{
		plan: logicalPlan,
		updateExprs: []compiler.Expr{
			&compiler.ColumnRef{
				IsPrimaryKey: false,
				ColIdx:       0,
			},
		},
	}
	fn := &filterNodeV2{
		plan:      logicalPlan,
		predicate: &compiler.IntLit{Value: 277},
	}
	fn.parent = un
	un.child = fn
	sn := &scanNodeV2{
		plan: logicalPlan,
	}
	sn.parent = fn
	fn.child = sn
	logicalPlan.root = un
	logicalPlan.compile()
	for i := range logicalPlan.commands {
		fmt.Printf("%d %#v\n", i+1, logicalPlan.commands[i])
	}
}

func generateSelect() {
	logicalPlan := &planV2{
		commands:        []vm.Command{},
		constInts:       make(map[int]int),
		constStrings:    make(map[string]int),
		freeRegister:    1,
		transactionType: transactionTypeRead,
		cursorId:        1,
	}
	pn := &projectNodeV2{
		plan: logicalPlan,
	}
	fn := &filterNodeV2{
		plan: logicalPlan,
	}
	fn.parent = pn
	pn.child = fn
	sn := &scanNodeV2{
		plan: logicalPlan,
	}
	sn.parent = fn
	fn.child = sn
	logicalPlan.root = pn
	logicalPlan.compile()
	for i := range logicalPlan.commands {
		fmt.Printf("%d %#v\n", i+1, logicalPlan.commands[i])
	}
}

// declareConstInt gets or sets a register with the const value and returns the
// register. It is guaranteed the value will be in the register for the duration
// of the plan.
func (p *planV2) declareConstInt(i int) int {
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
func (p *planV2) declareConstString(s string) int {
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
func (p *planV2) declareConstVar(position int) int {
	_, ok := p.constVars[position]
	if !ok {
		p.constVars[position] = p.freeRegister
		p.freeRegister += 1
	}
	return p.constVars[position]
}

func (p *planV2) compile() {
	initCmd := &vm.InitCmd{}
	p.commands = append(p.commands, initCmd)
	p.root.produce()
	p.commands = append(p.commands, &vm.HaltCmd{})
	initCmd.P2 = len(p.commands) + 1
	p.pushTransaction()
	p.pushConstants()
	p.commands = append(p.commands, &vm.GotoCmd{P1: 2})
}

func (p *planV2) pushTransaction() {
	switch p.transactionType {
	case transactionTypeNone:
		return
	case transactionTypeRead:
		p.commands = append(
			p.commands,
			&vm.TransactionCmd{P2: int(p.transactionType)},
		)
		p.commands = append(
			p.commands,
			&vm.OpenReadCmd{P1: p.cursorId, P2: p.rootPageNumber},
		)
	case transactionTypeWrite:
		p.commands = append(
			p.commands,
			&vm.TransactionCmd{P2: int(p.transactionType)},
		)
		p.commands = append(
			p.commands,
			&vm.OpenWriteCmd{P1: p.cursorId, P2: p.rootPageNumber},
		)
	default:
		panic("unexpected transaction type")
	}
}

func (p *planV2) pushConstants() {
	for v := range p.constInts {
		p.commands = append(p.commands, &vm.IntegerCmd{P1: v, P2: p.constInts[v]})
	}
	for v := range p.constStrings {
		p.commands = append(p.commands, &vm.StringCmd{P1: p.constStrings[v], P4: v})
	}
	for v := range p.constVars {
		p.commands = append(p.commands, &vm.VariableCmd{P1: p.constVars[v], P2: v})
	}
}

type nodeV2 interface {
	produce()
	consume()
}

type updateNodeV2 struct {
	child nodeV2
	plan  *planV2
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

func (u *updateNodeV2) produce() {
	u.child.produce()
}

func (u *updateNodeV2) consume() {
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
	endRecordRegister := u.plan.freeRegister
	for _, e := range u.updateExprs {
		generateExpressionTo(u.plan, e, startRecordRegister)
		startRecordRegister += 1
	}

	// Make the record for inserting
	u.plan.commands = append(u.plan.commands, &vm.MakeRecordCmd{
		P1: startRecordRegister,
		P2: endRecordRegister,
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

type filterNodeV2 struct {
	child     nodeV2
	parent    nodeV2
	plan      *planV2
	predicate compiler.Expr
}

func (f *filterNodeV2) produce() {
	f.child.produce()
}

func (f *filterNodeV2) consume() {
	if f.predicate == nil {
		f.parent.consume()
		return
	}
	jumpCommand := generatePredicate(f.plan, f.predicate)
	f.parent.consume()
	jumpCommand.SetJumpAddress(len(f.plan.commands) + 1)
}

type scanNodeV2 struct {
	parent nodeV2
	plan   *planV2
}

func (s *scanNodeV2) produce() {
	s.consume()
}

func (s *scanNodeV2) consume() {
	rewindCmd := &vm.RewindCmd{P1: s.plan.cursorId}
	s.plan.commands = append(s.plan.commands, rewindCmd)
	loopBeginAddress := len(s.plan.commands) + 1
	s.parent.consume()
	s.plan.commands = append(s.plan.commands, &vm.NextCmd{
		P1: s.plan.cursorId,
		P2: loopBeginAddress,
	})
	rewindCmd.P2 = len(s.plan.commands) + 1
}

type projectNodeV2 struct {
	child nodeV2
	plan  *planV2
}

func (p *projectNodeV2) produce() {
	p.child.produce()
}

func (p *projectNodeV2) consume() {
	startRegister := p.plan.freeRegister
	p.plan.commands = append(p.plan.commands, &vm.RowIdCmd{
		P1: p.plan.cursorId,
		P2: p.plan.freeRegister,
	})
	p.plan.freeRegister += 1
	p.plan.commands = append(p.plan.commands, &vm.ColumnCmd{
		P1: p.plan.cursorId,
		P2: 0,
		P3: p.plan.freeRegister,
	})
	p.plan.freeRegister += 1
	p.plan.commands = append(p.plan.commands, &vm.ResultRowCmd{
		P1: startRegister,
		P2: p.plan.freeRegister - startRegister,
	})
}

type constantNodeV2 struct {
	parent nodeV2
	plan   *planV2
}

func (c *constantNodeV2) produce() {
	c.consume()
}

func (c *constantNodeV2) consume() {
	c.parent.consume()
}
