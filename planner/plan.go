package planner

import (
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/chirst/cdb/vm"
)

// QueryPlan contains the query plan tree stemming from the root node. It is
// capable of converting the tree to a string representation for a query
// prefixed with `EXPLAIN QUERY PLAN`.
//
// The structure also holds the necessary data and receivers for generating a
// plan as well as the final commands that define the execution plan.
type QueryPlan struct {
	// plan holds the string representation also known as the tree.
	plan string
	// root is the root node of the plan tree.
	root logicalNode
	// ExplainQueryPlan is a flag indicating if the SQL asked for the query plan
	// to be printed as a string representation with `EXPLAIN QUERY PLAN`.
	ExplainQueryPlan bool
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

func newQueryPlan(
	root logicalNode,
	explainQueryPlan bool,
	transactionType transactionType,
	rootPageNumber int,
) *QueryPlan {
	return &QueryPlan{
		root:             root,
		ExplainQueryPlan: explainQueryPlan,
		commands:         []vm.Command{},
		constInts:        make(map[int]int),
		constStrings:     make(map[string]int),
		constVars:        make(map[int]int),
		freeRegister:     1,
		transactionType:  transactionType,
		cursorId:         1,
		rootPageNumber:   rootPageNumber,
	}
}

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

// compile sets byte code for the root node and it's children on commands.
func (p *QueryPlan) compile() {
	initCmd := &vm.InitCmd{}
	p.commands = append(p.commands, initCmd)
	p.root.produce()
	p.commands = append(p.commands, &vm.HaltCmd{})
	initCmd.P2 = len(p.commands)
	p.pushTransaction()
	// these constants are pushed ordered since maps are unordered making it
	// difficult to assert that a sequence of instructions appears.
	p.pushConstantInts()
	p.pushConstantStrings()
	p.pushConstantVars()
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
		temp = append(temp, &vm.StringCmd{P1: p.constStrings[v], P4: v})
	}
	slices.SortFunc(temp, func(a, b *vm.StringCmd) int {
		return a.P1 - b.P1
	})
	for i := range temp {
		p.commands = append(p.commands, temp[i])
	}
}

func (p *QueryPlan) pushConstantVars() {
	temp := []*vm.VariableCmd{}
	for v := range p.constVars {
		temp = append(temp, &vm.VariableCmd{P1: v, P2: p.constVars[v]})
	}
	slices.SortFunc(temp, func(a, b *vm.VariableCmd) int {
		return a.P2 - b.P2
	})
	for i := range temp {
		p.commands = append(p.commands, temp[i])
	}
}

// ToString evaluates and returns the query plan as a string representation.
func (p *QueryPlan) ToString() string {
	qp := &QueryPlan{}
	qp.walk(p.root, 0)
	qp.trimLeft()
	return qp.connectSiblings()
}

func (p *QueryPlan) walk(root logicalNode, depth int) {
	p.visit(root, depth+1)
	for _, c := range root.children() {
		p.walk(c, depth+1)
	}
}

func (p *QueryPlan) visit(ln logicalNode, depth int) {
	padding := ""
	for i := 0; i < depth; i += 1 {
		padding += "    "
	}
	if depth == 1 {
		padding += " ── "
	} else {
		padding += " └─ "
	}
	p.plan += fmt.Sprintf("%s%s\n", padding, ln.print())
}

// trimLeft performs extra formatting after the initial walk is completed.
func (p *QueryPlan) trimLeft() {
	trimBy := 4
	newPlan := []string{}
	for _, row := range strings.Split(p.plan, "\n") {
		newRow := row
		if len(row) >= trimBy {
			newRow = row[trimBy:]
		}
		newPlan = append(newPlan, newRow)
	}
	p.plan = strings.Join(newPlan, "\n")
}

// connectSiblings is a messy method to perform extra formatting after the
// initial recursive walk is completed. connectSiblings goes over the string
// representation in reverse row order and forwards column order. When a '└'
// character is found connectSiblings moves upwards on the current column making
// replacements until the top is reached. Once reached the column and row search
// continue.
func (p *QueryPlan) connectSiblings() string {
	planMatrix := strings.Split(p.plan, "\n")
	for rowIdx := len(planMatrix) - 1; 0 < rowIdx; rowIdx -= 1 {
		row := planMatrix[rowIdx]
		for charIdx, char := range row {
			if char == '└' {
				for backwardsRowIdx := rowIdx - 1; 0 < backwardsRowIdx; backwardsRowIdx -= 1 {
					if len(planMatrix[backwardsRowIdx]) < charIdx {
						continue
					}
					char, _ := utf8.DecodeRuneInString(planMatrix[backwardsRowIdx][charIdx:])
					if char == ' ' {
						out := []rune(planMatrix[backwardsRowIdx])
						out[charIdx] = '|'
						planMatrix[backwardsRowIdx] = string(out)
					}
					if char == '└' {
						out := []rune(planMatrix[backwardsRowIdx])
						out[charIdx] = '├'
						planMatrix[backwardsRowIdx] = string(out)
					}
				}
			}
		}
	}
	return strings.Join(planMatrix, "\n")
}
