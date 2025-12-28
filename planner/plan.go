package planner

import (
	"fmt"
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

func (p *projectNode) print() string {
	return "project"
}

func (p *projectNode) children() []logicalNode {
	return []logicalNode{p.child}
}

func (s *scanNode) print() string {
	return "scan table"
}

func (c *constantNode) print() string {
	return "constant data source"
}

func (c *countNode) print() string {
	return "count table"
}

func (j *joinNode) print() string {
	return fmt.Sprint(j.operation)
}

func (c *createNode) print() string {
	if c.noop {
		return fmt.Sprintf("assert table %s does not exist", c.tableName)
	}
	return fmt.Sprintf("create table %s", c.tableName)
}

func (i *insertNode) print() string {
	return "insert"
}

func (u *updateNode) print() string {
	return "update"
}

func (f *filterNode) print() string {
	return "filter"
}

func (s *scanNode) children() []logicalNode {
	return []logicalNode{}
}

func (c *constantNode) children() []logicalNode {
	return []logicalNode{}
}

func (c *countNode) children() []logicalNode {
	return []logicalNode{}
}

func (j *joinNode) children() []logicalNode {
	return []logicalNode{j.left, j.right}
}

func (c *createNode) children() []logicalNode {
	return []logicalNode{}
}

func (i *insertNode) children() []logicalNode {
	return []logicalNode{}
}

func (u *updateNode) children() []logicalNode {
	return []logicalNode{}
}

func (f *filterNode) children() []logicalNode {
	return []logicalNode{f.child}
}
