package planner

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// QueryPlan contains the query plan tree. It is capable of converting the tree
// to a string representation for a query prefixed with `EXPLAIN QUERY PLAN`.
type QueryPlan struct {
	// plan holds the string representation also known as the tree.
	plan string
	// root holds the root node of the query plan
	root logicalNode
	// ExplainQueryPlan is a flag indicating if the SQL asked for the query plan
	// to be printed as a string representation with `EXPLAIN QUERY PLAN`.
	ExplainQueryPlan bool
}

func newQueryPlan(root logicalNode, explainQueryPlan bool) *QueryPlan {
	return &QueryPlan{
		root:             root,
		ExplainQueryPlan: explainQueryPlan,
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

func (p *projectNodeV2) print() string {
	return "project"
}

func (p *projectNodeV2) children() []logicalNode {
	return []logicalNode{}
}

func (s *scanNodeV2) print() string {
	return fmt.Sprintf("scan table")
}

func (c *constantNodeV2) print() string {
	return "constant data source"
}

func (c *countNodeV2) print() string {
	return fmt.Sprintf("count table")
}

func (j *joinNodeV2) print() string {
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

func (u *updateNodeV2) print() string {
	return "update"
}

func (s *scanNodeV2) children() []logicalNode {
	return []logicalNode{}
}

func (c *constantNodeV2) children() []logicalNode {
	return []logicalNode{}
}

func (c *countNodeV2) children() []logicalNode {
	return []logicalNode{}
}

func (j *joinNodeV2) children() []logicalNode {
	return []logicalNode{j.left, j.right}
}

func (c *createNode) children() []logicalNode {
	return []logicalNode{}
}

func (i *insertNode) children() []logicalNode {
	return []logicalNode{}
}

func (u *updateNodeV2) children() []logicalNode {
	return []logicalNode{}
}
