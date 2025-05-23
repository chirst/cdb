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

func (p *projectNode) print() string {
	list := "("
	for i, proj := range p.projections {
		list += proj.print()
		if i+1 < len(p.projections) {
			list += ", "
		}
	}
	list += ")"
	return "project" + list
}

func (p *projection) print() string {
	if p.isCount {
		return "count(*)"
	}
	if p.colName == "" {
		return "<anonymous>"
	}
	return p.colName
}

func (s *scanNode) print() string {
	if s.scanPredicate != nil {
		return fmt.Sprintf("scan table %s with predicate", s.tableName)
	}
	return fmt.Sprintf("scan table %s", s.tableName)
}

func (c *constantNode) print() string {
	return "constant data source"
}

func (c *countNode) print() string {
	return fmt.Sprintf("count table %s", c.tableName)
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

func (p *projectNode) children() []logicalNode {
	return []logicalNode{p.child}
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
