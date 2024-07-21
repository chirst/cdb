package planner

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// QueryPlan contains the query plan tree. It is capable of converting  the tree
// to a string representation for a query prefixed with `EXPLAIN QUERY PLAN`.
type QueryPlan struct {
	plan             string
	root             logicalNode
	ExplainQueryPlan bool
}

func (p *QueryPlan) ToString() string {
	qp := &QueryPlan{}
	qp.walk(p.root, 0)
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
	if depth != 0 {
		padding += " └─ "
	} else {
		padding += " ── "
	}
	p.plan += fmt.Sprintf("%s%s\n", padding, ln.print())
}

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
	return p.colName
}

func (s *scanNode) print() string {
	return fmt.Sprintf("scan table %s", s.tableName)
}

func (c *countNode) print() string {
	return fmt.Sprintf("count table %s", c.tableName)
}

func (j *joinNode) print() string {
	return fmt.Sprint(j.operation)
}

func (p *projectNode) children() []logicalNode {
	return []logicalNode{p.child}
}

func (s *scanNode) children() []logicalNode {
	return []logicalNode{}
}

func (c *countNode) children() []logicalNode {
	return []logicalNode{}
}

func (j *joinNode) children() []logicalNode {
	return []logicalNode{j.left, j.right}
}
