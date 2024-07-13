package planner

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/vm"
)

// selectCatalog defines the catalog methods needed by the select planner
type selectCatalog interface {
	GetColumns(tableOrIndexName string) ([]string, error)
	GetRootPageNumber(tableOrIndexName string) (int, error)
	GetVersion() string
	GetPrimaryKeyColumn(tableName string) (string, error)
}

type selectPlanner struct {
	catalog selectCatalog
}

func NewSelect(catalog selectCatalog) *selectPlanner {
	return &selectPlanner{
		catalog: catalog,
	}
}

func (p *selectPlanner) GetPlan(s *compiler.SelectStmt) (*vm.ExecutionPlan, error) {
	executionPlan := vm.NewExecutionPlan(p.catalog.GetVersion())
	executionPlan.Explain = s.Explain
	executionPlan.ExplainQueryPlan = s.ExplainQueryPlan
	lp := p.getLogicalPlan(s)
	if executionPlan.ExplainQueryPlan {
		printer := &printLogicalNodeVisitor{}
		walkLogicalTree(lp, printer, 0)
		executionPlan.FormattedTree = connectSiblings(printer.plan)
		return executionPlan, nil
	}
	resultHeader := []string{}
	cols, err := p.catalog.GetColumns(s.From.TableName)
	if err != nil {
		return nil, err
	}
	if s.ResultColumn.All {
		resultHeader = append(resultHeader, cols...)
	} else if s.ResultColumn.Count {
		resultHeader = append(resultHeader, "")
	}
	rootPage, err := p.catalog.GetRootPageNumber(s.From.TableName)
	if err != nil {
		return nil, err
	}
	cursorId := 1
	commands := []vm.Command{}
	commands = append(commands, &vm.InitCmd{P2: 1})
	commands = append(commands, &vm.TransactionCmd{P2: 0})
	commands = append(commands, &vm.OpenReadCmd{P1: cursorId, P2: rootPage})
	if s.ResultColumn.All {
		rwc := &vm.RewindCmd{P1: cursorId}
		commands = append(commands, rwc)
		pkColName, err := p.catalog.GetPrimaryKeyColumn(s.From.TableName)
		if err != nil {
			return nil, err
		}
		registerIdx := 1
		gap := 0
		colIdx := 0
		for _, c := range cols {
			if c == pkColName {
				commands = append(commands, &vm.RowIdCmd{P1: cursorId, P2: registerIdx})
			} else {
				commands = append(commands, &vm.ColumnCmd{P1: cursorId, P2: colIdx, P3: registerIdx})
				colIdx += 1
			}
			registerIdx += 1
			gap += 1
		}
		commands = append(commands, &vm.ResultRowCmd{P1: 1, P2: gap})
		commands = append(commands, &vm.NextCmd{P1: cursorId, P2: 4})
		commands = append(commands, &vm.HaltCmd{})
		rwc.P2 = len(commands) - 1
	} else {
		commands = append(commands, &vm.CountCmd{P1: cursorId, P2: 1})
		commands = append(commands, &vm.ResultRowCmd{P1: 1, P2: 1})
		commands = append(commands, &vm.HaltCmd{})
	}
	executionPlan.Commands = commands
	executionPlan.ResultHeader = resultHeader
	return executionPlan, nil
}

func walkLogicalTree(root logicalNode, lnv logicalNodeVisitor, depth int) {
	root.accept(lnv, depth+1)
	for _, c := range root.children() {
		walkLogicalTree(c, lnv, depth+1)
	}
}

type logicalNodeVisitor interface {
	visit(ln logicalNode, depth int)
}

type printLogicalNodeVisitor struct {
	plan string
}

func (p *printLogicalNodeVisitor) visit(ln logicalNode, depth int) {
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

type logicalNode interface {
	print() string
	accept(v logicalNodeVisitor, depth int)
	children() []logicalNode
}

type projectNode struct {
	projections []projection
	child       logicalNode
}

type projection struct {
	isAll   bool
	isCount bool
}

func (p *projection) print() string {
	if p.isAll {
		return "*"
	}
	return "count(*)"
}

type scanNode struct {
	tableName string
}

type joinNode struct {
	left      logicalNode
	right     logicalNode
	operation string
}

func (p *projectNode) accept(v logicalNodeVisitor, depth int) {
	v.visit(p, depth)
}

func (s *scanNode) accept(v logicalNodeVisitor, depth int) {
	v.visit(s, depth)
}

func (j *joinNode) accept(v logicalNodeVisitor, depth int) {
	v.visit(j, depth)
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

func (s *scanNode) print() string {
	return fmt.Sprintf("scan table %s", s.tableName)
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

func (j *joinNode) children() []logicalNode {
	return []logicalNode{j.left, j.right}
}

func (p *selectPlanner) getLogicalPlan(s *compiler.SelectStmt) logicalNode {
	// return &projectNode{
	// 	projections: []projection{
	// 		{
	// 			isAll:   s.ResultColumn.All,
	// 			isCount: s.ResultColumn.Count,
	// 		},
	// 	},
	// 	child: &scanNode{
	// 		tableName: s.From.TableName,
	// 	},
	// }
	return &projectNode{
		projections: []projection{
			{
				isAll:   true,
				isCount: false,
			},
		},
		child: &joinNode{
			operation: "join",
			left: &joinNode{
				operation: "join",
				left: &scanNode{
					tableName: "foo",
				},
				right: &joinNode{
					operation: "join",
					left: &scanNode{
						tableName: "baz",
					},
					right: &scanNode{
						tableName: "buzz",
					},
				},
			},
			right: &scanNode{
				tableName: "bar",
			},
		},
	}
}

func connectSiblings(rawPlan string) string {
	planMatrix := strings.Split(rawPlan, "\n")
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
