package planner

import (
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
	// catalog contains the schema
	catalog selectCatalog
	// stmt contains the AST
	stmt *compiler.SelectStmt
	// queryPlan contains the logical plan. The root node must be a projection.
	queryPlan *projectNode
	// executionPlan contains the execution plan for the vm
	executionPlan *vm.ExecutionPlan
	// catalogVersion contains the version of catalog this query plan was
	// generated with. The catalog version is used for concurrency control.
	catalogVersion string
}

// NewSelect returns an instance of a select planner for the given AST.
func NewSelect(catalog selectCatalog, stmt *compiler.SelectStmt) *selectPlanner {
	return &selectPlanner{
		catalog:        catalog,
		stmt:           stmt,
		catalogVersion: catalog.GetVersion(),
	}
}

// getQueryPlan generates the query plan tree for the planner.
func (p *selectPlanner) getQueryPlan() error {
	tableName := p.stmt.From.TableName
	rootPageNumber, err := p.catalog.GetRootPageNumber(tableName)
	if err != nil {
		return err
	}
	var child logicalNode
	if p.stmt.ResultColumn.All {
		child = &scanNode{
			tableName: tableName,
			rootPage:  rootPageNumber,
		}
	} else {
		child = &countNode{
			tableName: tableName,
			rootPage:  rootPageNumber,
		}
	}
	p.queryPlan = &projectNode{
		projections: []projection{
			{
				isAll:   p.stmt.ResultColumn.All,
				isCount: p.stmt.ResultColumn.Count,
			},
		},
		child: child,
	}
	return nil
}

// GetPlan returns the bytecode execution plan for the planner.
func (p *selectPlanner) GetPlan() (*vm.ExecutionPlan, error) {
	if err := p.getQueryPlan(); err != nil {
		return nil, err
	}
	p.newExecutionPlan()
	if err := p.resultHeader(); err != nil {
		return nil, err
	}
	p.buildInit()

	// TODO should handle more than one projection. Possibly needs to work with
	// result header.
	// projection := p.queryPlan.projections[0]
	switch c := p.queryPlan.child.(type) {
	case *scanNode:
		if err := p.buildScan(c); err != nil {
			return nil, err
		}
	case *countNode:
		p.buildOptimizedCountScan(c)
	default:
		panic("unhandled node")
	}
	p.executionPlan.Append(&vm.HaltCmd{})
	return p.executionPlan, nil
}

func (p *selectPlanner) newExecutionPlan() {
	p.executionPlan = vm.NewExecutionPlan(p.catalogVersion)
	p.executionPlan.Explain = p.stmt.Explain
	if p.stmt.ExplainQueryPlan {
		p.executionPlan.FormattedTree = formatLogicalPlan(p.queryPlan)
	}
}

func (p *selectPlanner) resultHeader() error {
	resultHeader := []string{}
	cols, err := p.catalog.GetColumns(p.stmt.From.TableName)
	if err != nil {
		return err
	}
	if p.stmt.ResultColumn.All {
		resultHeader = append(resultHeader, cols...)
	} else if p.stmt.ResultColumn.Count {
		resultHeader = append(resultHeader, "")
	}
	p.executionPlan.ResultHeader = resultHeader
	return nil
}

func (p *selectPlanner) buildInit() {
	p.executionPlan.Append(&vm.InitCmd{P2: 1})
	p.executionPlan.Append(&vm.TransactionCmd{P2: 0})
}

func (p *selectPlanner) buildScan(n *scanNode) error {
	// Open read cursor
	const cursorId = 1
	p.executionPlan.Append(&vm.OpenReadCmd{P1: cursorId, P2: n.rootPage})

	// Rewind to begin loop for scan
	rwc := &vm.RewindCmd{P1: cursorId}
	p.executionPlan.Append(rwc)

	// Projections within loop
	pkColName, err := p.catalog.GetPrimaryKeyColumn(p.stmt.From.TableName)
	if err != nil {
		return err
	}
	cols, err := p.catalog.GetColumns(p.stmt.From.TableName)
	if err != nil {
		return err
	}
	registerIdx := 1
	gap := 0
	colIdx := 0
	for _, c := range cols {
		if c == pkColName {
			p.executionPlan.Append(&vm.RowIdCmd{P1: cursorId, P2: registerIdx})
		} else {
			p.executionPlan.Append(&vm.ColumnCmd{P1: cursorId, P2: colIdx, P3: registerIdx})
			colIdx += 1
		}
		registerIdx += 1
		gap += 1
	}
	p.executionPlan.Append(&vm.ResultRowCmd{P1: 1, P2: gap})

	// Loop or break
	p.executionPlan.Append(&vm.NextCmd{P1: cursorId, P2: 4})

	// Set rewind to jump to the address after NextCmd if the table is empty.
	rwc.P2 = len(p.executionPlan.Commands)
	return nil
}

func (p *selectPlanner) buildOptimizedCountScan(n *countNode) {
	const cursorId = 1
	p.executionPlan.Append(&vm.OpenReadCmd{P1: cursorId, P2: n.rootPage})
	p.executionPlan.Append(&vm.CountCmd{P1: cursorId, P2: 1})
	p.executionPlan.Append(&vm.ResultRowCmd{P1: 1, P2: 1})
}
