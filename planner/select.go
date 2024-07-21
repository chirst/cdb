package planner

import (
	"errors"

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

// selectPlanner is capable of generating a logical query plan and a physical
// execution plan for a select statement. The planners within are separated by
// their responsibility. Notice a statement or catalog is not shared with with
// the execution planner. This is by design since the logical query planner also
// performs binding.
type selectPlanner struct {
	qp *selectQueryPlanner
	ep *selectExecutionPlanner
}

// selectQueryPlanner converts an AST to a logical query plan. Along the way it
// also validates the AST makes sense with the catalog (a process known as
// binding).
type selectQueryPlanner struct {
	// catalog contains the schema
	catalog selectCatalog
	// stmt contains the AST
	stmt *compiler.SelectStmt
	// queryPlan contains the logical plan. The root node must be a projection.
	queryPlan *projectNode
}

// selectExecutionPlanner converts logical nodes in a query plan tree to
// bytecode that can be run by the vm.
type selectExecutionPlanner struct {
	// queryPlan contains the logical plan. This node is populated by calling
	// the QueryPlan method.
	queryPlan *projectNode
	// executionPlan contains the execution plan for the vm. This is built by
	// calling ExecutionPlan.
	executionPlan *vm.ExecutionPlan
}

// NewSelect returns an instance of a select planner for the given AST.
func NewSelect(catalog selectCatalog, stmt *compiler.SelectStmt) *selectPlanner {
	return &selectPlanner{
		qp: &selectQueryPlanner{
			catalog: catalog,
			stmt:    stmt,
		},
		ep: &selectExecutionPlanner{
			executionPlan: vm.NewExecutionPlan(
				catalog.GetVersion(),
				stmt.Explain,
			),
		},
	}
}

// QueryPlan generates the query plan tree for the planner.
func (p *selectPlanner) QueryPlan() (*QueryPlan, error) {
	tableName := p.qp.stmt.From.TableName
	rootPageNumber, err := p.qp.catalog.GetRootPageNumber(tableName)
	if err != nil {
		return nil, err
	}
	var child logicalNode
	if p.qp.stmt.ResultColumn.All {
		scanColumns, err := p.qp.getScanColumns()
		if err != nil {
			return nil, err
		}
		child = &scanNode{
			tableName:   tableName,
			rootPage:    rootPageNumber,
			scanColumns: scanColumns,
		}
	} else {
		child = &countNode{
			tableName: tableName,
			rootPage:  rootPageNumber,
		}
	}
	projections, err := p.qp.getProjections()
	if err != nil {
		return nil, err
	}
	p.qp.queryPlan = &projectNode{
		projections: projections,
		child:       child,
	}
	p.ep.queryPlan = p.qp.queryPlan
	return &QueryPlan{
		root:             p.qp.queryPlan,
		ExplainQueryPlan: p.qp.stmt.ExplainQueryPlan,
	}, nil
}

func (p *selectQueryPlanner) getScanColumns() ([]scanColumn, error) {
	pkColName, err := p.catalog.GetPrimaryKeyColumn(p.stmt.From.TableName)
	if err != nil {
		return nil, err
	}
	cols, err := p.catalog.GetColumns(p.stmt.From.TableName)
	if err != nil {
		return nil, err
	}
	scanColumns := []scanColumn{}
	idx := 0
	for _, c := range cols {
		if c == pkColName {
			scanColumns = append(scanColumns, scanColumn{
				isPrimaryKey: c == pkColName,
			})
		} else {
			scanColumns = append(scanColumns, scanColumn{
				colIdx: idx,
			})
			idx += 1
		}
	}
	return scanColumns, nil
}

func (p *selectQueryPlanner) getProjections() ([]projection, error) {
	if p.stmt.ResultColumn.All {
		cols, err := p.catalog.GetColumns(p.stmt.From.TableName)
		if err != nil {
			return nil, err
		}
		projections := []projection{}
		for _, c := range cols {
			projections = append(projections, projection{
				colName: c,
			})
		}
		return projections, nil
	} else if p.stmt.ResultColumn.Count {
		return []projection{
			{
				isCount: true,
			},
		}, nil
	}
	return nil, errors.New("unhandled projection")
}

// ExecutionPlan returns the bytecode execution plan for the planner. Calling
// QueryPlan is not a prerequisite to this method as it will be called by
// ExecutionPlan if needed.
func (sp *selectPlanner) ExecutionPlan() (*vm.ExecutionPlan, error) {
	if sp.qp.queryPlan == nil {
		_, err := sp.QueryPlan()
		if err != nil {
			return nil, err
		}
	}
	p := sp.ep
	p.resultHeader()
	p.buildInit()

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

func (p *selectExecutionPlanner) resultHeader() {
	resultHeader := []string{}
	for _, p := range p.queryPlan.projections {
		resultHeader = append(resultHeader, p.colName)
	}
	p.executionPlan.ResultHeader = resultHeader
}

func (p *selectExecutionPlanner) buildInit() {
	p.executionPlan.Append(&vm.InitCmd{P2: 1})
	p.executionPlan.Append(&vm.TransactionCmd{P2: 0})
}

func (p *selectExecutionPlanner) buildScan(n *scanNode) error {
	const cursorId = 1
	p.executionPlan.Append(&vm.OpenReadCmd{P1: cursorId, P2: n.rootPage})

	rwc := &vm.RewindCmd{P1: cursorId}
	p.executionPlan.Append(rwc)

	for i, c := range n.scanColumns {
		register := i + 1
		if c.isPrimaryKey {
			p.executionPlan.Append(&vm.RowIdCmd{P1: cursorId, P2: register})
		} else {
			p.executionPlan.Append(&vm.ColumnCmd{P1: cursorId, P2: c.colIdx, P3: register})
		}
	}
	p.executionPlan.Append(&vm.ResultRowCmd{P1: 1, P2: len(n.scanColumns)})

	p.executionPlan.Append(&vm.NextCmd{P1: cursorId, P2: 4})

	rwc.P2 = len(p.executionPlan.Commands)
	return nil
}

func (p *selectExecutionPlanner) buildOptimizedCountScan(n *countNode) {
	const cursorId = 1
	p.executionPlan.Append(&vm.OpenReadCmd{P1: cursorId, P2: n.rootPage})
	p.executionPlan.Append(&vm.CountCmd{P1: cursorId, P2: 1})
	p.executionPlan.Append(&vm.ResultRowCmd{P1: 1, P2: 1})
}
