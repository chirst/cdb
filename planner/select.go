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
		scanColumns, err := p.getScanColumns()
		if err != nil {
			return err
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
	projections, err := p.getProjections()
	if err != nil {
		return err
	}
	p.queryPlan = &projectNode{
		projections: projections,
		child:       child,
	}
	return nil
}

func (p *selectPlanner) getScanColumns() ([]scanColumn, error) {
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

func (p *selectPlanner) getProjections() ([]projection, error) {
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

// GetPlan returns the bytecode execution plan for the planner.
func (p *selectPlanner) GetPlan() (*vm.ExecutionPlan, error) {
	if err := p.getQueryPlan(); err != nil {
		return nil, err
	}
	p.newExecutionPlan()
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

func (p *selectPlanner) newExecutionPlan() {
	p.executionPlan = vm.NewExecutionPlan(p.catalogVersion)
	p.executionPlan.Explain = p.stmt.Explain
	if p.stmt.ExplainQueryPlan {
		p.executionPlan.FormattedTree = formatLogicalPlan(p.queryPlan)
	}
}

func (p *selectPlanner) resultHeader() {
	resultHeader := []string{}
	for _, p := range p.queryPlan.projections {
		resultHeader = append(resultHeader, p.colName)
	}
	p.executionPlan.ResultHeader = resultHeader
}

func (p *selectPlanner) buildInit() {
	p.executionPlan.Append(&vm.InitCmd{P2: 1})
	p.executionPlan.Append(&vm.TransactionCmd{P2: 0})
}

func (p *selectPlanner) buildScan(n *scanNode) error {
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

func (p *selectPlanner) buildOptimizedCountScan(n *countNode) {
	const cursorId = 1
	p.executionPlan.Append(&vm.OpenReadCmd{P1: cursorId, P2: n.rootPage})
	p.executionPlan.Append(&vm.CountCmd{P1: cursorId, P2: 1})
	p.executionPlan.Append(&vm.ResultRowCmd{P1: 1, P2: 1})
}
