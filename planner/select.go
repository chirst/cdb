package planner

import (
	"errors"
	"fmt"
	"slices"

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
// their responsibility.
type selectPlanner struct {
	// queryPlanner is responsible for transforming the AST to a logical query
	// plan tree. This tree is made up of nodes that map closely to a relational
	// algebra tree. The query planner also performs binding and validation.
	queryPlanner *selectQueryPlanner
	// executionPlanner transforms the logical query tree to a bytecode routine,
	// built to be ran by the virtual machine.
	executionPlanner *selectExecutionPlanner
}

// selectQueryPlanner converts an AST to a logical query plan. Along the way it
// also validates the AST makes sense with the catalog (a process known as
// binding).
type selectQueryPlanner struct {
	// catalog contains the schema
	catalog selectCatalog
	// stmt contains the AST
	stmt *compiler.SelectStmt
	// queryPlan contains the logical plan being built. The root node must be a
	// projection.
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
		queryPlanner: &selectQueryPlanner{
			catalog: catalog,
			stmt:    stmt,
		},
		executionPlanner: &selectExecutionPlanner{
			executionPlan: vm.NewExecutionPlan(
				catalog.GetVersion(),
				stmt.Explain,
			),
		},
	}
}

// QueryPlan generates the query plan tree for the planner.
func (p *selectPlanner) QueryPlan() (*QueryPlan, error) {
	qp, err := p.queryPlanner.getQueryPlan()
	if err != nil {
		return nil, err
	}
	p.executionPlanner.queryPlan = p.queryPlanner.queryPlan
	return qp, err
}

func (p *selectQueryPlanner) getQueryPlan() (*QueryPlan, error) {
	tableName := p.stmt.From.TableName
	rootPageNumber, err := p.catalog.GetRootPageNumber(tableName)
	if err != nil {
		return nil, err
	}
	var child logicalNode
	for _, resultColumn := range p.stmt.ResultColumns {
		if resultColumn.All {
			scanColumns, err := p.getScanColumns()
			if err != nil {
				return nil, err
			}
			switch c := child.(type) {
			case *scanNode:
				c.scanColumns = append(c.scanColumns, scanColumns...)
			case nil:
				child = &scanNode{
					tableName:   tableName,
					rootPage:    rootPageNumber,
					scanColumns: scanColumns,
				}
			default:
				return nil, errors.New("expected scanNode")
			}
		} else if resultColumn.Expression != nil {
			switch e := resultColumn.Expression.(type) {
			case *compiler.ColumnRef:
				if e.Table == "" {
					e.Table = p.stmt.From.TableName
				}
				cols, err := p.catalog.GetColumns(e.Table)
				if err != nil {
					return nil, err
				}
				colIdx := slices.Index(cols, e.Column)
				pkCol, err := p.catalog.GetPrimaryKeyColumn(e.Table)
				if err != nil {
					return nil, err
				}
				pkColIdx := slices.Index(cols, pkCol)
				if pkColIdx < colIdx {
					colIdx -= 1
				}
				switch c := child.(type) {
				case *scanNode:
					c.scanColumns = append(c.scanColumns, scanColumn{
						isPrimaryKey: pkCol == e.Column,
						colIdx:       colIdx,
					})
				case nil:
					child = &scanNode{
						tableName: e.Table,
						rootPage:  rootPageNumber,
						scanColumns: []scanColumn{
							{
								isPrimaryKey: pkCol == e.Column,
								colIdx:       colIdx,
							},
						},
					}
				default:
					return nil, fmt.Errorf("expected scan node but got %#v", c)
				}
			case *compiler.FunctionExpr:
				if e.FnType != compiler.FnCount {
					return nil, fmt.Errorf(
						"only %s function is supported", e.FnType,
					)
				}
				switch child.(type) {
				case nil:
					child = &countNode{
						tableName: tableName,
						rootPage:  rootPageNumber,
					}
				default:
					return nil, errors.New(
						"count with other result columns not supported",
					)
				}
			default:
				return nil, fmt.Errorf(
					"unhandled expression for result column %#v", resultColumn,
				)
			}
		} else {
			return nil, fmt.Errorf("unhandled result column %#v", resultColumn)
		}
	}
	projections, err := p.getProjections()
	if err != nil {
		return nil, err
	}
	p.queryPlan = &projectNode{
		projections: projections,
		child:       child,
	}
	return newQueryPlan(p.queryPlan, p.stmt.ExplainQueryPlan), nil
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
	var projections []projection
	for _, resultColumn := range p.stmt.ResultColumns {
		if resultColumn.All {
			cols, err := p.catalog.GetColumns(p.stmt.From.TableName)
			if err != nil {
				return nil, err
			}
			for _, c := range cols {
				projections = append(projections, projection{
					colName: c,
				})
			}
		} else if resultColumn.Expression != nil {
			switch e := resultColumn.Expression.(type) {
			case *compiler.ColumnRef:
				colName := e.Column
				if resultColumn.Alias != "" {
					colName = resultColumn.Alias
				}
				projections = append(projections, projection{
					colName: colName,
				})
			case *compiler.FunctionExpr:
				projections = append(projections, projection{
					isCount: true,
					colName: resultColumn.Alias,
				})
			default:
				return nil, fmt.Errorf("unhandled result column expression %#v", e)
			}
		}
	}
	return projections, nil
}

// ExecutionPlan returns the bytecode execution plan for the planner. Calling
// QueryPlan is not a prerequisite to this method as it will be called by
// ExecutionPlan if needed.
func (sp *selectPlanner) ExecutionPlan() (*vm.ExecutionPlan, error) {
	if sp.queryPlanner.queryPlan == nil {
		_, err := sp.QueryPlan()
		if err != nil {
			return nil, err
		}
	}
	return sp.executionPlanner.getExecutionPlan()
}

func (p *selectExecutionPlanner) getExecutionPlan() (*vm.ExecutionPlan, error) {
	p.setResultHeader()
	p.buildInit()
	switch c := p.queryPlan.child.(type) {
	case *scanNode:
		if err := p.buildScan(c); err != nil {
			return nil, err
		}
	case *countNode:
		p.buildOptimizedCountScan(c)
	default:
		return nil, fmt.Errorf("unhandled node %#v", c)
	}
	p.executionPlan.Append(&vm.HaltCmd{})
	return p.executionPlan, nil
}

func (p *selectExecutionPlanner) setResultHeader() {
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

// buildOptimizedCountScan is a special optimization made when a table only has
// a count aggregate and no other projections. Since the optimized scan
// aggregates the count of tuples on each page, but does not look at individual
// tuples.
func (p *selectExecutionPlanner) buildOptimizedCountScan(n *countNode) {
	const cursorId = 1
	p.executionPlan.Append(&vm.OpenReadCmd{P1: cursorId, P2: n.rootPage})
	p.executionPlan.Append(&vm.CountCmd{P1: cursorId, P2: 1})
	p.executionPlan.Append(&vm.ResultRowCmd{P1: 1, P2: 1})
}
