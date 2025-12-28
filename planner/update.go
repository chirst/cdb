package planner

import (
	"errors"
	"slices"

	"github.com/chirst/cdb/catalog"
	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/vm"
)

// updateCatalog is the required catalog methods for the update planner.
type updateCatalog interface {
	GetVersion() string
	GetRootPageNumber(string) (int, error)
	GetColumns(string) ([]string, error)
	GetPrimaryKeyColumn(string) (string, error)
	GetColumnType(tableName string, columnName string) (catalog.CdbType, error)
}

// updatePanner houses the query planner and execution planner for a update
// statement.
type updatePlanner struct {
	queryPlanner     *updateQueryPlanner
	executionPlanner *updateExecutionPlanner
}

// updateQueryPlanner generates a queryPlan for the given update statement.
type updateQueryPlanner struct {
	catalog   updateCatalog
	stmt      *compiler.UpdateStmt
	queryPlan *updateNode
}

// updateExecutionPlanner generates a byte code routine for the given queryPlan.
type updateExecutionPlanner struct {
	queryPlan     *updateNode
	executionPlan *vm.ExecutionPlan
}

// NewUpdate create a update planner.
func NewUpdate(catalog updateCatalog, stmt *compiler.UpdateStmt) *updatePlanner {
	return &updatePlanner{
		queryPlanner: &updateQueryPlanner{
			catalog: catalog,
			stmt:    stmt,
		},
		executionPlanner: &updateExecutionPlanner{
			executionPlan: vm.NewExecutionPlan(
				catalog.GetVersion(),
				stmt.Explain,
			),
		},
	}
}

// QueryPlan sets up a high level plan to be passed to ExecutionPlan.
func (p *updatePlanner) QueryPlan() (*QueryPlan, error) {
	qp, err := p.queryPlanner.getQueryPlan()
	if err != nil {
		return nil, err
	}
	p.executionPlanner.queryPlan = p.queryPlanner.queryPlan
	return qp, err
}

// getQueryPlan returns a updateNode with a high level plan.
func (p *updateQueryPlanner) getQueryPlan() (*QueryPlan, error) {
	rootPage, err := p.catalog.GetRootPageNumber(p.stmt.TableName)
	if err != nil {
		return nil, errTableNotExist
	}
	updateNode := &updateNode{updateExprs: []compiler.Expr{}}
	logicalPlan := newQueryPlan(
		updateNode,
		p.stmt.ExplainQueryPlan,
		transactionTypeWrite,
		rootPage,
	)
	updateNode.plan = logicalPlan
	p.queryPlan = updateNode
	logicalPlan.root = updateNode

	if err := p.errIfPrimaryKeySet(); err != nil {
		return nil, err
	}

	if err := p.errIfSetNotOnDestinationTable(); err != nil {
		return nil, err
	}

	if err := p.setQueryPlanRecordExpressions(); err != nil {
		return nil, err
	}

	scanNode := &scanNode{
		plan: logicalPlan,
	}
	if p.stmt.Predicate != nil {
		cev := &catalogExprVisitor{}
		cev.Init(p.catalog, p.stmt.TableName)
		p.stmt.Predicate.BreadthWalk(cev)
		filterNode := &filterNode{
			plan:      logicalPlan,
			predicate: p.stmt.Predicate,
			parent:    updateNode,
			child:     scanNode,
		}
		updateNode.child = filterNode
		scanNode.parent = filterNode
	} else {
		scanNode.parent = updateNode
		updateNode.child = scanNode
	}

	return logicalPlan, nil
}

// errIfPrimaryKeySet checks the primary key isn't being updated because it
// could cause an infinite loop if not handled properly.
func (p *updateQueryPlanner) errIfPrimaryKeySet() error {
	pkColumnName, err := p.catalog.GetPrimaryKeyColumn(p.stmt.TableName)
	if err != nil {
		return err
	}
	if _, ok := p.stmt.SetList[pkColumnName]; ok {
		return errors.New("updating primary key not supported")
	}
	return nil
}

// errIfSetNotOnDestinationTable checks the set list has column names that are
// part of the table being updated.
func (p *updateQueryPlanner) errIfSetNotOnDestinationTable() error {
	schemaColumns, err := p.catalog.GetColumns(p.stmt.TableName)
	if err != nil {
		return err
	}
	for colName := range p.stmt.SetList {
		if !slices.Contains(schemaColumns, colName) {
			return errSetColumnNotExist
		}
	}
	return nil
}

// setQueryPlanRecordExpressions populates the query plan with appropriate
// expressions for setting up to make a record.
func (p *updateQueryPlanner) setQueryPlanRecordExpressions() error {
	schemaColumns, err := p.catalog.GetColumns(p.stmt.TableName)
	if err != nil {
		return err
	}
	pkColName, err := p.catalog.GetPrimaryKeyColumn(p.stmt.TableName)
	if err != nil {
		return err
	}
	idx := 0
	for _, schemaColumn := range schemaColumns {
		if schemaColumn == pkColName {
			continue
		}
		if setListExpression, ok := p.stmt.SetList[schemaColumn]; ok {
			p.queryPlan.updateExprs = append(
				p.queryPlan.updateExprs,
				setListExpression,
			)
		} else {
			p.queryPlan.updateExprs = append(
				p.queryPlan.updateExprs,
				&compiler.ColumnRef{
					Table:        p.stmt.TableName,
					Column:       schemaColumn,
					IsPrimaryKey: pkColName == schemaColumn,
					ColIdx:       idx,
				},
			)
		}
		if schemaColumn != pkColName {
			idx += 1
		}
	}
	for i := range p.queryPlan.updateExprs {
		cev := &catalogExprVisitor{}
		cev.Init(p.catalog, p.stmt.TableName)
		p.queryPlan.updateExprs[i].BreadthWalk(cev)
	}
	return nil
}

// Execution plan is a byte code routine based off a high level query plan.
func (p *updatePlanner) ExecutionPlan() (*vm.ExecutionPlan, error) {
	if p.queryPlanner.queryPlan == nil {
		_, err := p.QueryPlan()
		if err != nil {
			return nil, err
		}
	}
	p.queryPlanner.queryPlan.plan.compile()
	p.executionPlanner.executionPlan.Commands = p.queryPlanner.queryPlan.plan.commands
	return p.executionPlanner.executionPlan, nil
}
