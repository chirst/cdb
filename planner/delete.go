package planner

import (
	"github.com/chirst/cdb/catalog"
	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/vm"
)

type deleteCatalog interface {
	GetVersion() string
	GetRootPageNumber(string) (int, error)
	GetColumns(string) ([]string, error)
	GetPrimaryKeyColumn(string) (string, error)
	GetColumnType(tableName string, columnName string) (catalog.CdbType, error)
}

type deletePlanner struct {
	catalog       deleteCatalog
	stmt          *compiler.DeleteStmt
	queryPlan     *deleteNode
	executionPlan *vm.ExecutionPlan
}

func NewDelete(catalog deleteCatalog, stmt *compiler.DeleteStmt) *deletePlanner {
	return &deletePlanner{
		catalog: catalog,
		stmt:    stmt,
		executionPlan: vm.NewExecutionPlan(
			catalog.GetVersion(),
			stmt.Explain,
		),
	}
}

// QueryPlan implements db.statementPlanner.
func (d *deletePlanner) QueryPlan() (*QueryPlan, error) {
	rootPageNumber, err := d.catalog.GetRootPageNumber(d.stmt.TableName)
	if err != nil {
		return nil, errTableNotExist
	}
	deleteNode := &deleteNode{
		rootPageNumber: rootPageNumber,
		cursorId:       1,
	}
	qp := newQueryPlan(deleteNode, d.stmt.ExplainQueryPlan, transactionTypeWrite)
	deleteNode.plan = qp
	d.queryPlan = deleteNode
	sn := &scanNode{
		plan:           qp,
		tableName:      d.stmt.TableName,
		rootPageNumber: rootPageNumber,
		cursorId:       1,
		isWriteCursor:  true,
	}
	if d.stmt.Predicate != nil {
		cev := &catalogExprVisitor{}
		cev.Init(d.catalog, d.stmt.TableName)
		d.stmt.Predicate.BreadthWalk(cev)
		fn := &filterNode{
			plan:      qp,
			predicate: d.stmt.Predicate,
			cursorId:  1,
		}
		deleteNode.child = fn
		fn.parent = deleteNode
		sn.parent = fn
		fn.child = sn
	} else {
		deleteNode.child = sn
		sn.parent = deleteNode
	}
	(&optimizer{}).optimizePlan(qp)
	return qp, nil
}

// ExecutionPlan implements db.statementPlanner.
func (d *deletePlanner) ExecutionPlan() (*vm.ExecutionPlan, error) {
	if d.queryPlan == nil {
		_, err := d.QueryPlan()
		if err != nil {
			return nil, err
		}
	}
	d.queryPlan.plan.compile()
	d.executionPlan.Commands = d.queryPlan.plan.commands
	return d.executionPlan, nil
}
