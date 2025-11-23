package planner

import (
	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/vm"
)

type updateCatalog interface {
	GetVersion() string
	GetRootPageNumber(string) (int, error)
}

type updatePlanner struct {
	queryPlanner     *updateQueryPlanner
	executionPlanner *updateExecutionPlanner
}

type updateQueryPlanner struct {
	catalog   updateCatalog
	stmt      *compiler.UpdateStmt
	queryPlan *updateNode
}

type updateExecutionPlanner struct {
	queryPlan     *updateNode
	executionPlan *vm.ExecutionPlan
}

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

func (p *updatePlanner) QueryPlan() (*QueryPlan, error) {
	qp, err := p.queryPlanner.getQueryPlan()
	if err != nil {
		return nil, err
	}
	p.executionPlanner.queryPlan = p.queryPlanner.queryPlan
	return qp, err
}

func (p *updateQueryPlanner) getQueryPlan() (*QueryPlan, error) {
	rootPage, err := p.catalog.GetRootPageNumber(p.stmt.TableName)
	if err != nil {
		return nil, errTableNotExist
	}
	updateNode := &updateNode{
		rootPage: rootPage,
	}
	// list of expressions for the update
	// if the column is in the set list then use the set list expression
	// - validate the set list left most node is a columnref
	// - validate they are not duplicate
	// - validate they are no more than a binary expr with = and a int or string on right
	// - validate the pk is not updated in a way that can loop
	// else the column is not in the set list then use a columnRef expression
	p.queryPlan = updateNode
	return &QueryPlan{
		ExplainQueryPlan: p.stmt.ExplainQueryPlan,
		root:             updateNode,
	}, nil
}

func (p *updatePlanner) ExecutionPlan() (*vm.ExecutionPlan, error) {
	if p.queryPlanner.queryPlan == nil {
		_, err := p.QueryPlan()
		if err != nil {
			return nil, err
		}
	}
	return p.executionPlanner.getExecutionPlan()
}

func (p *updateExecutionPlanner) getExecutionPlan() (*vm.ExecutionPlan, error) {
	// Init
	p.executionPlan.Append(&vm.InitCmd{P2: 1})
	p.executionPlan.Append(&vm.TransactionCmd{P2: 1})
	p.executionPlan.Append(&vm.OpenWriteCmd{P1: 1, P2: p.queryPlan.rootPage})

	// Go to start of table
	rewindCmd := &vm.RewindCmd{P1: 1} // P2 deferred
	p.executionPlan.Append(rewindCmd)

	// Loop
	loopStartAddress := len(p.executionPlan.Commands)
	// take each item in the set list and build to make a record
	p.executionPlan.Append(&vm.RowIdCmd{P1: 1, P2: 1})
	p.executionPlan.Append(&vm.ColumnCmd{P1: 1, P2: 0, P3: 2})
	p.executionPlan.Append(&vm.IntegerCmd{P1: 1, P2: 3})
	p.executionPlan.Append(&vm.MakeRecordCmd{P1: 2, P2: 2, P3: 4})
	p.executionPlan.Append(&vm.DeleteCmd{P1: 1})
	p.executionPlan.Append(&vm.InsertCmd{P1: 1, P2: 4, P3: 1})
	p.executionPlan.Append(&vm.NextCmd{P1: 1, P2: loopStartAddress})

	// End
	p.executionPlan.Append(&vm.HaltCmd{})
	rewindCmd.P2 = len(p.executionPlan.Commands) - 1
	return p.executionPlan, nil
}
