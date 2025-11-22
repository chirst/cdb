package planner

import (
	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/vm"
)

type updateCatalog interface {
	GetVersion() string
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
	return &QueryPlan{}, nil
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
	rootPage := 2
	haltAddress := 12
	rewindAddress := 3
	p.executionPlan.Append(&vm.InitCmd{P2: 1})
	p.executionPlan.Append(&vm.TransactionCmd{P2: 1})
	p.executionPlan.Append(&vm.OpenWriteCmd{P1: 1, P2: rootPage})
	p.executionPlan.Append(&vm.RewindCmd{P1: 1, P2: haltAddress})
	p.executionPlan.Append(&vm.RowIdCmd{P1: 1, P2: 1})
	p.executionPlan.Append(&vm.ColumnCmd{P1: 1, P2: 0, P3: 2})
	p.executionPlan.Append(&vm.IntegerCmd{P1: 1, P2: 3})
	p.executionPlan.Append(&vm.MakeRecordCmd{P1: 2, P2: 2, P3: 4})
	p.executionPlan.Append(&vm.DeleteCmd{P1: 1})
	p.executionPlan.Append(&vm.InsertCmd{P1: 1, P2: 4, P3: 1})
	p.executionPlan.Append(&vm.NextCmd{P1: 1, P2: rewindAddress + 1})
	p.executionPlan.Append(&vm.HaltCmd{})
	return p.executionPlan, nil
}
