package planner

// TODO
// This planner will eventually consist of smaller parts.
// 1. Something like a binder may be necessary which would validate the values
//    in the statement make sense given the current schema.
// 2. A logical query planner which would transform the ast into a relational
//    algebra like structure. This structure would allow for optimizations like
//    predicate push down.
// 3. Perhaps a physical planner which would maybe take into account statistics
//    and indexes.
// Somewhere in these structures would be the ability to print a query plan that
// is higher level than the bytecode operations. A typical explain tree.

import (
	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/vm"
)

// selectCatalog defines the catalog methods needed by the select planner
type selectCatalog interface {
	GetColumns(tableOrIndexName string) ([]string, error)
	GetRootPageNumber(tableOrIndexName string) (int, error)
	GetVersion() string
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
		commands = append(commands, &vm.RowIdCmd{P1: cursorId, P2: 1})
		colIdx := 0
		registerIdx := 2
		gap := 1
		for _, c := range cols {
			if c == "id" {
				continue
			}
			commands = append(commands, &vm.ColumnCmd{P1: cursorId, P2: colIdx, P3: registerIdx})
			colIdx += 1
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
