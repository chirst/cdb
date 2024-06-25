package planner

import (
	"fmt"

	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/vm"
)

// insertCatalog defines the catalog methods needed by the select planner
type insertCatalog interface {
	GetColumns(tableOrIndexName string) ([]string, error)
	GetRootPageNumber(tableOrIndexName string) (int, error)
}

type insertPlanner struct {
	catalog insertCatalog
}

func NewInsert(catalog insertCatalog) *insertPlanner {
	return &insertPlanner{
		catalog: catalog,
	}
}

func (p *insertPlanner) GetPlan(s *compiler.InsertStmt) (*vm.ExecutionPlan, error) {
	rootPageNumber, err := p.catalog.GetRootPageNumber(s.TableName)
	if err != nil {
		return nil, err
	}
	cols, err := p.catalog.GetColumns(s.TableName)
	if err != nil {
		return nil, err
	}
	cursorId := 1
	commands := []vm.Command{}
	commands = append(commands, &vm.InitCmd{P2: 1})
	commands = append(commands, &vm.TransactionCmd{P2: 1})
	commands = append(commands, &vm.OpenWriteCmd{P1: cursorId, P2: rootPageNumber})
	commands = append(commands, &vm.NewRowIdCmd{P1: rootPageNumber, P2: 1})
	gap := 1
	registerIdx := 2
	for _, c := range cols {
		if c == "id" {
			continue
		}
		vIdx := -1
		for i, scn := range s.ColNames {
			if scn == c {
				vIdx = i
			}
		}
		if vIdx == -1 {
			return nil, fmt.Errorf("column name %s not specified", c)
		}
		commands = append(commands, &vm.StringCmd{P1: registerIdx, P4: s.ColValues[vIdx]})
		registerIdx += 1
		gap += 1
	}
	commands = append(commands, &vm.MakeRecordCmd{P1: 2, P2: gap, P3: 2 + gap + 1})
	commands = append(commands, &vm.InsertCmd{P1: rootPageNumber, P2: 2 + gap + 1, P3: 1})
	commands = append(commands, &vm.HaltCmd{})
	return &vm.ExecutionPlan{
		Explain:  s.Explain,
		Commands: commands,
	}, nil
}
