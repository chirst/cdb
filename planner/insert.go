package planner

import (
	"errors"
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

	if len(s.ColValues)%len(s.ColNames) != 0 {
		return nil, errors.New("values list did not match columns list")
	}
	for valueIdx := range len(s.ColValues) / len(s.ColNames) {
		idIdx := 1
		commands = append(commands, &vm.NewRowIdCmd{P1: rootPageNumber, P2: idIdx})
		gap := 1
		registerIdx := 2
		for _, c := range cols {
			if c == "id" {
				// TODO handle id column
				continue
			}
			vIdx := -1
			for i, scn := range s.ColNames {
				if scn == c {
					vIdx = i + (valueIdx * len(s.ColNames))
				}
			}
			if vIdx == -1 {
				return nil, fmt.Errorf("column name %s not specified", c)
			}
			commands = append(commands, &vm.StringCmd{P1: registerIdx, P4: s.ColValues[vIdx]})
			registerIdx += 1
			gap += 1
		}
		commands = append(commands, &vm.MakeRecordCmd{P1: 2, P2: gap, P3: 1 + gap})
		commands = append(commands, &vm.InsertCmd{P1: rootPageNumber, P2: 1 + gap, P3: idIdx})
	}

	commands = append(commands, &vm.HaltCmd{})
	return &vm.ExecutionPlan{
		Explain:  s.Explain,
		Commands: commands,
	}, nil
}
