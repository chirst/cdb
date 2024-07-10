package planner

// TODO insert should have a unique constraints on a specified primary key
//
// With a primary key specified and specifying value for primary key. Will check
// that key is unique and throw a unique constraint error if it is not.
// INSERT INTO foo (id, bar) VALUES (1, 'asdf')
//
// Will use the NotExists instruction with a halt immediately after. The
// NotExists will seek that the table doesn't have a row with the given pk
// Halt will roll the transaction back if it is called in error.

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/vm"
)

var errTableNotExist = errors.New("table does not exist")
var errValuesNotMatch = errors.New("values list did not match columns list")
var errMissingColumnName = errors.New("missing column")

// insertCatalog defines the catalog methods needed by the insert planner
type insertCatalog interface {
	GetColumns(tableOrIndexName string) ([]string, error)
	GetRootPageNumber(tableOrIndexName string) (int, error)
	GetVersion() string
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
	executionPlan := vm.NewExecutionPlan(p.catalog.GetVersion())
	executionPlan.Explain = s.Explain
	rootPageNumber, err := p.catalog.GetRootPageNumber(s.TableName)
	if err != nil {
		return nil, errTableNotExist
	}
	catalogColumnNames, err := p.catalog.GetColumns(s.TableName)
	if err != nil {
		return nil, err
	}
	cursorId := 1
	commands := []vm.Command{}
	commands = append(commands, &vm.InitCmd{P2: 1})
	commands = append(commands, &vm.TransactionCmd{P2: 1})
	commands = append(commands, &vm.OpenWriteCmd{P1: cursorId, P2: rootPageNumber})

	if len(s.ColValues)%len(s.ColNames) != 0 {
		return nil, errValuesNotMatch
	}

	for valueIdx := range len(s.ColValues) / len(s.ColNames) {
		keyRegister := 1
		statementIDIdx := slices.IndexFunc(s.ColNames, func(s string) bool {
			return strings.ToLower(s) == "id"
		})
		if statementIDIdx == -1 {
			commands = append(commands, &vm.NewRowIdCmd{P1: rootPageNumber, P2: keyRegister})
		} else {
			rowId, err := strconv.Atoi(s.ColValues[statementIDIdx*len(s.ColNames)])
			if err != nil {
				return nil, err
			}
			commands = append(commands, &vm.IntegerCmd{P1: rowId, P2: keyRegister})
		}
		registerIdx := keyRegister
		for _, catalogColumnName := range catalogColumnNames {
			if strings.ToLower(catalogColumnName) == "id" {
				continue
			}
			registerIdx += 1
			vIdx := -1
			for i, statementColumnName := range s.ColNames {
				if statementColumnName == catalogColumnName {
					vIdx = i + (valueIdx * len(s.ColNames))
				}
			}
			if vIdx == -1 {
				return nil, fmt.Errorf("%w %s", errMissingColumnName, catalogColumnName)
			}
			commands = append(commands, &vm.StringCmd{P1: registerIdx, P4: s.ColValues[vIdx]})
		}
		commands = append(commands, &vm.MakeRecordCmd{P1: 2, P2: registerIdx - 1, P3: registerIdx + 1})
		commands = append(commands, &vm.InsertCmd{P1: rootPageNumber, P2: registerIdx + 1, P3: keyRegister})
	}

	commands = append(commands, &vm.HaltCmd{})
	executionPlan.Commands = commands
	return executionPlan, nil
}
