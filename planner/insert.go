package planner

import (
	"errors"
	"fmt"
	"slices"
	"strconv"

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
	GetPrimaryKeyColumn(tableName string) (string, error)
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

	if err := checkValuesMatchColumns(s); err != nil {
		return nil, err
	}

	pkColumn, err := p.catalog.GetPrimaryKeyColumn(s.TableName)
	if err != nil {
		return nil, err
	}
	for valueIdx := range len(s.ColValues) / len(s.ColNames) {
		keyRegister := 1
		statementIDIdx := -1
		if pkColumn != "" {
			statementIDIdx = slices.IndexFunc(s.ColNames, func(s string) bool {
				return s == pkColumn
			})
		}
		if statementIDIdx == -1 {
			commands = append(commands, &vm.NewRowIdCmd{P1: rootPageNumber, P2: keyRegister})
		} else {
			rowId, err := strconv.Atoi(s.ColValues[statementIDIdx+valueIdx*len(s.ColNames)])
			if err != nil {
				return nil, err
			}
			integerCmdIdx := len(commands) + 2
			commands = append(commands, &vm.NotExistsCmd{P1: rootPageNumber, P2: integerCmdIdx, P3: rowId})
			commands = append(commands, &vm.HaltCmd{P1: 1, P4: "pk unique constraint violated"})
			commands = append(commands, &vm.IntegerCmd{P1: rowId, P2: keyRegister})
		}
		registerIdx := keyRegister
		for _, catalogColumnName := range catalogColumnNames {
			if catalogColumnName != "" && catalogColumnName == pkColumn {
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

func checkValuesMatchColumns(s *compiler.InsertStmt) error {
	// TODO need to enhance for INSERT INTO foo (name) VALUES ('n1', 'n2')
	vl := len(s.ColValues)
	cl := len(s.ColNames)
	if vl%cl != 0 {
		return errValuesNotMatch
	}
	return nil
}
