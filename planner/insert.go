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
	stmt    *compiler.InsertStmt
}

func NewInsert(catalog insertCatalog, stmt *compiler.InsertStmt) *insertPlanner {
	return &insertPlanner{
		catalog: catalog,
		stmt:    stmt,
	}
}

func (p *insertPlanner) QueryPlan() (*QueryPlan, error) {
	return &QueryPlan{}, nil
}

func (p *insertPlanner) ExecutionPlan() (*vm.ExecutionPlan, error) {
	executionPlan := vm.NewExecutionPlan(p.catalog.GetVersion(), p.stmt.Explain)
	rootPageNumber, err := p.catalog.GetRootPageNumber(p.stmt.TableName)
	if err != nil {
		return nil, errTableNotExist
	}
	catalogColumnNames, err := p.catalog.GetColumns(p.stmt.TableName)
	if err != nil {
		return nil, err
	}
	cursorId := 1
	commands := []vm.Command{}
	commands = append(commands, &vm.InitCmd{P2: 1})
	commands = append(commands, &vm.TransactionCmd{P2: 1})
	commands = append(commands, &vm.OpenWriteCmd{P1: cursorId, P2: rootPageNumber})

	if err := checkValuesMatchColumns(p.stmt); err != nil {
		return nil, err
	}

	pkColumn, err := p.catalog.GetPrimaryKeyColumn(p.stmt.TableName)
	if err != nil {
		return nil, err
	}
	for valueIdx := range len(p.stmt.ColValues) / len(p.stmt.ColNames) {
		keyRegister := 1
		statementIDIdx := -1
		if pkColumn != "" {
			statementIDIdx = slices.IndexFunc(p.stmt.ColNames, func(s string) bool {
				return s == pkColumn
			})
		}
		if statementIDIdx == -1 {
			commands = append(commands, &vm.NewRowIdCmd{P1: rootPageNumber, P2: keyRegister})
		} else {
			rowId, err := strconv.Atoi(p.stmt.ColValues[statementIDIdx+valueIdx*len(p.stmt.ColNames)])
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
			for i, statementColumnName := range p.stmt.ColNames {
				if statementColumnName == catalogColumnName {
					vIdx = i + (valueIdx * len(p.stmt.ColNames))
				}
			}
			if vIdx == -1 {
				return nil, fmt.Errorf("%w %s", errMissingColumnName, catalogColumnName)
			}
			commands = append(commands, &vm.StringCmd{P1: registerIdx, P4: p.stmt.ColValues[vIdx]})
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
