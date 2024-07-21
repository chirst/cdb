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
	qp *insertQueryPlanner
	ep *insertExecutionPlanner
}

type insertQueryPlanner struct {
	catalog   insertCatalog
	stmt      *compiler.InsertStmt
	queryPlan *insertNode
}

type insertExecutionPlanner struct {
	queryPlan     *insertNode
	executionPlan *vm.ExecutionPlan
}

func NewInsert(catalog insertCatalog, stmt *compiler.InsertStmt) *insertPlanner {
	return &insertPlanner{
		qp: &insertQueryPlanner{
			catalog: catalog,
			stmt:    stmt,
		},
		ep: &insertExecutionPlanner{
			executionPlan: vm.NewExecutionPlan(
				catalog.GetVersion(),
				stmt.Explain,
			),
		},
	}
}

func (ip *insertPlanner) QueryPlan() (*QueryPlan, error) {
	p := ip.qp
	rootPage, err := p.catalog.GetRootPageNumber(p.stmt.TableName)
	if err != nil {
		return nil, errTableNotExist
	}
	catalogColumnNames, err := p.catalog.GetColumns(p.stmt.TableName)
	if err != nil {
		return nil, err
	}
	if err := checkValuesMatchColumns(p.stmt); err != nil {
		return nil, err
	}
	pkColumn, err := p.catalog.GetPrimaryKeyColumn(p.stmt.TableName)
	if err != nil {
		return nil, err
	}
	insertNode := &insertNode{
		rootPage:           rootPage,
		catalogColumnNames: catalogColumnNames,
		pkColumn:           pkColumn,
		colNames:           p.stmt.ColNames,
		colValues:          p.stmt.ColValues,
	}
	p.queryPlan = insertNode
	ip.ep.queryPlan = insertNode
	return newQueryPlan(insertNode, p.stmt.ExplainQueryPlan), nil
}

func (ip *insertPlanner) ExecutionPlan() (*vm.ExecutionPlan, error) {
	if ip.qp.queryPlan == nil {
		_, err := ip.QueryPlan()
		if err != nil {
			return nil, err
		}
	}
	ep := ip.ep
	cursorId := 1
	ep.executionPlan.Append(&vm.InitCmd{P2: 1})
	ep.executionPlan.Append(&vm.TransactionCmd{P2: 1})
	ep.executionPlan.Append(&vm.OpenWriteCmd{P1: cursorId, P2: ep.queryPlan.rootPage})

	for valueIdx := range len(ep.queryPlan.colValues) / len(ep.queryPlan.colNames) {
		keyRegister := 1
		statementIDIdx := -1
		if ep.queryPlan.pkColumn != "" {
			statementIDIdx = slices.IndexFunc(ep.queryPlan.colNames, func(s string) bool {
				return s == ep.queryPlan.pkColumn
			})
		}
		if statementIDIdx == -1 {
			ep.executionPlan.Append(&vm.NewRowIdCmd{P1: ep.queryPlan.rootPage, P2: keyRegister})
		} else {
			rowId, err := strconv.Atoi(ep.queryPlan.colValues[statementIDIdx+valueIdx*len(ep.queryPlan.colNames)])
			if err != nil {
				return nil, err
			}
			integerCmdIdx := len(ep.executionPlan.Commands) + 2
			ep.executionPlan.Append(&vm.NotExistsCmd{P1: ep.queryPlan.rootPage, P2: integerCmdIdx, P3: rowId})
			ep.executionPlan.Append(&vm.HaltCmd{P1: 1, P4: "pk unique constraint violated"})
			ep.executionPlan.Append(&vm.IntegerCmd{P1: rowId, P2: keyRegister})
		}
		registerIdx := keyRegister
		for _, catalogColumnName := range ep.queryPlan.catalogColumnNames {
			if catalogColumnName != "" && catalogColumnName == ep.queryPlan.pkColumn {
				continue
			}
			registerIdx += 1
			vIdx := -1
			for i, statementColumnName := range ep.queryPlan.colNames {
				if statementColumnName == catalogColumnName {
					vIdx = i + (valueIdx * len(ep.queryPlan.colNames))
				}
			}
			if vIdx == -1 {
				return nil, fmt.Errorf("%w %s", errMissingColumnName, catalogColumnName)
			}
			ep.executionPlan.Append(&vm.StringCmd{P1: registerIdx, P4: ep.queryPlan.colValues[vIdx]})
		}
		ep.executionPlan.Append(&vm.MakeRecordCmd{P1: 2, P2: registerIdx - 1, P3: registerIdx + 1})
		ep.executionPlan.Append(&vm.InsertCmd{P1: ep.queryPlan.rootPage, P2: registerIdx + 1, P3: keyRegister})
	}

	ep.executionPlan.Append(&vm.HaltCmd{})
	return ep.executionPlan, nil
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
