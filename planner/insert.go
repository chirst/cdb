package planner

import (
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/vm"
)

var (
	errTableNotExist     = errors.New("table does not exist")
	errValuesNotMatch    = errors.New("values list did not match columns list")
	errMissingColumnName = errors.New("missing column")
)

// pkConstraint is the error message displayed when a primary key constraint is
// violated.
const pkConstraint = "pk unique constraint violated"

// insertCatalog defines the catalog methods needed by the insert planner
type insertCatalog interface {
	GetColumns(tableOrIndexName string) ([]string, error)
	GetRootPageNumber(tableOrIndexName string) (int, error)
	GetVersion() string
	GetPrimaryKeyColumn(tableName string) (string, error)
}

// insertPlanner consists of planners capable of generating a logical query plan
// tree and bytecode execution plan for a insert statement.
type insertPlanner struct {
	// The query planner generates a logical query plan tree made up of nodes
	// similar to relational algebra operators. The query planner performs
	// validation while building the tree. Otherwise known as binding.
	queryPlanner *insertQueryPlanner
	// The executionPlanner transforms the logical query plan tree to a bytecode
	// execution plan that can be ran by the virtual machine.
	executionPlanner *insertExecutionPlanner
}

// insertQueryPlanner converts the AST generated by the compiler to a logical
// query plan tree. It is also responsible for validating the AST against the
// system catalog.
type insertQueryPlanner struct {
	// catalog contains the schema.
	catalog insertCatalog
	// stmt contains the AST.
	stmt *compiler.InsertStmt
	// queryPlan contains the query plan being constructed. For an insert, the
	// root node must be an insertNode.
	queryPlan *insertNode
}

// insertExecutionPlanner converts the logical query plan to a bytecode routine
// to be ran by the vm.
type insertExecutionPlanner struct {
	// queryPlan contains the query plan generated by the query planner's
	// QueryPlan method.
	queryPlan *insertNode
	// executionPlan contains the execution plan generated by calling
	// ExecutionPlan.
	executionPlan *vm.ExecutionPlan
}

// NewInsert returns an instance of an insert planner for the given AST.
func NewInsert(catalog insertCatalog, stmt *compiler.InsertStmt) *insertPlanner {
	return &insertPlanner{
		queryPlanner: &insertQueryPlanner{
			catalog: catalog,
			stmt:    stmt,
		},
		executionPlanner: &insertExecutionPlanner{
			executionPlan: vm.NewExecutionPlan(
				catalog.GetVersion(),
				stmt.Explain,
			),
		},
	}
}

// QueryPlan generates the query plan tree for the planner.
func (p *insertPlanner) QueryPlan() (*QueryPlan, error) {
	qp, err := p.queryPlanner.getQueryPlan()
	if err != nil {
		return nil, err
	}
	p.executionPlanner.queryPlan = p.queryPlanner.queryPlan
	return qp, err
}

func (p *insertQueryPlanner) getQueryPlan() (*QueryPlan, error) {
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
	return newQueryPlan(insertNode, p.stmt.ExplainQueryPlan), nil
}

// ExecutionPlan returns the bytecode routine for the planner. Calling QueryPlan
// is not prerequisite to calling ExecutionPlan as ExecutionPlan will be called
// as needed.
func (p *insertPlanner) ExecutionPlan() (*vm.ExecutionPlan, error) {
	if p.queryPlanner.queryPlan == nil {
		_, err := p.QueryPlan()
		if err != nil {
			return nil, err
		}
	}
	return p.executionPlanner.getExecutionPlan()
}

func (p *insertExecutionPlanner) getExecutionPlan() (*vm.ExecutionPlan, error) {
	p.buildInit()
	cursorId := p.openWrite()
	for valueIdx := range len(p.queryPlan.colValues) {
		// For simplicity, the primary key is in the first register.
		const keyRegister = 1
		if err := p.buildPrimaryKey(cursorId, keyRegister, valueIdx); err != nil {
			return nil, err
		}
		registerIdx := keyRegister
		for _, catalogColumnName := range p.queryPlan.catalogColumnNames {
			if catalogColumnName != "" && catalogColumnName == p.queryPlan.pkColumn {
				// Skip the primary key column since it is handled before.
				continue
			}
			registerIdx += 1
			if err := p.buildNonPkValue(valueIdx, registerIdx, catalogColumnName); err != nil {
				return nil, err
			}
		}
		p.executionPlan.Append(&vm.MakeRecordCmd{P1: 2, P2: registerIdx - 1, P3: registerIdx + 1})
		p.executionPlan.Append(&vm.InsertCmd{P1: cursorId, P2: registerIdx + 1, P3: keyRegister})
	}
	p.executionPlan.Append(&vm.HaltCmd{})
	return p.executionPlan, nil
}

func (p *insertExecutionPlanner) buildInit() {
	p.executionPlan.Append(&vm.InitCmd{P2: 1})
	p.executionPlan.Append(&vm.TransactionCmd{P2: 1})
}

func (p *insertExecutionPlanner) openWrite() int {
	const cursorId = 1
	p.executionPlan.Append(&vm.OpenWriteCmd{P1: cursorId, P2: p.queryPlan.rootPage})
	return cursorId
}

func (p *insertExecutionPlanner) buildPrimaryKey(writeCursorId, keyRegister, valueIdx int) error {
	// If the table has a user defined pk column it needs to be looked up in the
	// user defined column list. If the user has defined the pk column the
	// execution plan will involve checking the uniqueness of the pk during
	// execution. Otherwise the system guarantees a unique key.
	statementPkIdx := -1
	if p.queryPlan.pkColumn != "" {
		statementPkIdx = slices.IndexFunc(p.queryPlan.colNames, func(s string) bool {
			return s == p.queryPlan.pkColumn
		})
	}
	if statementPkIdx == -1 {
		p.executionPlan.Append(&vm.NewRowIdCmd{P1: writeCursorId, P2: keyRegister})
		return nil
	}
	rowId, err := strconv.Atoi(p.queryPlan.colValues[valueIdx][statementPkIdx])
	if err != nil {
		return err
	}
	integerCmdIdx := len(p.executionPlan.Commands) + 2
	p.executionPlan.Append(&vm.NotExistsCmd{P1: writeCursorId, P2: integerCmdIdx, P3: rowId})
	p.executionPlan.Append(&vm.HaltCmd{P1: 1, P4: pkConstraint})
	p.executionPlan.Append(&vm.IntegerCmd{P1: rowId, P2: keyRegister})
	return nil
}

func (p *insertExecutionPlanner) buildNonPkValue(valueIdx, registerIdx int, catalogColumnName string) error {
	// Get the statement index of the column name. Because the name positions
	// can mismatch the table column positions.
	stmtColIdx := slices.IndexFunc(p.queryPlan.colNames, func(stmtColName string) bool {
		return stmtColName == catalogColumnName
	})
	// Requires the statement to define a value for each column in the table.
	if stmtColIdx == -1 {
		return fmt.Errorf("%w %s", errMissingColumnName, catalogColumnName)
	}
	p.executionPlan.Append(&vm.StringCmd{P1: registerIdx, P4: p.queryPlan.colValues[valueIdx][stmtColIdx]})
	return nil
}

func checkValuesMatchColumns(s *compiler.InsertStmt) error {
	cl := len(s.ColNames)
	for _, cv := range s.ColValues {
		if cl != len(cv) {
			return errValuesNotMatch
		}
	}
	return nil
}
