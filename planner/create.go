package planner

import (
	"errors"
	"slices"

	"github.com/chirst/cdb/catalog"
	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/vm"
)

var (
	errInvalidPKColumnType = errors.New("primary key must be INTEGER type")
	errTableExists         = errors.New("table exists")
	errMoreThanOnePK       = errors.New("more than one primary key specified")
)

// createCatalog defines the catalog methods needed by the create planner
type createCatalog interface {
	GetColumns(tableOrIndexName string) ([]string, error)
	GetRootPageNumber(tableOrIndexName string) (int, error)
	TableExists(tableName string) bool
	GetVersion() string
}

// createPlanner is capable of generating a logical query plan and a physical
// executionPlan for a create statement. The planners within are separated by
// their responsibility.
type createPlanner struct {
	// queryPlanner is responsible for transforming the AST to a logical query
	// plan tree. This tree is made up of nodes similar to a relational algebra
	// tree. The query planner also performs binding and validation.
	queryPlanner *createQueryPlanner
	// executionPlanner is responsible for converting the logical query plan
	// tree to a bytecode execution plan capable of being run by the virtual
	// machine.
	executionPlanner *createExecutionPlanner
}

// createQueryPlanner converts the AST to a logical query plan. Along the way it
// validates the statement makes sense with the catalog a process known as
// binding.
type createQueryPlanner struct {
	// catalog contains the schema
	catalog createCatalog
	// stmt contains the AST
	stmt *compiler.CreateStmt
	// queryPlan contains the query plan being constructed. The root node must
	// be createNode.
	queryPlan *createNode
}

// createExecutionPlanner converts logical nodes to a bytecode execution plan
// that can be run by the vm.
type createExecutionPlanner struct {
	// queryPlan contains the logical query plan. The is populated by calling
	// QueryPlan.
	queryPlan *createNode
	// executionPlan contains the bytecode execution plan being constructed.
	// This is populated by calling ExecutionPlan.
	executionPlan *vm.ExecutionPlan
}

// NewCreate creates a planner for the given create statement.
func NewCreate(catalog createCatalog, stmt *compiler.CreateStmt) *createPlanner {
	return &createPlanner{
		queryPlanner: &createQueryPlanner{
			catalog: catalog,
			stmt:    stmt,
		},
		executionPlanner: &createExecutionPlanner{
			executionPlan: vm.NewExecutionPlan(
				catalog.GetVersion(),
				stmt.Explain,
			),
		},
	}
}

// QueryPlan generates the query plan for the planner.
func (p *createPlanner) QueryPlan() (*QueryPlan, error) {
	qp, err := p.queryPlanner.getQueryPlan()
	if err != nil {
		return nil, err
	}
	p.executionPlanner.queryPlan = p.queryPlanner.queryPlan
	return qp, err
}

func (p *createQueryPlanner) getQueryPlan() (*QueryPlan, error) {
	tableExists := p.catalog.TableExists(p.stmt.TableName)
	if p.stmt.IfNotExists && tableExists {
		noopCreateNode := &createNode{
			noop:      true,
			tableName: p.stmt.TableName,
		}
		p.queryPlan = noopCreateNode
		return newQueryPlan(noopCreateNode, p.stmt.ExplainQueryPlan), nil
	}
	if tableExists {
		return nil, errTableExists
	}
	jSchema, err := p.getSchemaString()
	if err != nil {
		return nil, err
	}
	createNode := &createNode{
		objectType: "table",
		objectName: p.stmt.TableName,
		tableName:  p.stmt.TableName,
		schema:     jSchema,
	}
	p.queryPlan = createNode
	return newQueryPlan(createNode, p.stmt.ExplainQueryPlan), nil
}

func (p *createQueryPlanner) getSchemaString() (string, error) {
	if err := p.ensurePrimaryKeyCount(); err != nil {
		return "", err
	}
	if err := p.ensurePrimaryKeyInteger(); err != nil {
		return "", err
	}
	jSchema, err := p.schemaFrom().ToJSON()
	if err != nil {
		return "", err
	}
	return string(jSchema), nil
}

// The id column must be an integer. The index key is capable of being something
// other than an integer, but is not worth implementing at the moment. Integer
// primary keys are superior for auto incrementing and being unique.
func (p *createQueryPlanner) ensurePrimaryKeyInteger() error {
	hasPK := slices.ContainsFunc(p.stmt.ColDefs, func(cd compiler.ColDef) bool {
		return cd.PrimaryKey
	})
	if !hasPK {
		return nil
	}
	hasIntegerPK := slices.ContainsFunc(p.stmt.ColDefs, func(cd compiler.ColDef) bool {
		return cd.PrimaryKey && cd.ColType == "INTEGER"
	})
	if !hasIntegerPK {
		return errInvalidPKColumnType
	}
	return nil
}

// Only one primary key is supported at this time.
func (p *createQueryPlanner) ensurePrimaryKeyCount() error {
	count := 0
	for _, cd := range p.stmt.ColDefs {
		if cd.PrimaryKey {
			count += 1
		}
	}
	if count > 1 {
		return errMoreThanOnePK
	}
	return nil
}

func (p *createQueryPlanner) schemaFrom() *catalog.TableSchema {
	schema := catalog.TableSchema{
		Columns: []catalog.TableColumn{},
	}
	for _, cd := range p.stmt.ColDefs {
		schema.Columns = append(schema.Columns, catalog.TableColumn{
			Name:       cd.ColName,
			ColType:    cd.ColType,
			PrimaryKey: cd.PrimaryKey,
		})
	}
	return &schema
}

// ExecutionPlan returns the bytecode execution plan for the planner. Calling
// QueryPlan is not a prerequisite to this method as it will be called by
// ExecutionPlan if needed.
func (p *createPlanner) ExecutionPlan() (*vm.ExecutionPlan, error) {
	if p.queryPlanner.queryPlan == nil {
		_, err := p.QueryPlan()
		if err != nil {
			return nil, err
		}
	}
	if p.queryPlanner.queryPlan.noop {
		return p.executionPlanner.getNoopExecutionPlan(), nil
	}
	return p.executionPlanner.getExecutionPlan(), nil
}

// getNoopExecutionPlan asserts the query can be ran based on the information
// provided by the catalog. If the catalog were to go out of date this execution
// plan will be recompiled before it is ever ran.
func (p *createExecutionPlanner) getNoopExecutionPlan() *vm.ExecutionPlan {
	p.executionPlan.Append(&vm.InitCmd{P2: 1})
	p.executionPlan.Append(&vm.TransactionCmd{P2: 1})
	p.executionPlan.Append(&vm.HaltCmd{})
	return p.executionPlan
}

func (p *createExecutionPlanner) getExecutionPlan() *vm.ExecutionPlan {
	const cursorId = 1
	p.executionPlan.Append(&vm.InitCmd{P2: 1})
	p.executionPlan.Append(&vm.TransactionCmd{P2: 1})
	p.executionPlan.Append(&vm.CreateBTreeCmd{P2: 1})
	p.executionPlan.Append(&vm.OpenWriteCmd{P1: cursorId, P2: 1})
	p.executionPlan.Append(&vm.NewRowIdCmd{P1: cursorId, P2: 2})
	p.executionPlan.Append(&vm.StringCmd{P1: 3, P4: p.queryPlan.objectType})
	p.executionPlan.Append(&vm.StringCmd{P1: 4, P4: p.queryPlan.objectName})
	p.executionPlan.Append(&vm.StringCmd{P1: 5, P4: p.queryPlan.tableName})
	p.executionPlan.Append(&vm.CopyCmd{P1: 1, P2: 6})
	p.executionPlan.Append(&vm.StringCmd{P1: 7, P4: string(p.queryPlan.schema)})
	p.executionPlan.Append(&vm.MakeRecordCmd{P1: 3, P2: 5, P3: 8})
	p.executionPlan.Append(&vm.InsertCmd{P1: cursorId, P2: 8, P3: 2})
	p.executionPlan.Append(&vm.ParseSchemaCmd{})
	p.executionPlan.Append(&vm.HaltCmd{})
	return p.executionPlan
}
