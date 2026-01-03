package planner

import (
	"slices"

	"github.com/chirst/cdb/catalog"
	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/vm"
)

// createCatalog defines the catalog methods needed by the create planner
type createCatalog interface {
	GetColumns(tableOrIndexName string) ([]string, error)
	GetRootPageNumber(tableOrIndexName string) (int, error)
	TableExists(tableName string) bool
	GetVersion() string
}

// createPlanner is capable of generating a logical query plan and a physical
// executionPlan for a create statement.
type createPlanner struct {
	// catalog contains the schema
	catalog createCatalog
	// stmt contains the AST
	stmt *compiler.CreateStmt
	// queryPlan contains the query plan being constructed. The root node must
	// be createNode.
	queryPlan *createNode
	// executionPlan contains the bytecode execution plan being constructed.
	// This is populated by calling ExecutionPlan.
	executionPlan *vm.ExecutionPlan
}

// NewCreate creates a planner for the given create statement.
func NewCreate(catalog createCatalog, stmt *compiler.CreateStmt) *createPlanner {
	return &createPlanner{
		catalog: catalog,
		stmt:    stmt,
		executionPlan: vm.NewExecutionPlan(
			catalog.GetVersion(),
			stmt.Explain,
		),
	}
}

// QueryPlan generates the query plan for the planner.
func (p *createPlanner) QueryPlan() (*QueryPlan, error) {
	schemaTableRoot := 1
	tableExists := p.catalog.TableExists(p.stmt.TableName)
	if p.stmt.IfNotExists && tableExists {
		noopCreateNode := &createNode{
			noop:                  true,
			tableName:             p.stmt.TableName,
			catalogRootPageNumber: schemaTableRoot,
			catalogCursorId:       1,
		}
		p.queryPlan = noopCreateNode
		qp := newQueryPlan(
			noopCreateNode,
			p.stmt.ExplainQueryPlan,
			transactionTypeWrite,
		)
		noopCreateNode.plan = qp
		return qp, nil
	}
	if tableExists {
		return nil, errTableExists
	}
	jSchema, err := p.getSchemaString()
	if err != nil {
		return nil, err
	}
	createNode := &createNode{
		objectType:            "table",
		objectName:            p.stmt.TableName,
		tableName:             p.stmt.TableName,
		schema:                jSchema,
		catalogRootPageNumber: schemaTableRoot,
		catalogCursorId:       1,
	}
	p.queryPlan = createNode
	qp := newQueryPlan(
		createNode,
		p.stmt.ExplainQueryPlan,
		transactionTypeWrite,
	)
	createNode.plan = qp
	return qp, nil
}

func (p *createPlanner) getSchemaString() (string, error) {
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
func (p *createPlanner) ensurePrimaryKeyInteger() error {
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
func (p *createPlanner) ensurePrimaryKeyCount() error {
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

func (p *createPlanner) schemaFrom() *catalog.TableSchema {
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
	if p.queryPlan == nil {
		_, err := p.QueryPlan()
		if err != nil {
			return nil, err
		}
	}
	p.queryPlan.plan.compile()
	p.executionPlan.Commands = p.queryPlan.plan.commands
	return p.executionPlan, nil
}
