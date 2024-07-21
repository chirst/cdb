package planner

import (
	"errors"
	"slices"

	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/kv"
	"github.com/chirst/cdb/vm"
)

var errInvalidPKColumnType = errors.New("primary key must be INTEGER type")
var errTableExists = errors.New("table exists")
var errMoreThanOnePK = errors.New("more than one primary key specified")

// createCatalog defines the catalog methods needed by the create planner
type createCatalog interface {
	GetColumns(tableOrIndexName string) ([]string, error)
	GetRootPageNumber(tableOrIndexName string) (int, error)
	TableExists(tableName string) bool
	GetVersion() string
}

type createPlanner struct {
	qp *createQueryPlanner
	ep *createExecutionPlanner
}

type createQueryPlanner struct {
	catalog   createCatalog
	stmt      *compiler.CreateStmt
	queryPlan logicalNode
}

type createExecutionPlanner struct {
	queryPlan     logicalNode
	executionPlan *vm.ExecutionPlan
}

func NewCreate(catalog createCatalog, stmt *compiler.CreateStmt) *createPlanner {
	return &createPlanner{
		qp: &createQueryPlanner{
			catalog: catalog,
			stmt:    stmt,
		},
		ep: &createExecutionPlanner{
			executionPlan: vm.NewExecutionPlan(
				catalog.GetVersion(),
				stmt.Explain,
			),
		},
	}
}

func (p *createPlanner) QueryPlan() (*QueryPlan, error) {
	tableName, err := p.qp.ensureTableDoesNotExist()
	if err != nil {
		return nil, err
	}
	jSchema, err := p.qp.getSchemaString()
	if err != nil {
		return nil, err
	}
	createNode := &createNode{
		objectType: "table",
		objectName: tableName,
		tableName:  tableName,
		schema:     jSchema,
	}
	qp := newQueryPlan(createNode, p.qp.stmt.ExplainQueryPlan)
	p.ep.queryPlan = createNode
	return qp, nil
}

func (p *createQueryPlanner) ensureTableDoesNotExist() (string, error) {
	tableName := p.stmt.TableName
	if p.catalog.TableExists(tableName) {
		return "", errTableExists
	}
	return tableName, nil
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

func (p *createQueryPlanner) schemaFrom() *kv.TableSchema {
	schema := kv.TableSchema{
		Columns: []kv.TableColumn{},
	}
	for _, cd := range p.stmt.ColDefs {
		schema.Columns = append(schema.Columns, kv.TableColumn{
			Name:       cd.ColName,
			ColType:    cd.ColType,
			PrimaryKey: cd.PrimaryKey,
		})
	}
	return &schema
}

func (cp *createPlanner) ExecutionPlan() (*vm.ExecutionPlan, error) {
	if cp.qp.queryPlan == nil {
		_, err := cp.QueryPlan()
		if err != nil {
			return nil, err
		}
	}
	p := cp.ep
	createNode, ok := p.queryPlan.(*createNode)
	if !ok {
		panic("expected create node")
	}
	p.executionPlan.Append(&vm.InitCmd{P2: 1})
	p.executionPlan.Append(&vm.TransactionCmd{P2: 1})
	p.executionPlan.Append(&vm.CreateBTreeCmd{P2: 1})
	p.executionPlan.Append(&vm.OpenWriteCmd{P1: 1, P2: 1})
	p.executionPlan.Append(&vm.NewRowIdCmd{P1: 1, P2: 2})
	p.executionPlan.Append(&vm.StringCmd{P1: 3, P4: createNode.objectType})
	p.executionPlan.Append(&vm.StringCmd{P1: 4, P4: createNode.objectName})
	p.executionPlan.Append(&vm.StringCmd{P1: 5, P4: createNode.tableName})
	p.executionPlan.Append(&vm.CopyCmd{P1: 1, P2: 6})
	p.executionPlan.Append(&vm.StringCmd{P1: 7, P4: string(createNode.schema)})
	p.executionPlan.Append(&vm.MakeRecordCmd{P1: 3, P2: 5, P3: 8})
	p.executionPlan.Append(&vm.InsertCmd{P1: 1, P2: 8, P3: 2})
	p.executionPlan.Append(&vm.ParseSchemaCmd{})
	p.executionPlan.Append(&vm.HaltCmd{})
	return p.executionPlan, nil
}
