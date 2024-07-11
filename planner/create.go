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
	catalog createCatalog
}

func NewCreate(catalog createCatalog) *createPlanner {
	return &createPlanner{
		catalog: catalog,
	}
}

func (c *createPlanner) GetPlan(s *compiler.CreateStmt) (*vm.ExecutionPlan, error) {
	executionPlan := vm.NewExecutionPlan(c.catalog.GetVersion())
	executionPlan.Explain = s.Explain
	err := c.ensureTableDoesNotExist(s)
	if err != nil {
		return nil, err
	}
	jSchema, err := getSchemaString(s)
	if err != nil {
		return nil, err
	}
	// objectType could be an index, trigger, or in this case a table.
	objectType := "table"
	// objectName is the name of the index, trigger, or table.
	objectName := s.TableName
	// tableName is name of the table this object is associated with.
	tableName := s.TableName
	commands := []vm.Command{}
	commands = append(commands, &vm.InitCmd{P2: 1})
	commands = append(commands, &vm.TransactionCmd{P2: 1})
	commands = append(commands, &vm.CreateBTreeCmd{P2: 1})
	commands = append(commands, &vm.OpenWriteCmd{P1: 1, P2: 1})
	commands = append(commands, &vm.NewRowIdCmd{P1: 1, P2: 2})
	commands = append(commands, &vm.StringCmd{P1: 3, P4: objectType})
	commands = append(commands, &vm.StringCmd{P1: 4, P4: objectName})
	commands = append(commands, &vm.StringCmd{P1: 5, P4: tableName})
	commands = append(commands, &vm.CopyCmd{P1: 1, P2: 6})
	commands = append(commands, &vm.StringCmd{P1: 7, P4: string(jSchema)})
	commands = append(commands, &vm.MakeRecordCmd{P1: 3, P2: 5, P3: 8})
	commands = append(commands, &vm.InsertCmd{P1: 1, P2: 8, P3: 2})
	commands = append(commands, &vm.ParseSchemaCmd{})
	commands = append(commands, &vm.HaltCmd{})
	executionPlan.Commands = commands
	return executionPlan, nil
}

func (c *createPlanner) ensureTableDoesNotExist(s *compiler.CreateStmt) error {
	if c.catalog.TableExists(s.TableName) {
		return errTableExists
	}
	return nil
}

func getSchemaString(s *compiler.CreateStmt) (string, error) {
	if err := ensurePrimaryKeyCount(s); err != nil {
		return "", err
	}
	if err := ensurePrimaryKeyInteger(s); err != nil {
		return "", err
	}
	jSchema, err := schemaFrom(s).ToJSON()
	if err != nil {
		return "", err
	}
	return string(jSchema), nil
}

// The id column must be an integer. The index key is capable of being something
// other than an integer, but is not worth implementing at the moment. Integer
// primary keys are superior for auto incrementing and being unique.
func ensurePrimaryKeyInteger(s *compiler.CreateStmt) error {
	hasPK := slices.ContainsFunc(s.ColDefs, func(cd compiler.ColDef) bool {
		return cd.PrimaryKey
	})
	if !hasPK {
		return nil
	}
	hasIntegerPK := slices.ContainsFunc(s.ColDefs, func(cd compiler.ColDef) bool {
		return cd.PrimaryKey && cd.ColType == "INTEGER"
	})
	if !hasIntegerPK {
		return errInvalidPKColumnType
	}
	return nil
}

// Only one primary key is supported at this time.
func ensurePrimaryKeyCount(s *compiler.CreateStmt) error {
	count := 0
	for _, cd := range s.ColDefs {
		if cd.PrimaryKey {
			count += 1
		}
	}
	if count > 1 {
		return errMoreThanOnePK
	}
	return nil
}

func schemaFrom(s *compiler.CreateStmt) *kv.TableSchema {
	schema := kv.TableSchema{
		Columns: []kv.TableColumn{},
	}
	for _, cd := range s.ColDefs {
		schema.Columns = append(schema.Columns, kv.TableColumn{
			Name:       cd.ColName,
			ColType:    cd.ColType,
			PrimaryKey: cd.PrimaryKey,
		})
	}
	return &schema
}
