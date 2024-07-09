package planner

// TODO Catalog access at this level could cause a race since a mutex isn't
// acquired until the commands start execution. This is likely an issue for all
// planners, but is caused by CREATE since it is a writer to the catalog. This
// could potentially be solved by extending the reach of the kv mutex into the
// planner.
//
// TODO planner has odd dependencies on kv and vm.

import (
	"errors"
	"slices"
	"strings"

	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/kv"
	"github.com/chirst/cdb/vm"
)

var errInvalidIDColumnType = errors.New("id column must be INTEGER type")
var errTableExists = errors.New("table exists")

// createCatalog defines the catalog methods needed by the create planner
type createCatalog interface {
	GetColumns(tableOrIndexName string) ([]string, error)
	GetRootPageNumber(tableOrIndexName string) (int, error)
	TableExists(tableName string) bool
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
	return &vm.ExecutionPlan{
		Explain:  s.Explain,
		Commands: commands,
	}, nil
}

func (c *createPlanner) ensureTableDoesNotExist(s *compiler.CreateStmt) error {
	if c.catalog.TableExists(s.TableName) {
		return errTableExists
	}
	return nil
}

func getSchemaString(s *compiler.CreateStmt) (string, error) {
	ensureIDColumn(s)
	if err := ensureIntegerID(s); err != nil {
		return "", err
	}
	jSchema, err := schemaFrom(s).ToJSON()
	if err != nil {
		return "", err
	}
	return string(jSchema), nil
}

// Add an id column as the first argument if the statement doesn't contain some
// upper lower case variation of id. This primary key is not optional, but is
// allowed to be specified in any nth column position or with any casing.
func ensureIDColumn(s *compiler.CreateStmt) {
	hasId := slices.ContainsFunc(s.ColDefs, func(cd compiler.ColDef) bool {
		return strings.ToLower(cd.ColName) == "id"
	})
	if hasId {
		return
	}
	s.ColDefs = append(
		[]compiler.ColDef{
			{
				ColName: "id",
				ColType: "INTEGER",
			},
		},
		s.ColDefs...,
	)
}

// The id column must be an integer. The index key is capable of being something
// other than an integer, but is not worth implementing at the moment. Integer
// primary keys are superior for auto incrementing and being unique.
func ensureIntegerID(s *compiler.CreateStmt) error {
	hasIntegerID := slices.ContainsFunc(s.ColDefs, func(cd compiler.ColDef) bool {
		return strings.ToLower(cd.ColName) == "id" && cd.ColType == "INTEGER"
	})
	if hasIntegerID {
		return nil
	}
	return errInvalidIDColumnType
}

func schemaFrom(s *compiler.CreateStmt) *kv.TableSchema {
	schema := kv.TableSchema{
		Columns: []kv.TableColumn{},
	}
	for _, cd := range s.ColDefs {
		schema.Columns = append(schema.Columns, kv.TableColumn{
			Name:    cd.ColName,
			ColType: cd.ColType,
		})
	}
	return &schema
}
