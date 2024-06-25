package planner

import (
	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/kv"
	"github.com/chirst/cdb/vm"
)

// createCatalog defines the catalog methods needed by the select planner
type createCatalog interface {
	GetColumns(tableOrIndexName string) ([]string, error)
	GetRootPageNumber(tableOrIndexName string) (int, error)
}

type createPlanner struct {
	catalog createCatalog
}

func NewCreate(catalog createCatalog) *createPlanner {
	return &createPlanner{
		catalog: catalog,
	}
}

func (*createPlanner) GetPlan(s *compiler.CreateStmt) (*vm.ExecutionPlan, error) {
	schema := kv.TableSchema{
		Columns: []kv.TableColumn{},
	}
	for _, cd := range s.ColDefs {
		schema.Columns = append(schema.Columns, kv.TableColumn{
			Name:    cd.ColName,
			ColType: cd.ColType,
		})
	}
	jSchema, err := schema.ToJSON()
	if err != nil {
		return nil, err
	}
	oType := "table"
	tName := s.TableName
	commands := []vm.Command{}
	commands = append(commands, &vm.InitCmd{P2: 1})
	commands = append(commands, &vm.TransactionCmd{P2: 1})
	commands = append(commands, &vm.CreateBTreeCmd{P2: 1})
	commands = append(commands, &vm.OpenWriteCmd{P1: 1, P2: 1})
	commands = append(commands, &vm.NewRowIdCmd{P1: 1, P2: 2})
	commands = append(commands, &vm.StringCmd{P1: 3, P4: oType})
	commands = append(commands, &vm.StringCmd{P1: 4, P4: tName})
	commands = append(commands, &vm.StringCmd{P1: 5, P4: tName})
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
