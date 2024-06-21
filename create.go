package main

import (
	"github.com/chirst/cdb/compiler"
)

type createPlanner struct {
	catalog *catalog
}

func newCreatePlanner(catalog *catalog) *createPlanner {
	return &createPlanner{
		catalog: catalog,
	}
}

func (*createPlanner) getPlan(s *compiler.CreateStmt) (*executionPlan, error) {
	schema := tableSchema{
		Columns: []tableColumn{},
	}
	for _, cd := range s.ColDefs {
		schema.Columns = append(schema.Columns, tableColumn{
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
	commands := []command{}
	commands = append(commands, &initCmd{p2: 1})
	commands = append(commands, &transactionCmd{p2: 1})
	commands = append(commands, &createBTreeCmd{p2: 1})
	commands = append(commands, &openWriteCmd{p1: 1, p2: 1})
	commands = append(commands, &newRowIdCmd{p1: 1, p2: 2})
	commands = append(commands, &stringCmd{p1: 3, p4: oType})
	commands = append(commands, &stringCmd{p1: 4, p4: tName})
	commands = append(commands, &stringCmd{p1: 5, p4: tName})
	commands = append(commands, &copyCmd{p1: 1, p2: 6})
	commands = append(commands, &stringCmd{p1: 7, p4: string(jSchema)})
	commands = append(commands, &makeRecordCmd{p1: 3, p2: 5, p3: 8})
	commands = append(commands, &insertCmd{p1: 1, p2: 8, p3: 2})
	commands = append(commands, &parseSchemaCmd{})
	commands = append(commands, &haltCmd{})
	return &executionPlan{
		explain:  s.Explain,
		commands: commands,
	}, nil
}
