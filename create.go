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
	commands := map[int]command{}
	commands[1] = &initCmd{p2: 2}
	commands[2] = &transactionCmd{p2: 1}
	commands[3] = &createBTreeCmd{p2: 1}
	commands[4] = &openWriteCmd{p1: 1, p2: 1}
	commands[5] = &newRowIdCmd{p1: 1, p2: 2}
	commands[6] = &stringCmd{p1: 3, p4: oType}
	commands[7] = &stringCmd{p1: 4, p4: tName}
	commands[8] = &stringCmd{p1: 5, p4: tName}
	commands[9] = &copyCmd{p1: 1, p2: 6}
	commands[10] = &stringCmd{p1: 7, p4: string(jSchema)}
	commands[11] = &makeRecordCmd{p1: 3, p2: 4, p3: 8}
	commands[12] = &insertCmd{p1: 1, p2: 8, p3: 2}
	commands[13] = &parseSchemaCmd{}
	commands[14] = &haltCmd{p2: 1}
	return &executionPlan{
		explain:  s.Explain,
		commands: commands,
	}, nil
}
