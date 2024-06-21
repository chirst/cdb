package main

import (
	"fmt"

	"github.com/chirst/cdb/compiler"
)

type insertPlanner struct {
	catalog *catalog
}

func newInsertPlanner(catalog *catalog) *insertPlanner {
	return &insertPlanner{
		catalog: catalog,
	}
}

func (p *insertPlanner) getPlan(s *compiler.InsertStmt) (*executionPlan, error) {
	rootPageNumber, err := p.catalog.getRootPageNumber(s.TableName)
	if err != nil {
		return nil, err
	}
	cols, err := p.catalog.getColumns(s.TableName)
	if err != nil {
		return nil, err
	}
	cursorId := 1
	commands := []command{}
	commands = append(commands, &initCmd{p2: 1})
	commands = append(commands, &transactionCmd{p2: 1})
	commands = append(commands, &openWriteCmd{p1: cursorId, p2: rootPageNumber})
	commands = append(commands, &newRowIdCmd{p1: rootPageNumber, p2: 1})
	gap := -1
	registerIdx := 2
	for _, c := range cols {
		if c == "id" {
			continue
		}
		vIdx := -1
		for i, scn := range s.ColNames {
			if scn == c {
				vIdx = i
			}
		}
		if vIdx == -1 {
			return nil, fmt.Errorf("column name %s not specified", c)
		}
		commands = append(commands, &stringCmd{p1: registerIdx, p4: s.ColValues[vIdx]})
		registerIdx += 1
		gap += 1
	}
	commands = append(commands, &makeRecordCmd{p1: 2, p2: gap, p3: 2 + gap + 1})
	commands = append(commands, &insertCmd{p1: rootPageNumber, p2: 2 + gap + 1, p3: 1})
	commands = append(commands, &haltCmd{})
	return &executionPlan{
		explain:  s.Explain,
		commands: commands,
	}, nil
}
