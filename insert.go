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
	commands = append(commands, &haltCmd{p2: 1})
	return &executionPlan{
		explain:  s.Explain,
		commands: commands,
	}, nil
}

// EXPLAIN INSERT INTO foo (first) VALUES ('wat');
// | addr | opcode      | p1  | p2  | p3  | p4  | p5  | comment |
// | ---- | ----------- | --- | --- | --- | --- | --- | ------- |
// | 0    | Init        | 0   | 7   | 0   |     | 0   |         |
// | 1    | OpenWrite   | 0   | 2   | 0   | 2   | 0   |         |
// | 2    | String8     | 0   | 3   | 0   | wat | 0   |         |
// | 3    | NewRowid    | 0   | 1   | 0   |     | 0   |         |
// | 4    | MakeRecord  | 2   | 2   | 4   | DB  | 0   |         |
// | 5    | Insert      | 0   | 4   | 1   | foo | 57  |         |
// | 6    | Halt        | 0   | 0   | 0   |     | 0   |         |
// | 7    | Transaction | 0   | 1   | 1   | 0   | 1   |         |
// | 8    | Null        | 0   | 2   | 0   |     | 0   |         |
// | 9    | Goto        | 0   | 1   | 0   |     | 0   |         |
