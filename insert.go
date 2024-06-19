package main

import (
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

func (*insertPlanner) getPlan(s *compiler.InsertStmt) (*executionPlan, error) {
	commands := map[int]command{}
	commands[1] = &initCmd{p2: 2}
	commands[2] = &transactionCmd{p2: 1}
	commands[3] = &openWriteCmd{p1: 1, p2: 2}
	commands[4] = &newRowIdCmd{p1: 2, p2: 1}
	commands[5] = &stringCmd{p1: 2, p4: "gud"}
	commands[6] = &stringCmd{p1: 3, p4: "dude"}
	commands[7] = &makeRecordCmd{p1: 2, p2: 1, p3: 4}
	commands[8] = &insertCmd{p1: 1, p2: 4, p3: 1}
	commands[9] = &haltCmd{p2: 1}
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
