package main

import (
	"errors"

	"github.com/chirst/cdb/compiler"
)

type insertPlanner struct {
	catalog *catalog
}

func newInsertPlanner() *insertPlanner {
	return &insertPlanner{
		catalog: newCatalog(),
	}
}

func (*insertPlanner) getPlan(s *compiler.InsertStmt) (*executionPlan, error) {
	return nil, errors.New("insert planner not implemented")
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
