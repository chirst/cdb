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
