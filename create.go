package main

import (
	"errors"

	"github.com/chirst/cdb/compiler"
)

type createPlanner struct {
	catalog *catalog
}

func newCreatePlanner() *createPlanner {
	return &createPlanner{
		catalog: newCatalog(),
	}
}

func (*createPlanner) getPlan(s *compiler.CreateStmt) (*executionPlan, error) {
	return nil, errors.New("create planner not implemented")
}
