// db serves as an interface for the database where raw SQL goes in and
// convenient data structures come out. db is intended to be consumed by things
// like a repl (read eval print loop), a program, or a transport protocol such
// as http.
package main

import (
	"fmt"

	"github.com/chirst/cdb/compiler"
)

type db struct{}

func newDb() *db {
	return &db{}
}

func (db *db) execute(sql string) executeResult {
	tokens := compiler.NewLexer(sql).Lex()
	statement, err := compiler.NewParser(tokens).Parse()
	if err != nil {
		return executeResult{err: err}
	}
	executionPlan, err := db.getExecutionPlanFor(statement)
	if err != nil {
		return executeResult{err: err}
	}
	return *newVm().execute(executionPlan)
}

func (*db) getExecutionPlanFor(statement compiler.Stmt) (*executionPlan, error) {
	logicalPlanner := newLogicalPlanner()
	physicalPlanner := newPhysicalPlanner()
	switch s := statement.(type) {
	case *compiler.SelectStmt:
		lp := logicalPlanner.forSelect(s)
		return physicalPlanner.forSelect(lp, s.Explain), nil
	case *compiler.CreateStmt:
	case *compiler.InsertStmt:
	}
	return nil, fmt.Errorf("statement not supported")
}
