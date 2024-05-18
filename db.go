// db serves as an interface for the database where raw SQL goes in and
// convenient data structures come out. db is intended to be consumed by things
// like a repl (read eval print loop), a program, or a transport protocol such
// as http.
package main

import (
	"errors"

	"github.com/chirst/cdb/compiler"
)

type db struct{}

func newDb() *db {
	return &db{}
}

func (*db) execute(sql string) executeResult {
	tokens := compiler.NewLexer(sql).Lex()
	statement, err := compiler.NewParser(tokens).Parse()
	if err != nil {
		return executeResult{err: err}
	}
	logicalPlanner := newLogicalPlanner()
	physicalPlanner := newPhysicalPlanner()
	var executionPlan *executionPlan
	if ss, ok := statement.(*compiler.SelectStmt); ok {
		lp := logicalPlanner.forSelect(ss)
		executionPlan = physicalPlanner.forSelect(lp, ss.Explain)
	}
	if executionPlan == nil {
		return executeResult{err: errors.New("statement not supported")}
	}
	return *newVm().execute(executionPlan)
}
