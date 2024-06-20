// db serves as an interface for the database where raw SQL goes in and
// convenient data structures come out. db is intended to be consumed by things
// like a repl (read eval print loop), a program, or a transport protocol such
// as http.
package main

import (
	"fmt"

	"github.com/chirst/cdb/compiler"
)

type db struct {
	vm *vm
}

func newDb() (*db, error) {
	kv, err := NewKv(false)
	if err != nil {
		return nil, err
	}
	return &db{
		vm: newVm(kv),
	}, nil
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
	return *db.vm.execute(executionPlan)
}

func (db *db) getExecutionPlanFor(statement compiler.Stmt) (*executionPlan, error) {
	switch s := statement.(type) {
	case *compiler.SelectStmt:
		return newSelectPlanner(db.vm.kv.catalog).getPlan(s)
	case *compiler.CreateStmt:
		return newCreatePlanner(db.vm.kv.catalog).getPlan(s)
	case *compiler.InsertStmt:
		return newInsertPlanner(db.vm.kv.catalog).getPlan(s)
	}
	return nil, fmt.Errorf("statement not supported")
}
