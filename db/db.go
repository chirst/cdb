// db serves as an interface for the database where raw SQL goes in and
// convenient data structures come out. db is intended to be consumed by things
// like a repl (read eval print loop), a program, or a transport protocol
package db

import (
	"fmt"

	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/kv"
	"github.com/chirst/cdb/planner"
	"github.com/chirst/cdb/vm"
)

type executor interface {
	Execute(*vm.ExecutionPlan) *vm.ExecuteResult
}

type dbCatalog interface {
	GetColumns(tableOrIndexName string) ([]string, error)
	GetRootPageNumber(tableOrIndexName string) (int, error)
}

type DB struct {
	vm      executor
	catalog dbCatalog
}

func New(useMemory bool) (*DB, error) {
	kv, err := kv.New(useMemory)
	if err != nil {
		return nil, err
	}
	return &DB{
		vm:      vm.New(kv),
		catalog: kv.GetCatalog(),
	}, nil
}

func (db *DB) Execute(sql string) vm.ExecuteResult {
	tokens := compiler.NewLexer(sql).Lex()
	statement, err := compiler.NewParser(tokens).Parse()
	if err != nil {
		return vm.ExecuteResult{Err: err}
	}
	executionPlan, err := db.getExecutionPlanFor(statement)
	if err != nil {
		return vm.ExecuteResult{Err: err}
	}
	return *db.vm.Execute(executionPlan)
}

func (db *DB) getExecutionPlanFor(statement compiler.Stmt) (*vm.ExecutionPlan, error) {
	switch s := statement.(type) {
	case *compiler.SelectStmt:
		return planner.NewSelect(db.catalog).GetPlan(s)
	case *compiler.CreateStmt:
		return planner.NewCreate(db.catalog).GetPlan(s)
	case *compiler.InsertStmt:
		return planner.NewInsert(db.catalog).GetPlan(s)
	}
	return nil, fmt.Errorf("statement not supported")
}
