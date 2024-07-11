// db serves as an interface for the database where raw SQL goes in and
// convenient data structures come out. db is intended to be consumed by things
// like a repl (read eval print loop), a program, or a transport protocol
package db

import (
	"errors"
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
	TableExists(tableName string) bool
	GetVersion() string
	GetPrimaryKeyColumn(tableName string) (string, error)
}

type DB struct {
	vm        executor
	catalog   dbCatalog
	UseMemory bool
}

func New(useMemory bool, filename string) (*DB, error) {
	kv, err := kv.New(useMemory, filename)
	if err != nil {
		return nil, err
	}
	return &DB{
		vm:        vm.New(kv),
		catalog:   kv.GetCatalog(),
		UseMemory: useMemory,
	}, nil
}

func (db *DB) Execute(sql string) vm.ExecuteResult {
	tokens := compiler.NewLexer(sql).Lex()
	statement, err := compiler.NewParser(tokens).Parse()
	if err != nil {
		return vm.ExecuteResult{Err: err}
	}
	var executeResult vm.ExecuteResult
	for {
		executionPlan, err := db.getExecutionPlanFor(statement)
		if err != nil {
			return vm.ExecuteResult{Err: err}
		}
		executeResult = *db.vm.Execute(executionPlan)
		if !errors.Is(executeResult.Err, vm.ErrVersionChanged) {
			break
		}
	}
	return executeResult
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
