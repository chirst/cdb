// db serves as an interface for the database where raw SQL goes in and
// convenient data structures come out. db is intended to be consumed by things
// like a repl (read eval print loop), a program, or a transport protocol
package db

import (
	"errors"
	"time"

	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/kv"
	"github.com/chirst/cdb/planner"
	"github.com/chirst/cdb/vm"
)

type executor interface {
	Execute(*vm.ExecutionPlan) *vm.ExecuteResult
}

type statementPlanner interface {
	ExecutionPlan() (*vm.ExecutionPlan, error)
	QueryPlan() (*planner.QueryPlan, error)
}

type dbCatalog interface {
	GetColumns(string) ([]string, error)
	GetRootPageNumber(string) (int, error)
	TableExists(string) bool
	GetVersion() string
	GetPrimaryKeyColumn(string) (string, error)
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

func (db *DB) Tokenize(sql string) compiler.Statements {
	return compiler.NewLexer(sql).ToStatements()
}

func (db *DB) IsTerminated(statements compiler.Statements) bool {
	return compiler.IsTerminated(statements)
}

func (db *DB) ExecuteRaw(sql string) vm.ExecuteResult {
	statements := compiler.NewLexer(sql).ToStatements()
	if len(statements) != 1 {
		return vm.ExecuteResult{Err: errors.New("must be single statement")}
	}
	return db.Execute(statements[0])
}

func (db *DB) Execute(statements compiler.Statement) vm.ExecuteResult {
	start := time.Now()
	statement, err := compiler.NewParser(statements).Parse()
	if err != nil {
		return vm.ExecuteResult{Err: err}
	}

	planner := db.getPlannerFor(statement)
	qp, err := planner.QueryPlan()
	if err != nil {
		return vm.ExecuteResult{Err: err}
	}
	if qp.ExplainQueryPlan {
		return vm.ExecuteResult{
			Text: qp.ToString(),
		}
	}

	var executeResult vm.ExecuteResult
	for {
		executionPlan, err := planner.ExecutionPlan()
		if err != nil {
			return vm.ExecuteResult{Err: err}
		}
		executeResult = *db.vm.Execute(executionPlan)
		if !errors.Is(executeResult.Err, vm.ErrVersionChanged) {
			break
		}
	}
	executeResult.Duration = time.Since(start)
	return executeResult
}

func (db *DB) getPlannerFor(statement compiler.Stmt) statementPlanner {
	switch s := statement.(type) {
	case *compiler.SelectStmt:
		return planner.NewSelect(db.catalog, s)
	case *compiler.CreateStmt:
		return planner.NewCreate(db.catalog, s)
	case *compiler.InsertStmt:
		return planner.NewInsert(db.catalog, s)
	}
	panic("statement not supported")
}
