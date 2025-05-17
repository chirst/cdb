// db serves as an interface for the database where raw SQL goes in and
// convenient data structures come out. db is intended to be consumed by things
// like a repl (read eval print loop), a program, or a transport protocol
package db

import (
	"errors"
	"slices"
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

func (db *DB) Tokenize(sql string) [][]compiler.Token {
	tokens := compiler.NewLexer(sql).Lex()
	statements := [][]compiler.Token{}
	start := 0
	for i := range tokens {
		if tokens[i].Value == ";" {
			statements = append(statements, tokens[start:i+1])
			start = i + 1
		}
	}
	if start == len(tokens) {
		return statements
	}
	return append(statements, tokens[start:])
}

func (db *DB) IsTerminated(statements [][]compiler.Token) bool {
	if len(statements) == 0 {
		return false
	}
	lastStatement := statements[len(statements)-1]
	for _, token := range slices.Backward(lastStatement) {
		if token.TokenType == compiler.TkWhitespace {
			continue
		}
		if token.Value == ";" {
			return true
		}
		break
	}
	return false
}

func (db *DB) Execute(tokens []compiler.Token) vm.ExecuteResult {
	start := time.Now()
	statement, err := compiler.NewParser(tokens).Parse()
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
