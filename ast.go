// ast (Abstract Syntax Tree) defines a data structure representing a SQL
// program. This data structure is typically generated from a parser and lexer.
// This data structure is intended to be compiled into a execution plan.
//
// This implementation is inspired by https://www.sqlite.org/lang.html
package main

type stmtList struct {
	statements []stmt
}

const (
	stmtTypeSelect = iota + 1
	stmtTypeCreate
)

type stmt struct {
	stmtType        int
	selectStmt      *selectStmt
	createTableStmt *createTableStmt
	explain         bool
}

type selectStmt struct {
	from          *tableOrSubQuery
	distinct      bool
	resultColumns []resultColumn
	where         *expr
	groupBy       []expr
	having        *expr
}

type resultColumn struct {
	expr         *expr  // SELECT COUNT(*) FROM foo f;
	exprAlias    string // SELECT COUNT(*) AS "fCount" FROM foo f;
	all          bool   // SELECT * FROM foo f;
	allTableName string // SELECT f.* FROM foo f;
}

type expr struct{}

type tableOrSubQuery struct {
}

type createTableStmt struct {
}
