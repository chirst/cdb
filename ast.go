// ast (Abstract Syntax Tree) defines a data structure representing a SQL
// program. This data structure is typically generated from a parser and lexer.
// This data structure is intended to be compiled into a execution plan.
//
// This implementation is inspired by https://www.sqlite.org/lang.html
package main

type stmtList []any

const (
	stmtTypeSelect = iota + 1
)

type selectStmt struct {
	explain       bool
	from          *tableOrSubQuery
	resultColumns []resultColumn
}

type resultColumn struct {
	all bool // SELECT * FROM foo f;
}

type tableOrSubQuery struct {
	tableName string
}
