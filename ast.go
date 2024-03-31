// ast (Abstract Syntax Tree) defines a data structure representing a SQL
// program. This data structure is generated from the parser. This data
// structure is intended to be compiled into a execution plan.
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
