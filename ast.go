// ast (Abstract Syntax Tree) defines a data structure representing a SQL
// program. This data structure is generated from the parser. This data
// structure is intended to be compiled into a execution plan.
package main

type stmtList []stmt

type stmt interface {
	getPlan() executionPlan
}

const (
	stmtTypeSelect = iota + 1
)

func newSelectStmt(explain bool) *selectStmt {
	return &selectStmt{
		explain: explain,
	}
}

type selectStmt struct {
	explain       bool
	from          *tableOrSubQuery
	resultColumns []resultColumn
}

type resultColumn struct {
	all  bool // SELECT * FROM foo f;
	expr *expr
}

type tableOrSubQuery struct {
	tableName string
}

type expr struct {
	literal *literal
}

type literal struct {
	numericLiteral int
}
