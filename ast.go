// ast (Abstract Syntax Tree) defines a data structure representing a SQL
// program. This data structure is generated from the parser. This data
// structure is intended to be compiled into a execution plan.
package main

type stmtList []stmt

type stmt interface{}

type stmtBase struct {
	explain bool
}

type selectStmt struct {
	*stmtBase
	from          *from
	resultColumns []resultColumn
}

type resultColumn struct {
	all  bool // SELECT * FROM foo f;
	expr *expr
}

type from struct {
	tableName string
}

type expr struct {
	literal *literal
}

type literal struct {
	numericLiteral int
}
