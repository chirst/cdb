// ast (Abstract Syntax Tree) defines a data structure representing a SQL
// program. This data structure is generated from the parser. This data
// structure is intended to be compiled into a execution plan.
package compiler

type StmtList []Stmt

type Stmt interface{}

type StmtBase struct {
	Explain bool
}

type SelectStmt struct {
	*StmtBase
	From         *From
	ResultColumn ResultColumn
}

type ResultColumn struct {
	All bool
}

type From struct {
	TableName string
}
