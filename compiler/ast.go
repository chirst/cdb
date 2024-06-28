package compiler

// ast (Abstract Syntax Tree) defines a data structure representing a SQL
// program. This data structure is generated from the parser. This data
// structure is intended to be compiled into a execution plan.

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

type CreateStmt struct {
	*StmtBase
	TableName string
	ColDefs   []ColDef
}

type ColDef struct {
	ColName string
	ColType string
}

type InsertStmt struct {
	*StmtBase
	TableName string
	ColNames  []string
	ColValues []string
}
