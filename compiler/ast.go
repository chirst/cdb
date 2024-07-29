package compiler

// ast (Abstract Syntax Tree) defines a data structure representing a SQL
// program. This data structure is generated from the parser. This data
// structure is intended to be compiled into a execution plan.

type Stmt interface{}

type StmtBase struct {
	Explain          bool
	ExplainQueryPlan bool
}

type SelectStmt struct {
	*StmtBase
	From         *From
	ResultColumn ResultColumn
}

type ResultColumn struct {
	All   bool
	Count bool
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
	ColName    string
	ColType    string
	PrimaryKey bool
}

type InsertStmt struct {
	*StmtBase
	TableName string
	ColNames  []string
	ColValues [][]string
}

// type Expr interface {
// 	Type() string
// }

// type BinaryExpr struct {
// 	Left     Expr
// 	Operator string
// 	Right    Expr
// }

// type UnaryExpr struct {
// 	Operator string
// 	Operand  Expr
// }

// type ColumnRef struct {
// 	Table  string
// 	Column string
// }

// type IntLit struct {
// 	Value int
// }

// type FunctionExpr struct {
// 	Name string
// 	Args []Expr
// }
