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
	From          *From
	ResultColumns []ResultColumn
}

type ResultColumn struct {
	All  bool // SELECT * FROM foo f;
	Expr *Expr
}

type From struct {
	TableName string
}

type Expr struct {
	Literal *Literal
}

type Literal struct {
	NumericLiteral int
}
