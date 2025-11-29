package compiler

import "github.com/chirst/cdb/catalog"

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
	From          *From
	ResultColumns []ResultColumn
	Where         Expr
}

// ResultColumn is the column definitions in a select statement.
type ResultColumn struct {
	// All is * in a select statement for example SELECT * FROM foo
	All bool
	// AllTable is all for a table for example SELECT foo.* FROM foo
	AllTable string
	// Expression contains the more complicated result column rules
	Expression Expr
	// Alias is the alias for an expression for example SELECT 1 AS "bar"
	Alias string
}

type From struct {
	TableName string
}

type CreateStmt struct {
	*StmtBase
	// IfNotExists is true when the create statement includes `CREATE TABLE IF
	// NOT EXISTS` meaning the statement should not throw if the table already
	// exists.
	IfNotExists bool
	TableName   string
	ColDefs     []ColDef
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
	// ColValues is a 2d list where the first dimension represents a row and the
	// second dimension represents a column value.
	ColValues [][]Expr
}

type UpdateStmt struct {
	*StmtBase
	TableName string
	// SetList is a mapping of column names to the expressions the column should
	// be updated to.
	SetList map[string]Expr
	// Predicate is the where clause. It may be nil when there is no where.
	Predicate Expr
}

type ExprVisitor interface {
	VisitBinaryExpr(*BinaryExpr)
	VisitUnaryExpr(*UnaryExpr)
	VisitColumnRefExpr(*ColumnRef)
	VisitIntLit(*IntLit)
	VisitStringLit(*StringLit)
	VisitVariable(*Variable)
	VisitFunctionExpr(*FunctionExpr)
}

// Expr defines the interface of an expression.
type Expr interface {
	// BreadthWalk implements the visitor pattern for a in-order breadth first
	// walk.
	BreadthWalk(v ExprVisitor)
}

// BinaryExpr is for an expression with two operands.
type BinaryExpr struct {
	Left     Expr
	Operator string
	Right    Expr
}

func (be *BinaryExpr) BreadthWalk(v ExprVisitor) {
	v.VisitBinaryExpr(be)
	be.Left.BreadthWalk(v)
	be.Right.BreadthWalk(v)
}

// UnaryExpr is an expression with one operand.
type UnaryExpr struct {
	Operator string
	Operand  Expr
}

func (ue *UnaryExpr) Accept(v ExprVisitor) {
	v.VisitUnaryExpr(ue)
	ue.Operand.BreadthWalk(v)
}

// ColumnRef is an expression with no operands. It references a column on a
// table.
type ColumnRef struct {
	Table  string
	Column string
	// Type is the type of the column
	Type catalog.CdbType
	// isPrimaryKey is filled out by the query planner. The property means the
	// column will be a key instead of a nth value.
	IsPrimaryKey bool
	// colIdx is filled out by the query planner. The property is the nth column
	// for non primary key values.
	ColIdx int
}

func (cr *ColumnRef) BreadthWalk(v ExprVisitor) {
	v.VisitColumnRefExpr(cr)
}

// IntLit is an expression that is a literal integer such as "1".
type IntLit struct {
	Value int
}

func (il *IntLit) BreadthWalk(v ExprVisitor) {
	v.VisitIntLit(il)
}

// StringLit is an expression that is a literal string such as "'asdf'".
type StringLit struct {
	Value string
}

func (sl *StringLit) BreadthWalk(v ExprVisitor) {
	v.VisitStringLit(sl)
}

type Variable struct {
	// Position is a unique integer defining what order the variable appeared in
	// the statement.
	Position int
}

func (vi *Variable) BreadthWalk(v ExprVisitor) {
	v.VisitVariable(vi)
}

// FunctionExpr is an expression that represents a function.
type FunctionExpr struct {
	// FnType corresponds to the type of function. For example fnCount is for
	// COUNT(*)
	FnType string
	Args   []Expr
}

const (
	FnCount = "COUNT"
)

func (f *FunctionExpr) BreadthWalk(v ExprVisitor) {
	v.VisitFunctionExpr(f)
}
