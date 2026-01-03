package planner

import (
	"github.com/chirst/cdb/catalog"
	"github.com/chirst/cdb/compiler"
)

// catalogExprVisitor assigns catalog information to visited expressions.
type catalogExprVisitor struct {
	catalog   cevCatalog
	tableName string
	err       error
}

type cevCatalog interface {
	GetColumns(string) ([]string, error)
	GetPrimaryKeyColumn(string) (string, error)
	GetColumnType(tableName string, columnName string) (catalog.CdbType, error)
}

func (c *catalogExprVisitor) Init(catalog cevCatalog, tableName string) {
	c.catalog = catalog
	c.tableName = tableName
}

func (c *catalogExprVisitor) VisitColumnRefExpr(e *compiler.ColumnRef) {
	pkCol, err := c.catalog.GetPrimaryKeyColumn(c.tableName)
	if err != nil {
		c.err = err
		return
	}
	cols, err := c.catalog.GetColumns(c.tableName)
	if err != nil {
		c.err = err
		return
	}
	idx := 0
	e.IsPrimaryKey = e.Column == pkCol
	for _, col := range cols {
		if col != pkCol {
			if e.Column == col {
				e.ColIdx = idx
			}
			idx += 1
		}
	}

	t, err := c.catalog.GetColumnType(c.tableName, e.Column)
	if err != nil {
		c.err = err
		return
	}
	e.Type = t
}

func (c *catalogExprVisitor) VisitBinaryExpr(e *compiler.BinaryExpr)     {}
func (c *catalogExprVisitor) VisitUnaryExpr(e *compiler.UnaryExpr)       {}
func (c *catalogExprVisitor) VisitIntLit(e *compiler.IntLit)             {}
func (c *catalogExprVisitor) VisitStringLit(e *compiler.StringLit)       {}
func (c *catalogExprVisitor) VisitVariable(e *compiler.Variable)         {}
func (c *catalogExprVisitor) VisitFunctionExpr(e *compiler.FunctionExpr) {}
