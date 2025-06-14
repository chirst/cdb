package planner

import "github.com/chirst/cdb/compiler"

// catalogExprVisitor assigns catalog information to visited expressions.
type catalogExprVisitor struct {
	catalog   selectCatalog
	tableName string
	err       error
}

func (c *catalogExprVisitor) Init(catalog selectCatalog, tableName string) {
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

	t, err := c.catalog.GetColumnType(e.Table, e.Column)
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
