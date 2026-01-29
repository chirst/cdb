package planner

import "github.com/chirst/cdb/compiler"

type optimizer struct{}

func (o *optimizer) optimizePlan(plan *QueryPlan) {
	if len(plan.root.children()) == 0 {
		return
	}
	filterNode, ok := plan.root.children()[0].(*filterNode)
	if !ok {
		return
	}
	sn, ok := filterNode.child.(*scanNode)
	if !ok {
		return
	}
	rowExpr := o.canOpt(filterNode.predicate)
	if rowExpr == nil {
		return
	}
	// If the filter can be moved to a seek then remove the filter and push the
	// predicate into a seek.
	seekN := &seekNode{
		parent:         filterNode.parent,
		plan:           sn.plan,
		tableName:      sn.tableName,
		rootPageNumber: sn.rootPageNumber,
		cursorId:       sn.cursorId,
		isWriteCursor:  sn.isWriteCursor,
		fullPredicate:  filterNode.predicate,
		predicate:      rowExpr,
	}
	seekN.parent.setChildren(seekN)
}

func (*optimizer) canOpt(predicate compiler.Expr) compiler.Expr {
	// The most basic optimization. Is the filter a primary key column ref equal
	// to a constant of some sort.
	be, ok := predicate.(*compiler.BinaryExpr)
	if !ok || be.Operator != compiler.OpEq {
		return nil
	}
	if lcr, ok := be.Left.(*compiler.ColumnRef); ok && lcr.IsPrimaryKey {
		switch t := be.Right.(type) {
		case *compiler.IntLit:
			return t
		case *compiler.StringLit:
			return t
		case *compiler.Variable:
			return t
		}
	}
	if rcr, ok := be.Left.(*compiler.ColumnRef); ok && rcr.IsPrimaryKey {
		switch t := be.Left.(type) {
		case *compiler.IntLit:
			return t
		case *compiler.StringLit:
			return t
		case *compiler.Variable:
			return t
		}
	}
	return nil
}
