package planner

import (
	"fmt"

	"github.com/chirst/cdb/compiler"
)

// This file defines the relational nodes in a logical query plan.

// logicalNode defines the interface for a node in the query plan tree.
type logicalNode interface {
	// children returns the child nodes.
	children() []logicalNode
	// print returns the string representation for explain.
	print() string
	// produce works with consume to generate byte code in the nodes associated
	// query plan. produce typically calls its children's produce methods until
	// a leaf is reached. When the leaf is reached consume is called which emits
	// byte code as the stack unwinds.
	produce()
	// consume works with produce.
	consume()
}

// TODO joinNode is unused, but remains as a prototype binary operation node.
type joinNode struct {
	// left is the left subtree of the join.
	left logicalNode
	// right is the right subtree of the join.
	right logicalNode
	// TODO operation is the type of join to be performed. Possibly left, right
	// or inner join. Could also have a field for join algorithm i.e. loop.
	operation string
}

func (j *joinNode) print() string {
	return fmt.Sprint(j.operation)
}

func (j *joinNode) children() []logicalNode {
	return []logicalNode{j.left, j.right}
}

// createNode represents a operation to create an object in the system catalog.
// For example a table, index, or trigger.
type createNode struct {
	plan *QueryPlan
	// objectName is the name of the index, trigger, or table.
	objectName string
	// objectType could be an index, trigger, or in this case a table.
	objectType string
	// tableName is name of the table this object is associated with.
	tableName string
	// schema is the json serialized schema definition for the object.
	schema string
	// noop is true when the node will perform no operation other than starting
	// a write transaction and then halting. The idea behind this is asserting
	// there is no work to do.
	//
	// Because the existence of the the object has already been determined while
	// generating the query plan. The query being ran means a noop is valid. This is
	// because the query plan would be invalidated given the existence of the object
	// has changed between query planning and query execution.
	noop bool
}

func (c *createNode) print() string {
	if c.noop {
		return fmt.Sprintf("assert table %s does not exist", c.tableName)
	}
	return fmt.Sprintf("create table %s", c.tableName)
}

func (c *createNode) children() []logicalNode {
	return []logicalNode{}
}

// insertNode represents an insert operation.
type insertNode struct {
	plan *QueryPlan
	// colValues are the values specified in the insert statement. It is two
	// dimensional i.e. VALUES (v1, v2), (v3, v4) is [[v1, v2], [v3, v4]].
	//
	// The logical planner must guarantee these values are in the correct
	// ordinal position as the code generator will not check.
	colValues [][]compiler.Expr
	// pkValues holds the pk expression separate from colValues for each values
	// entry. In case a pkValue wasn't specified in the values list a reasonable
	// value will be provided for the code generator or the autoPk will be true.
	pkValues []compiler.Expr
	// autoPk indicates the generator should use a NewRowIdCmd for pk
	// generation.
	autoPk bool
}

func (i *insertNode) print() string {
	return "insert"
}

func (i *insertNode) children() []logicalNode {
	return []logicalNode{}
}

type countNode struct {
	plan       *QueryPlan
	projection projection
}

func (c *countNode) children() []logicalNode {
	return []logicalNode{}
}

func (c *countNode) print() string {
	return "count table"
}

type constantNode struct {
	parent logicalNode
	plan   *QueryPlan
}

func (c *constantNode) print() string {
	return "constant data source"
}

func (c *constantNode) children() []logicalNode {
	return []logicalNode{}
}

type projection struct {
	expr compiler.Expr
	// alias is the alias of the projection or no alias for the zero value.
	alias string
}

type projectNode struct {
	child       logicalNode
	plan        *QueryPlan
	projections []projection
}

func (p *projectNode) print() string {
	return "project"
}

func (p *projectNode) children() []logicalNode {
	return []logicalNode{p.child}
}

type scanNode struct {
	parent logicalNode
	plan   *QueryPlan
}

func (s *scanNode) print() string {
	return "scan table"
}

func (s *scanNode) children() []logicalNode {
	return []logicalNode{}
}

type filterNode struct {
	child     logicalNode
	parent    logicalNode
	plan      *QueryPlan
	predicate compiler.Expr
}

func (f *filterNode) print() string {
	return "filter"
}

func (f *filterNode) children() []logicalNode {
	return []logicalNode{f.child}
}

type updateNode struct {
	child logicalNode
	plan  *QueryPlan
	// updateExprs is formed from the update statement AST. The idea is to
	// provide an expression for each column where the expression is either a
	// columnRef or the complex expression from the right hand side of the SET
	// keyword. Note it is important to provide the expressions in their correct
	// ordinal position as the generator will not try to order them correctly.
	//
	// The row id is not allowed to be updated at the moment because it could
	// cause infinite loops due to it changing the physical location of the
	// record. The query plan will have to use a temporary storage to update
	// primary keys.
	updateExprs []compiler.Expr
}

func (u *updateNode) print() string {
	return "update"
}

func (u *updateNode) children() []logicalNode {
	return []logicalNode{}
}
