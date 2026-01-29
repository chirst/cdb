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
	// setChildren allows the caller to set a node's children. It may be
	// advisable to call children to get an idea how many children the node has.
	setChildren(n ...logicalNode)
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

func (j *joinNode) setChildren(n ...logicalNode) {
	j.left = n[0]
	j.right = n[1]
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
	// rootPageNumber is the page number of the system catalog.
	catalogRootPageNumber int
	// catalogCursorId is the id of the cursor associated with the system
	// catalog table being updated.
	catalogCursorId int
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

func (c *createNode) setChildren(n ...logicalNode) {}

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
	// tableName is the name of the table being inserted to.
	tableName string
	// rootPageNumber is the page number of the table being inserted to.
	rootPageNumber int
	// cursorId is the id of the cursor associated with the table being inserted
	// to.
	cursorId int
}

func (i *insertNode) print() string {
	return "insert"
}

func (i *insertNode) children() []logicalNode {
	return []logicalNode{}
}

func (i *insertNode) setChildren(n ...logicalNode) {}

type countNode struct {
	plan       *QueryPlan
	projection projection
	// tableName is the name of the table being scanned.
	tableName string
	// rootPageNumber is the page number of the table being scanned.
	rootPageNumber int
	// cursorId is the id of the cursor associated with the table being scanned.
	cursorId int
}

func (c *countNode) children() []logicalNode {
	return []logicalNode{}
}

func (c *countNode) print() string {
	return fmt.Sprintf("count table %s", c.tableName)
}

func (c *countNode) setChildren(n ...logicalNode) {}

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

func (c *constantNode) setChildren(n ...logicalNode) {}

type projection struct {
	expr compiler.Expr
	// alias is the alias of the projection or no alias for the zero value.
	alias string
}

type projectNode struct {
	child       logicalNode
	plan        *QueryPlan
	projections []projection
	// cursorId is the id of the cursor associated with the table being
	// projected. In the future this will likely need to be enhanced since
	// projections are not entirely meant for one table.
	cursorId int
}

func (p *projectNode) print() string {
	return "project"
}

func (p *projectNode) children() []logicalNode {
	return []logicalNode{p.child}
}

func (p *projectNode) setChildren(n ...logicalNode) {
	p.child = n[0]
}

type scanNode struct {
	parent logicalNode
	plan   *QueryPlan
	// tableName is the name of the table being scanned.
	tableName string
	// rootPageNumber is the page number of the table being scanned.
	rootPageNumber int
	// cursorId is the id of the cursor associated with the table being scanned.
	cursorId int
	// isWriteCursor is true when the cursor should be a write cursor.
	isWriteCursor bool
}

func (s *scanNode) print() string {
	return fmt.Sprintf("scan table %s", s.tableName)
}

func (s *scanNode) children() []logicalNode {
	return []logicalNode{}
}

func (s *scanNode) setChildren(n ...logicalNode) {}

type seekNode struct {
	parent logicalNode
	plan   *QueryPlan
	// tableName is the name of the table being searched.
	tableName string
	// rootPageNumber is the root page number of the table being searched.
	rootPageNumber int
	// cursorId is the id of the cursor associated with the search.
	cursorId int
	// isWriteCursor determines whether or not the cursor is for read or write.
	isWriteCursor bool
	// fullPredicate is the entire expression this node matches.
	fullPredicate compiler.Expr
	// predicate is a subset of fullPredicate usually excluding the columnRef.
	predicate compiler.Expr
}

func (s *seekNode) print() string {
	return fmt.Sprintf("seek table %s (%s)", s.tableName, s.fullPredicate.Print())
}

func (s *seekNode) children() []logicalNode {
	return []logicalNode{}
}

func (s *seekNode) setChildren(n ...logicalNode) {}

type filterNode struct {
	child     logicalNode
	parent    logicalNode
	plan      *QueryPlan
	predicate compiler.Expr
	// cursorId is the id of the cursor associated with the table being filtered.
	// In the future this will likely need to be enhanced since filters are not
	// entirely meant for one table.
	cursorId int
}

func (f *filterNode) print() string {
	return "filter (" + f.predicate.Print() + ")"
}

func (f *filterNode) children() []logicalNode {
	return []logicalNode{f.child}
}

func (f *filterNode) setChildren(n ...logicalNode) {
	f.child = n[0]
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
	// tableName is the name of the table being updated.
	tableName string
	// rootPageNumber is the page number of the table being updated.
	rootPageNumber int
	// cursorId is the id of the cursor associated with the table being updated.
	cursorId int
}

func (u *updateNode) print() string {
	return fmt.Sprintf("update table %s", u.tableName)
}

func (u *updateNode) children() []logicalNode {
	return []logicalNode{u.child}
}

func (u *updateNode) setChildren(n ...logicalNode) {
	u.child = n[0]
}

type deleteNode struct {
	child          logicalNode
	plan           *QueryPlan
	rootPageNumber int
	cursorId       int
}

func (d *deleteNode) print() string {
	return "delete"
}

func (d *deleteNode) children() []logicalNode {
	return []logicalNode{d.child}
}

func (d *deleteNode) setChildren(n ...logicalNode) {
	d.child = n[0]
}
