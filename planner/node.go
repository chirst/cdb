package planner

import "github.com/chirst/cdb/compiler"

// This file defines the relational nodes in a logical query plan.

// logicalNode defines the interface for a node in the query plan tree.
type logicalNode interface {
	children() []logicalNode
	print() string
	produce()
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
