package planner

// logicalNode defines the interface for a node in the query plan tree.
type logicalNode interface {
	children() []logicalNode
	print() string
}

// projectNode defines what columns should be projected.
type projectNode struct {
	projections []projection
	child       logicalNode
}

type projection struct {
	isAll   bool
	isCount bool
}

// scanNode represents a full scan on a table
type scanNode struct {
	// tableName is the name of the table to be scanned
	tableName string
	// rootPage is the valid page number corresponding to the table
	rootPage int
}

// countNode represents a special optimization when a table needs a full count
// with no filtering or other projections.
type countNode struct {
	// tableName is the name of the table to be counted
	tableName string
	// rootPage is the valid page number corresponding to the table
	rootPage int
}

type joinNode struct {
	left      logicalNode
	right     logicalNode
	operation string
}
