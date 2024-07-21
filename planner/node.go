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
	isCount bool
	colName string
}

// scanNode represents a full scan on a table
type scanNode struct {
	// tableName is the name of the table to be scanned
	tableName string
	// rootPage is the valid page number corresponding to the table
	rootPage int
	// scanColumns contains information about how the scan will project columns
	scanColumns []scanColumn
}

type scanColumn struct {
	// isPrimaryKey means the column will be a key instead of a nth value.
	isPrimaryKey bool
	// colIdx is the nth column for non primary key values.
	colIdx int
}

// countNode represents a special optimization when a table needs a full count
// with no filtering or other projections.
type countNode struct {
	// tableName is the name of the table to be scanned
	tableName string
	// rootPage is the valid page number corresponding to the table
	rootPage int
}

type joinNode struct {
	left      logicalNode
	right     logicalNode
	operation string
}

type createNode struct {
	// objectName is the name of the index, trigger, or table.
	objectName string
	// objectType could be an index, trigger, or in this case a table.
	objectType string
	// tableName is name of the table this object is associated with.
	tableName string
	// schema is the json serialized schema definition for the object.
	schema string
}
