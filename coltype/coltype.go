// coltype exports constants used for column typing across several modules. Such
// as the planner, vm, and the catalog. These types are used in a result to
// indicate what type of data is stored in each result column.
package coltype

const (
	Unknown = iota
	Int
	Var
	Str
)

type CT = int
