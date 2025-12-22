package planner

import (
	"errors"
	"fmt"
	"math"

	"github.com/chirst/cdb/catalog"
	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/vm"
)

// selectCatalog defines the catalog methods needed by the select planner
type selectCatalog interface {
	GetColumns(tableOrIndexName string) ([]string, error)
	GetColumnType(tableName string, columnName string) (catalog.CdbType, error)
	GetRootPageNumber(tableOrIndexName string) (int, error)
	GetVersion() string
	GetPrimaryKeyColumn(tableName string) (string, error)
}

// selectPlanner is capable of generating a logical query plan and a physical
// execution plan for a select statement. The planners within are separated by
// their responsibility.
type selectPlanner struct {
	// queryPlanner is responsible for transforming the AST to a logical query
	// plan tree. This tree is made up of nodes that map closely to a relational
	// algebra tree. The query planner also performs binding and validation.
	queryPlanner *selectQueryPlanner
	// executionPlanner transforms the logical query tree to a bytecode routine,
	// built to be ran by the virtual machine.
	executionPlanner *selectExecutionPlanner
}

// selectQueryPlanner converts an AST to a logical query plan. Along the way it
// also validates the AST makes sense with the catalog (a process known as
// binding).
type selectQueryPlanner struct {
	// catalog contains the schema
	catalog selectCatalog
	// stmt contains the AST
	stmt *compiler.SelectStmt
	// queryPlan contains the logical plan being built. The root node must be a
	// projection.
	queryPlan *projectNodeV2
}

// selectExecutionPlanner converts logical nodes in a query plan tree to
// bytecode that can be run by the vm.
type selectExecutionPlanner struct {
	// queryPlan contains the logical plan. This node is populated by calling
	// the QueryPlan method.
	queryPlan *projectNodeV2
	// executionPlan contains the execution plan for the vm. This is built by
	// calling ExecutionPlan.
	executionPlan *vm.ExecutionPlan
}

// NewSelect returns an instance of a select planner for the given AST.
func NewSelect(catalog selectCatalog, stmt *compiler.SelectStmt) *selectPlanner {
	return &selectPlanner{
		queryPlanner: &selectQueryPlanner{
			catalog: catalog,
			stmt:    stmt,
		},
		executionPlanner: &selectExecutionPlanner{
			executionPlan: vm.NewExecutionPlan(
				catalog.GetVersion(),
				stmt.Explain,
			),
		},
	}
}

// QueryPlan generates the query plan tree for the planner.
func (p *selectPlanner) QueryPlan() (*QueryPlan, error) {
	qp, err := p.queryPlanner.getQueryPlan()
	if err != nil {
		return nil, err
	}
	p.executionPlanner.queryPlan = p.queryPlanner.queryPlan
	return qp, err
}

// getQueryPlan performs several passes on the AST to compute a more manageable
// tree structure of logical operators who closely resemble relational algebra
// operators.
//
// Firstly, getQueryPlan performs simplification to translate the projection
// portion of the select statement to uniform expressions. This means a "*",
// "table.*", or "alias.*" would simply be translated to ColumnRef expressions.
// From here the query is easier to work on as it is one consistent structure.
//
// From here, more simplification is performed. Folding computes constant
// expressions to reduce the complexity of the expression tree. This saves
// instructions ran during a scan. An example of this folding could be the
// binary expression 1 + 1 becoming a constant expression 2. Or a function UPPER
// on a string literal "foo" being simplified to just the string literal "FOO".
//
// Analysis steps are also performed. Such as assigning catalog information to
// ColumnRef expressions. This means associating table names with root page
// numbers, column names with their indices within a tuple, and column names
// with their constraints and available indexes.
func (p *selectQueryPlanner) getQueryPlan() (*QueryPlan, error) {
	err := p.optimizeResultColumns()
	if err != nil {
		return nil, err
	}

	var tableName string
	var rootPageNumber int
	if p.stmt.From != nil {
		tableName = p.stmt.From.TableName
	}
	if tableName != "" {
		rootPageNumber, err = p.catalog.GetRootPageNumber(tableName)
		if err != nil {
			return nil, errTableNotExist
		}
	}

	projections, err := p.getProjections()
	for i := range projections {
		cev := &catalogExprVisitor{}
		cev.Init(p.catalog, tableName)
		projections[i].expr.BreadthWalk(cev)
	}
	if err != nil {
		return nil, err
	}
	tt := transactionTypeRead
	if tableName == "" {
		tt = transactionTypeNone
	}
	plan := newPlan(tt, rootPageNumber)
	projectNode := &projectNodeV2{
		plan:        plan,
		projections: projections,
	}
	if p.stmt.Where != nil {
		cev := &catalogExprVisitor{}
		cev.Init(p.catalog, tableName)
		p.stmt.Where.BreadthWalk(cev)
		filterNode := &filterNodeV2{
			parent:    projectNode,
			plan:      plan,
			predicate: p.stmt.Where,
		}
		projectNode.child = filterNode
		if tableName == "" {
			constNode := &constantNodeV2{
				plan: plan,
			}
			filterNode.child = constNode
			constNode.parent = filterNode
		} else {
			scanNode := &scanNodeV2{
				plan: plan,
			}
			filterNode.child = scanNode
			scanNode.parent = filterNode
		}
	} else {
		if tableName == "" {
			constNode := &constantNodeV2{
				plan: plan,
			}
			projectNode.child = constNode
			constNode.parent = projectNode
		} else {
			scanNode := &scanNodeV2{
				plan: plan,
			}
			projectNode.child = scanNode
			scanNode.parent = projectNode
		}
	}
	p.queryPlan = projectNode
	plan.root = p.queryPlan
	return newQueryPlan(p.queryPlan, p.stmt.ExplainQueryPlan), nil
}

func (p *selectQueryPlanner) optimizeResultColumns() error {
	var err error
	for i := range p.stmt.ResultColumns {
		if p.stmt.ResultColumns[i].Expression != nil {
			p.stmt.ResultColumns[i].Expression, err = foldExpr(
				p.stmt.ResultColumns[i].Expression,
			)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// foldExpr folds expressions that can be computed before the query is executed.
// This optimization cuts down on instructions.
func foldExpr(e compiler.Expr) (compiler.Expr, error) {
	// Currently this only focuses on squashing binary expressions, but it could
	// do unary expressions or certain string manipulations. Anything involving
	// two constants.
	be, bok := e.(*compiler.BinaryExpr)
	if !bok {
		return e, nil
	}
	var err error
	be.Left, err = foldExpr(be.Left)
	if err != nil {
		return nil, err
	}
	be.Right, err = foldExpr(be.Right)
	if err != nil {
		return nil, err
	}
	// TODO need to support strings as well. Should probably share logic with vm
	// somehow.
	// TODO need to consider commutative operators such as + i.e. 4 + age + 5 vs
	// 4 + 5 + age where the former isn't folded due to the column, but could
	// be.
	le, lok := be.Left.(*compiler.IntLit)
	re, rok := be.Right.(*compiler.IntLit)
	if !lok || !rok {
		return be, nil
	}
	switch be.Operator {
	case compiler.OpAdd:
		return &compiler.IntLit{Value: le.Value + re.Value}, nil
	case compiler.OpDiv:
		if re.Value == 0 {
			return nil, errors.New("cannot divide by 0")
		}
		return &compiler.IntLit{Value: le.Value / re.Value}, nil
	case compiler.OpExp:
		return &compiler.IntLit{Value: int(math.Pow(float64(le.Value), float64(re.Value)))}, nil
	case compiler.OpMul:
		return &compiler.IntLit{Value: le.Value * re.Value}, nil
	case compiler.OpSub:
		return &compiler.IntLit{Value: le.Value - re.Value}, nil
	case compiler.OpEq:
		if le.Value == re.Value {
			return &compiler.IntLit{Value: 1}, nil
		}
		return &compiler.IntLit{Value: 0}, nil
	case compiler.OpGt:
		if le.Value > re.Value {
			return &compiler.IntLit{Value: 1}, nil
		}
		return &compiler.IntLit{Value: 0}, nil
	case compiler.OpLt:
		if le.Value < re.Value {
			return &compiler.IntLit{Value: 1}, nil
		}
		return &compiler.IntLit{Value: 0}, nil
	default:
		return nil, fmt.Errorf("folding not implemented for %s", be.Operator)
	}
}

// getCountNode supports the count function under special circumstances.
func (p *selectQueryPlanner) getCountNode(tableName string, rootPageNumber int) (*countNodeV2, error) {
	switch e := p.stmt.ResultColumns[0].Expression.(type) {
	case *compiler.FunctionExpr:
		if len(p.stmt.ResultColumns) != 1 {
			return nil, errors.New("count with other result columns not supported")
		}
		if e.FnType != compiler.FnCount {
			return nil, fmt.Errorf("only %s function is supported", e.FnType)
		}
		cn := &countNodeV2{
			plan: p.queryPlan.plan,
		}
		return cn, nil
	}
	return nil, nil
}

func (p *selectQueryPlanner) getScanColumns() ([]scanColumn, error) {
	pkColName, err := p.catalog.GetPrimaryKeyColumn(p.stmt.From.TableName)
	if err != nil {
		return nil, err
	}
	cols, err := p.catalog.GetColumns(p.stmt.From.TableName)
	if err != nil {
		return nil, err
	}
	scanColumns := []scanColumn{}
	idx := 0
	for _, c := range cols {
		if c == pkColName {
			scanColumns = append(scanColumns, &compiler.ColumnRef{
				Table:        p.stmt.From.TableName,
				Column:       c,
				IsPrimaryKey: c == pkColName,
			})
		} else {
			scanColumns = append(scanColumns, &compiler.ColumnRef{
				Table:  p.stmt.From.TableName,
				Column: c,
				ColIdx: idx,
			})
			idx += 1
		}
	}
	return scanColumns, nil
}

func (p *selectQueryPlanner) getProjections() ([]projectionV2, error) {
	var projections []projectionV2
	for _, resultColumn := range p.stmt.ResultColumns {
		if resultColumn.All {
			cols, err := p.catalog.GetColumns(p.stmt.From.TableName)
			if err != nil {
				return nil, err
			}
			for _, c := range cols {
				projections = append(projections, projectionV2{
					expr: &compiler.ColumnRef{
						Table:  p.stmt.From.TableName,
						Column: c,
					},
				})
			}
		} else if resultColumn.AllTable != "" {
			cols, err := p.catalog.GetColumns(p.stmt.From.TableName)
			if err != nil {
				return nil, err
			}
			for _, c := range cols {
				projections = append(projections, projectionV2{
					expr: &compiler.ColumnRef{
						Table:  p.stmt.From.TableName,
						Column: c,
					},
				})
			}
		} else if resultColumn.Expression != nil {
			projections = append(projections, projectionV2{
				expr:  resultColumn.Expression,
				alias: resultColumn.Alias,
			})
		}
	}
	return projections, nil
}

// ExecutionPlan returns the bytecode execution plan for the planner. Calling
// QueryPlan is not a prerequisite to this method as it will be called by
// ExecutionPlan if needed.
func (sp *selectPlanner) ExecutionPlan() (*vm.ExecutionPlan, error) {
	if sp.queryPlanner.queryPlan == nil {
		_, err := sp.QueryPlan()
		if err != nil {
			return nil, err
		}
	}
	return sp.executionPlanner.getExecutionPlan()
}

func (p *selectExecutionPlanner) getExecutionPlan() (*vm.ExecutionPlan, error) {
	p.setResultHeader()
	p.queryPlan.plan.compile()
	p.executionPlan.Commands = p.queryPlan.plan.commands
	return p.executionPlan, nil
}

func (p *selectExecutionPlanner) setResultHeader() {
	resultHeader := []string{}
	for range p.queryPlan.projections {
		resultHeader = append(resultHeader, "unknown")
	}
	p.executionPlan.ResultHeader = resultHeader
}

// setResultTypes attempts to precompute the type for each result column expr.
func (p *selectExecutionPlanner) setResultTypes(exprs []compiler.Expr) error {
	resolvedTypes := []catalog.CdbType{}
	for _, expr := range exprs {
		t, err := getExprType(expr)
		if err != nil {
			return err
		}
		resolvedTypes = append(resolvedTypes, t)
	}
	p.executionPlan.ResultTypes = resolvedTypes
	return nil
}

// getExprType resolves the type of the expression. In case a expression is a
// variable it will need to be resolved later on.
func getExprType(expr compiler.Expr) (catalog.CdbType, error) {
	switch c := expr.(type) {
	case *compiler.IntLit:
		return catalog.CdbType{ID: catalog.CTInt}, nil
	case *compiler.StringLit:
		return catalog.CdbType{ID: catalog.CTStr}, nil
	case *compiler.Variable:
		return catalog.CdbType{ID: catalog.CTVar, VarPosition: c.Position}, nil
	case *compiler.FunctionExpr:
		return catalog.CdbType{ID: catalog.CTInt}, nil
	case *compiler.ColumnRef:
		return c.Type, nil
	case *compiler.BinaryExpr:
		left, err := getExprType(c.Left)
		if err != nil {
			return catalog.CdbType{ID: catalog.CTUnknown}, err
		}
		right, err := getExprType(c.Right)
		if err != nil {
			return catalog.CdbType{ID: catalog.CTUnknown}, err
		}
		if left.ID > right.ID {
			return left, nil
		}
		return right, nil
	default:
		return catalog.CdbType{ID: catalog.CTUnknown}, fmt.Errorf("no handler for expr type %v", expr)
	}
}
