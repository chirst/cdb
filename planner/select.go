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
// execution plan for a select statement.
type selectPlanner struct {
	// catalog contains the schema.
	catalog selectCatalog
	// stmt contains the AST.
	stmt *compiler.SelectStmt
	// queryPlan contains the plan being built.
	queryPlan *QueryPlan
	// executionPlan contains the execution plan for the vm. This is built by
	// calling ExecutionPlan.
	executionPlan *vm.ExecutionPlan
}

// NewSelect returns an instance of a select planner for the given AST.
func NewSelect(catalog selectCatalog, stmt *compiler.SelectStmt) *selectPlanner {
	return &selectPlanner{
		catalog: catalog,
		stmt:    stmt,
		executionPlan: vm.NewExecutionPlan(
			catalog.GetVersion(),
			stmt.Explain,
		),
	}
}

// QueryPlan generates the query plan tree for the planner.
func (p *selectPlanner) QueryPlan() (*QueryPlan, error) {
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
	if err != nil {
		return nil, err
	}
	for i := range projections {
		cev := &catalogExprVisitor{}
		cev.Init(p.catalog, tableName)
		projections[i].expr.BreadthWalk(cev)
	}

	hasFunc := false
	for i := range projections {
		_, ok := projections[i].expr.(*compiler.FunctionExpr)
		if ok {
			hasFunc = true
		}
	}
	if hasFunc {
		if len(projections) != 1 {
			return nil, errors.New("only one projection allowed for COUNT")
		}
		if tableName == "" {
			return nil, errors.New("must have from for COUNT")
		}
		cn := &countNode{
			projection:     projections[0],
			rootPageNumber: rootPageNumber,
			tableName:      tableName,
			cursorId:       1,
		}
		plan := newQueryPlan(
			cn,
			p.stmt.ExplainQueryPlan,
			transactionTypeRead,
		)
		cn.plan = plan
		p.queryPlan = plan
		return plan, nil
	}

	tt := transactionTypeRead
	if tableName == "" {
		tt = transactionTypeNone
	}
	projectNode := &projectNode{
		projections: projections,
		cursorId:    1,
	}
	plan := newQueryPlan(projectNode, p.stmt.ExplainQueryPlan, tt)
	projectNode.plan = plan
	if p.stmt.Where != nil {
		cev := &catalogExprVisitor{}
		cev.Init(p.catalog, tableName)
		p.stmt.Where.BreadthWalk(cev)
		filterNode := &filterNode{
			parent:    projectNode,
			plan:      plan,
			predicate: p.stmt.Where,
			cursorId:  1,
		}
		projectNode.child = filterNode
		if tableName == "" {
			constNode := &constantNode{
				plan: plan,
			}
			filterNode.child = constNode
			constNode.parent = filterNode
		} else {
			scanNode := &scanNode{
				plan:           plan,
				tableName:      tableName,
				rootPageNumber: rootPageNumber,
				cursorId:       1,
			}
			filterNode.child = scanNode
			scanNode.parent = filterNode
		}
	} else {
		if tableName == "" {
			constNode := &constantNode{
				plan: plan,
			}
			projectNode.child = constNode
			constNode.parent = projectNode
		} else {
			scanNode := &scanNode{
				plan:           plan,
				tableName:      tableName,
				rootPageNumber: rootPageNumber,
				cursorId:       1,
			}
			projectNode.child = scanNode
			scanNode.parent = projectNode
		}
	}
	p.queryPlan = plan
	plan.root = projectNode
	(&optimizer{}).optimizePlan(plan)
	return plan, nil
}

// ExecutionPlan returns the bytecode execution plan for the planner. Calling
// QueryPlan is not a prerequisite to this method as it will be called by
// ExecutionPlan if needed.
func (sp *selectPlanner) ExecutionPlan() (*vm.ExecutionPlan, error) {
	if sp.queryPlan == nil {
		_, err := sp.QueryPlan()
		if err != nil {
			return nil, err
		}
	}
	sp.setResultHeader()
	sp.queryPlan.compile()
	sp.executionPlan.Commands = sp.queryPlan.commands
	return sp.executionPlan, nil
}

func (p *selectPlanner) optimizeResultColumns() error {
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

func (p *selectPlanner) getProjections() ([]projection, error) {
	var projections []projection
	for _, resultColumn := range p.stmt.ResultColumns {
		if resultColumn.All {
			cols, err := p.catalog.GetColumns(p.stmt.From.TableName)
			if err != nil {
				return nil, err
			}
			for _, c := range cols {
				projections = append(projections, projection{
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
				projections = append(projections, projection{
					expr: &compiler.ColumnRef{
						Table:  p.stmt.From.TableName,
						Column: c,
					},
				})
			}
		} else if resultColumn.Expression != nil {
			projections = append(projections, projection{
				expr:  resultColumn.Expression,
				alias: resultColumn.Alias,
			})
		}
	}
	return projections, nil
}

func (p *selectPlanner) setResultHeader() {
	resultHeader := []string{}
	switch t := p.queryPlan.root.(type) {
	case *projectNode:
		projectExprs := []compiler.Expr{}
		for _, projection := range t.projections {
			header := ""
			if projection.alias == "" {
				if cr, ok := projection.expr.(*compiler.ColumnRef); ok {
					header = cr.Column
				}
			} else {
				header = projection.alias
			}
			resultHeader = append(resultHeader, header)
			projectExprs = append(projectExprs, projection.expr)
		}
		p.setResultTypes(projectExprs)
	case *countNode:
		resultHeader = append(resultHeader, t.projection.alias)
		p.setResultTypes([]compiler.Expr{t.projection.expr})
	default:
		panic("unhandled node for result header")
	}
	p.executionPlan.ResultHeader = resultHeader
}

// setResultTypes attempts to precompute the type for each result column expr.
func (p *selectPlanner) setResultTypes(exprs []compiler.Expr) error {
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
