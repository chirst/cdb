package planner

import (
	"errors"
	"fmt"

	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/vm"
)

// selectCatalog defines the catalog methods needed by the select planner
type selectCatalog interface {
	GetColumns(tableOrIndexName string) ([]string, error)
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
	queryPlan *projectNode
}

// selectExecutionPlanner converts logical nodes in a query plan tree to
// bytecode that can be run by the vm.
type selectExecutionPlanner struct {
	// queryPlan contains the logical plan. This node is populated by calling
	// the QueryPlan method.
	queryPlan *projectNode
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
	// TODO revisit this functions comment and look into breaking this into
	// smaller functions.
	if p.stmt.From == nil || p.stmt.From.TableName == "" {
		child := &constantNode{
			resultColumns: p.stmt.ResultColumns,
		}
		projections, err := p.getProjections()
		if err != nil {
			return nil, err
		}
		p.queryPlan = &projectNode{
			projections: projections,
			child:       child,
		}
		return newQueryPlan(p.queryPlan, p.stmt.ExplainQueryPlan), nil
	}
	tableName := p.stmt.From.TableName
	rootPageNumber, err := p.catalog.GetRootPageNumber(tableName)
	if err != nil {
		return nil, err
	}
	// For now, count function is handled specially.
	for i, resultColumn := range p.stmt.ResultColumns {
		if i != 0 {
			return nil, errors.New("count with other result columns not supported")
		}
		switch e := resultColumn.Expression.(type) {
		case *compiler.FunctionExpr:
			if e.FnType != compiler.FnCount {
				return nil, fmt.Errorf("only %s function is supported", e.FnType)
			}
			child := &countNode{
				tableName: tableName,
				rootPage:  rootPageNumber,
			}
			projections, err := p.getProjections()
			if err != nil {
				return nil, err
			}
			p.queryPlan = &projectNode{
				projections: projections,
				child:       child,
			}
			return newQueryPlan(p.queryPlan, p.stmt.ExplainQueryPlan), nil
		}
		break
	}

	// At this point a constantNode and countNode should be ruled out. The
	// planner isn't looking at using indexes yet so we are safe to focus on
	// scanNodes.
	child := &scanNode{
		tableName:   tableName,
		rootPage:    rootPageNumber,
		scanColumns: []scanColumn{},
	}
	for _, resultColumn := range p.stmt.ResultColumns {
		if resultColumn.All {
			cols, err := p.getScanColumns()
			if err != nil {
				return nil, err
			}
			child.scanColumns = append(child.scanColumns, cols...)
		} else if resultColumn.AllTable != "" {
			if tableName != resultColumn.AllTable {
				return nil, fmt.Errorf("invalid expression %s.*", resultColumn.AllTable)
			}
			cols, err := p.getScanColumns()
			if err != nil {
				return nil, err
			}
			child.scanColumns = append(child.scanColumns, cols...)
		} else if resultColumn.Expression != nil {
			cev := &catalogExprVisitor{}
			if cev.err != nil {
				return nil, err
			}
			cev.Init(p.catalog, child.tableName)
			resultColumn.Expression.BreadthWalk(cev)
			child.scanColumns = append(child.scanColumns, resultColumn.Expression)
		} else {
			return nil, fmt.Errorf("unhandled result column %#v", resultColumn)
		}
	}
	projections, err := p.getProjections()
	if err != nil {
		return nil, err
	}
	p.queryPlan = &projectNode{
		projections: projections,
		child:       child,
	}
	return newQueryPlan(p.queryPlan, p.stmt.ExplainQueryPlan), nil
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

func (p *selectQueryPlanner) getProjections() ([]projection, error) {
	var projections []projection
	for _, resultColumn := range p.stmt.ResultColumns {
		if resultColumn.All {
			cols, err := p.catalog.GetColumns(p.stmt.From.TableName)
			if err != nil {
				return nil, err
			}
			for _, c := range cols {
				projections = append(projections, projection{
					colName: c,
				})
			}
		} else if resultColumn.AllTable != "" {
			cols, err := p.catalog.GetColumns(p.stmt.From.TableName)
			if err != nil {
				return nil, err
			}
			for _, c := range cols {
				projections = append(projections, projection{
					colName: c,
				})
			}
		} else if resultColumn.Expression != nil {
			switch e := resultColumn.Expression.(type) {
			case *compiler.ColumnRef:
				colName := e.Column
				if resultColumn.Alias != "" {
					colName = resultColumn.Alias
				}
				projections = append(projections, projection{
					colName: colName,
				})
			case *compiler.FunctionExpr:
				projections = append(projections, projection{
					isCount: true,
					colName: resultColumn.Alias,
				})
			default:
				projections = append(projections, projection{
					isCount: false,
					colName: resultColumn.Alias,
				})
			}
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
	p.buildInit()
	switch c := p.queryPlan.child.(type) {
	case *scanNode:
		p.executionPlan.Append(&vm.TransactionCmd{P2: 0})
		if err := p.buildScan(c); err != nil {
			return nil, err
		}
	case *countNode:
		p.executionPlan.Append(&vm.TransactionCmd{P2: 0})
		p.buildOptimizedCountScan(c)
	case *constantNode:
		p.buildConstantNode(c)
	default:
		return nil, fmt.Errorf("unhandled node %#v", c)
	}
	p.executionPlan.Append(&vm.HaltCmd{})
	return p.executionPlan, nil
}

func (p *selectExecutionPlanner) setResultHeader() {
	resultHeader := []string{}
	for _, p := range p.queryPlan.projections {
		resultHeader = append(resultHeader, p.colName)
	}
	p.executionPlan.ResultHeader = resultHeader
}

func (p *selectExecutionPlanner) buildInit() {
	p.executionPlan.Append(&vm.InitCmd{P2: 1})
}

// TODO look into refactoring/moving visitors throughout this file.
type catalogExprVisitor struct {
	catalog   selectCatalog
	tableName string
	err       error
}

func (c *catalogExprVisitor) Init(catalog selectCatalog, tableName string) {
	c.catalog = catalog
	c.tableName = tableName
}
func (c *catalogExprVisitor) VisitBinaryExpr(e *compiler.BinaryExpr) {}
func (c *catalogExprVisitor) VisitUnaryExpr(e *compiler.UnaryExpr)   {}
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
}
func (c *catalogExprVisitor) VisitIntLit(e *compiler.IntLit)             {}
func (c *catalogExprVisitor) VisitStringLit(e *compiler.StringLit)       {}
func (c *catalogExprVisitor) VisitFunctionExpr(e *compiler.FunctionExpr) {}

type constantRegisterVisitor struct {
	nextOpenRegister  int
	constantRegisters map[int]int
}

func (c *constantRegisterVisitor) Init(openRegister int) {
	c.constantRegisters = make(map[int]int)
	c.nextOpenRegister = openRegister
}
func (c *constantRegisterVisitor) fillRegisterIfNeeded(v int) {
	found := false
	for k := range c.constantRegisters {
		if k == v {
			found = true
		}
	}
	if !found {
		c.constantRegisters[v] = c.nextOpenRegister
		c.nextOpenRegister += 1
	}
}
func (c *constantRegisterVisitor) GetRegisters() map[int]int { return c.constantRegisters }
func (c *constantRegisterVisitor) GetRegisterCommands() []vm.Command {
	ret := []vm.Command{}
	for k := range c.constantRegisters {
		ret = append(ret, &vm.IntegerCmd{P1: k, P2: c.constantRegisters[k]})
	}
	return ret
}
func (c *constantRegisterVisitor) VisitBinaryExpr(e *compiler.BinaryExpr)     {}
func (c *constantRegisterVisitor) VisitUnaryExpr(e *compiler.UnaryExpr)       {}
func (c *constantRegisterVisitor) VisitColumnRefExpr(e *compiler.ColumnRef)   {}
func (c *constantRegisterVisitor) VisitIntLit(e *compiler.IntLit)             { c.fillRegisterIfNeeded(e.Value) }
func (c *constantRegisterVisitor) VisitStringLit(e *compiler.StringLit)       {}
func (c *constantRegisterVisitor) VisitFunctionExpr(e *compiler.FunctionExpr) {}

type exprCommandBuilder struct {
	cursorId       int
	openRegister   int
	outputRegister int
	commands       []vm.Command
	litRegisters   map[int]int
}

func (e *exprCommandBuilder) Init(cursorId int, openRegister int, litRegisters map[int]int, outputRegister int) {
	e.cursorId = cursorId
	e.openRegister = openRegister
	e.litRegisters = litRegisters
	e.outputRegister = outputRegister
}

func (e *exprCommandBuilder) getNextRegister(level int) int {
	if level == 0 {
		return e.outputRegister
	}
	r := e.openRegister
	e.openRegister += 1
	return r
}

func (e *exprCommandBuilder) BuildCommands(root compiler.Expr, level int) (outRegister int) {
	switch n := root.(type) {
	case *compiler.BinaryExpr:
		ol := e.BuildCommands(n.Left, level+1)
		or := e.BuildCommands(n.Right, level+1)
		r := e.getNextRegister(level)
		switch n.Operator {
		case compiler.OpAdd:
			e.commands = append(e.commands, &vm.AddCmd{P1: ol, P2: or, P3: r})
		case compiler.OpDiv:
			e.commands = append(e.commands, &vm.DivideCmd{P1: ol, P2: or, P3: r})
		case compiler.OpMul:
			e.commands = append(e.commands, &vm.MultiplyCmd{P1: ol, P2: or, P3: r})
		case compiler.OpExp:
			e.commands = append(e.commands, &vm.ExponentCmd{P1: ol, P2: or, P3: r})
		case compiler.OpSub:
			e.commands = append(e.commands, &vm.SubtractCmd{P1: ol, P2: or, P3: r})
		default:
			panic("no vm command for operator")
		}
		return r
	case *compiler.ColumnRef:
		r := e.getNextRegister(level)
		if n.IsPrimaryKey {
			e.commands = append(e.commands, &vm.RowIdCmd{P1: e.cursorId, P2: r})
		} else {
			e.commands = append(e.commands, &vm.ColumnCmd{P1: e.cursorId, P2: n.ColIdx, P3: r})
		}
		return r
	case *compiler.IntLit:
		if level == 0 {
			e.commands = append(e.commands, &vm.CopyCmd{P1: e.litRegisters[n.Value], P2: e.outputRegister})
		}
		return e.litRegisters[n.Value]
	}
	panic("unhandled expression in expr command builder")
}

func (p *selectExecutionPlanner) buildScan(n *scanNode) error {
	// Walks scan columns and builds a map of constant values to registers.
	// These constants can be used in the innards of the scan.
	const beginningRegister = 1
	crv := &constantRegisterVisitor{}
	crv.Init(beginningRegister)
	for _, c := range n.scanColumns {
		c.BreadthWalk(crv)
	}
	rcs := crv.GetRegisterCommands()
	for _, rc := range rcs {
		p.executionPlan.Append(rc)
	}
	constantRegisters := crv.GetRegisters()

	// Open an available cursor. Can just be 1 for now since no queries are
	// supported at the moment that requires more than one cursor.
	const cursorId = 1
	p.executionPlan.Append(&vm.OpenReadCmd{P1: cursorId, P2: n.rootPage})

	// Rewind moves the aforementioned cursor to the "start" of the table.
	rwc := &vm.RewindCmd{P1: cursorId}
	p.executionPlan.Append(rwc)

	// Mark beginning of innards for rewind
	innardsBeginningCommand := len(p.executionPlan.Commands)

	// Reserve registers for the column result. Claim registers after as needed.
	startScanRegister := crv.nextOpenRegister
	endScanRegisterOffset := len(n.scanColumns)

	// This is the innards of the scan meaning how each result column is handled
	// per iteration of the scan (loop).
	for i, c := range n.scanColumns {
		exprBuilder := &exprCommandBuilder{}
		exprBuilder.Init(1, startScanRegister+endScanRegisterOffset, constantRegisters, startScanRegister+i)
		exprBuilder.BuildCommands(c, 0)
		for _, tc := range exprBuilder.commands {
			p.executionPlan.Append(tc)
		}
	}

	// Result row gathers the aforementioned innards of the scan and makes them
	// into a single row for the query results.
	p.executionPlan.Append(&vm.ResultRowCmd{P1: startScanRegister, P2: endScanRegisterOffset})

	// Falls through or goes back to the start of the scan loop.
	p.executionPlan.Append(&vm.NextCmd{P1: cursorId, P2: innardsBeginningCommand})

	// Must tell the rewind command where to go in case the table is empty.
	rwc.P2 = len(p.executionPlan.Commands)
	return nil
}

// buildOptimizedCountScan is a special optimization made when a table only has
// a count aggregate and no other projections. Since the optimized scan
// aggregates the count of tuples on each page, but does not look at individual
// tuples.
func (p *selectExecutionPlanner) buildOptimizedCountScan(n *countNode) {
	const cursorId = 1
	p.executionPlan.Append(&vm.OpenReadCmd{P1: cursorId, P2: n.rootPage})
	p.executionPlan.Append(&vm.CountCmd{P1: cursorId, P2: 1})
	p.executionPlan.Append(&vm.ResultRowCmd{P1: 1, P2: 1})
}

// buildConstantNode is a single row operation produced by a "select" without a
// "from".
func (p *selectExecutionPlanner) buildConstantNode(n *constantNode) {
	// Build registers with constants. These are likely extra instructions, but
	// okay since it allows this to follow the same pattern a scan does.
	const beginningRegister = 1
	crv := &constantRegisterVisitor{}
	crv.Init(beginningRegister)
	for _, c := range n.resultColumns {
		c.Expression.BreadthWalk(crv)
	}
	rcs := crv.GetRegisterCommands()
	for _, rc := range rcs {
		p.executionPlan.Append(rc)
	}
	constantRegisters := crv.GetRegisters()

	// Like a scan, but for a single row.
	reservedRegisterStart := crv.nextOpenRegister
	reservedRegisterOffset := len(n.resultColumns)
	for i, rc := range n.resultColumns {
		exprBuilder := &exprCommandBuilder{}
		exprBuilder.Init(1, reservedRegisterStart+reservedRegisterOffset, constantRegisters, reservedRegisterStart+i)
		exprBuilder.BuildCommands(rc.Expression, 0)
		for _, tc := range exprBuilder.commands {
			p.executionPlan.Append(tc)
		}
	}
	p.executionPlan.Append(&vm.ResultRowCmd{P1: reservedRegisterStart, P2: reservedRegisterOffset})
}
