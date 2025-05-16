package planner

import (
	"errors"
	"fmt"
	"math"

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
	err := p.optimizeResultColumns()
	if err != nil {
		return nil, err
	}

	// Constant query has no "from".
	if p.stmt.From == nil || p.stmt.From.TableName == "" {
		child := &constantNode{
			resultColumns: p.stmt.ResultColumns,
			predicate:     p.stmt.Where,
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

	// Count node is specially supported for now.
	qp, err := p.getCountNode(tableName, rootPageNumber)
	if err != nil {
		return nil, err
	}
	if qp != nil {
		return qp, nil
	}

	if p.stmt.Where != nil {
		cev := &catalogExprVisitor{}
		cev.Init(p.catalog, p.stmt.From.TableName)
		if cev.err != nil {
			return nil, err
		}
		p.stmt.Where.BreadthWalk(cev)
	}

	// At this point a constant and count should be ruled out. The planner isn't
	// looking at using indexes yet so we are safe to focus on scanNodes.
	child := &scanNode{
		tableName:     tableName,
		rootPage:      rootPageNumber,
		scanColumns:   []scanColumn{},
		scanPredicate: p.stmt.Where,
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
	default:
		return nil, fmt.Errorf("folding not implemented for %s", be.Operator)
	}
}

// getCountNode supports the count function under special circumstances.
func (p *selectQueryPlanner) getCountNode(tableName string, rootPageNumber int) (*QueryPlan, error) {
	if len(p.stmt.ResultColumns) == 0 {
		return nil, nil
	}
	switch e := p.stmt.ResultColumns[0].Expression.(type) {
	case *compiler.FunctionExpr:
		if len(p.stmt.ResultColumns) != 1 {
			return nil, errors.New("count with other result columns not supported")
		}
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
	p.executionPlan.Append(&vm.InitCmd{P2: 1})
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
		if err := p.buildConstantNode(c); err != nil {
			return nil, err
		}
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

func (p *selectExecutionPlanner) buildScan(n *scanNode) error {
	// Build a map of constant values to registers by walking result columns and
	// the scan predicate.
	const beginningRegister = 1
	crv := &constantRegisterVisitor{}
	crv.Init(beginningRegister)
	for _, c := range n.scanColumns {
		c.BreadthWalk(crv)
	}
	if n.scanPredicate != nil {
		n.scanPredicate.BreadthWalk(crv)
	}
	rcs := crv.GetRegisterCommands()
	for _, rc := range rcs {
		p.executionPlan.Append(rc)
	}

	// Open an available cursor. Can just be 1 for now since no queries are
	// supported at the moment that requires more than one cursor.
	const cursorId = 1
	p.executionPlan.Append(&vm.OpenReadCmd{P1: cursorId, P2: n.rootPage})

	// Rewind moves the aforementioned cursor to the "start" of the table.
	rwc := &vm.RewindCmd{P1: cursorId}
	p.executionPlan.Append(rwc)

	// Mark beginning of scan for rewind
	scanBeginningCommand := len(p.executionPlan.Commands)

	// Reserve registers for the column result. Claim registers after as needed.
	startScanRegister := crv.nextOpenRegister
	endScanRegisterOffset := len(n.scanColumns)

	// This is the inside of the scan meaning how each result column is handled
	// per iteration of the scan (loop).
	var pkRegister int
	var openRegister int
	colRegisters := make(map[int]int)
	for i, c := range n.scanColumns {
		exprBuilder := &resultColumnCommandBuilder{}
		exprBuilder.Build(
			cursorId,
			startScanRegister+endScanRegisterOffset,
			crv.constantRegisters,
			startScanRegister+i,
			c,
		)
		if exprBuilder.pkRegister != 0 {
			pkRegister = exprBuilder.pkRegister
		}
		openRegister = exprBuilder.openRegister
		for crk, crv := range exprBuilder.colRegisters {
			colRegisters[crk] = crv
		}
		for _, tc := range exprBuilder.commands {
			p.executionPlan.Append(tc)
		}
	}

	// TODO predicate commands should come as early as possible to save
	// instructions, but for now this is easier.
	//
	// Walk scan predicate and build commands to calculate a conditional jump.
	if n.scanPredicate != nil {
		bpb := &booleanPredicateBuilder{}
		err := bpb.Build(
			cursorId,
			openRegister,
			len(p.executionPlan.Commands),
			crv.constantRegisters,
			colRegisters,
			pkRegister,
			n.scanPredicate,
		)
		if err != nil {
			return err
		}
		for _, bc := range bpb.commands {
			p.executionPlan.Append(bc)
		}
	}

	// Result row gathers the aforementioned inside of the scan and makes them
	// into a single row for the query results.
	p.executionPlan.Append(&vm.ResultRowCmd{P1: startScanRegister, P2: endScanRegisterOffset})

	// Falls through or goes back to the start of the scan loop.
	p.executionPlan.Append(&vm.NextCmd{P1: cursorId, P2: scanBeginningCommand})

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
func (p *selectExecutionPlanner) buildConstantNode(n *constantNode) error {
	// Build registers with constants. These are likely extra instructions, but
	// okay since it allows this to follow the same pattern a scan does.
	const beginningRegister = 1
	crv := &constantRegisterVisitor{}
	crv.Init(beginningRegister)
	for _, c := range n.resultColumns {
		c.Expression.BreadthWalk(crv)
	}
	if n.predicate != nil {
		n.predicate.BreadthWalk(crv)
	}
	rcs := crv.GetRegisterCommands()
	for _, rc := range rcs {
		p.executionPlan.Append(rc)
	}

	// Like a scan, but for a single row.
	reservedRegisterStart := crv.nextOpenRegister
	reservedRegisterOffset := len(n.resultColumns)
	var openRegister int
	for i, rc := range n.resultColumns {
		exprBuilder := &resultColumnCommandBuilder{}
		exprBuilder.Build(
			1,
			reservedRegisterStart+reservedRegisterOffset,
			crv.constantRegisters,
			reservedRegisterStart+i,
			rc.Expression,
		)
		for _, tc := range exprBuilder.commands {
			p.executionPlan.Append(tc)
		}
		openRegister = exprBuilder.openRegister
	}

	if n.predicate != nil {
		bpb := &booleanPredicateBuilder{}
		err := bpb.Build(
			0,
			openRegister,
			len(p.executionPlan.Commands),
			crv.constantRegisters,
			map[int]int{},
			0,
			n.predicate,
		)
		if err != nil {
			return err
		}
		for _, bc := range bpb.commands {
			p.executionPlan.Append(bc)
		}
	}

	p.executionPlan.Append(&vm.ResultRowCmd{P1: reservedRegisterStart, P2: reservedRegisterOffset})
	return nil
}

// resultColumnCommandBuilder builds commands for the given expression.
type resultColumnCommandBuilder struct {
	// cursorId is the cursor for the related table.
	cursorId int
	// openRegister is the next available register.
	openRegister int
	// outputRegister is the target register for the result of the expression.
	outputRegister int
	// commands are the commands to evaluate the expression.
	commands []vm.Command
	// litRegisters is a mapping of scalar values to registers containing them.
	litRegisters map[int]int
	// colRegisters is a mapping of column indexes to registers containing the
	// column. This is for subsequent routines to reuse the result of these
	// commands.
	colRegisters map[int]int
	// pkRegister is 0 value unless a register has been filled as part of Build.
	// This is for subsequent routines to reuse the result of the command.
	pkRegister int
}

func (e *resultColumnCommandBuilder) Build(
	cursorId int,
	openRegister int,
	litRegisters map[int]int,
	outputRegister int,
	root compiler.Expr,
) int {
	e.cursorId = cursorId
	e.openRegister = openRegister
	e.litRegisters = litRegisters
	e.colRegisters = make(map[int]int)
	e.outputRegister = outputRegister
	return e.build(root, 0)
}

func (e *resultColumnCommandBuilder) build(root compiler.Expr, level int) int {
	switch n := root.(type) {
	case *compiler.BinaryExpr:
		ol := e.build(n.Left, level+1)
		or := e.build(n.Right, level+1)
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
		// TODO handle OpEq
		// TODO handle OpLt
		// TODO handle OpGt
		default:
			panic("no vm command for operator")
		}
		return r
	case *compiler.ColumnRef:
		r := e.getNextRegister(level)
		if n.IsPrimaryKey {
			e.pkRegister = r
			e.commands = append(e.commands, &vm.RowIdCmd{P1: e.cursorId, P2: r})
		} else {
			e.colRegisters[n.ColIdx] = r
			e.commands = append(
				e.commands,
				&vm.ColumnCmd{P1: e.cursorId, P2: n.ColIdx, P3: r},
			)
		}
		return r
	case *compiler.IntLit:
		if level == 0 {
			e.commands = append(
				e.commands,
				&vm.CopyCmd{P1: e.litRegisters[n.Value], P2: e.outputRegister},
			)
		}
		return e.litRegisters[n.Value]
	}
	panic("unhandled expression in expr command builder")
}

func (e *resultColumnCommandBuilder) getNextRegister(level int) int {
	if level == 0 {
		return e.outputRegister
	}
	r := e.openRegister
	e.openRegister += 1
	return r
}

// booleanPredicateBuilder builds commands to calculate the boolean result of an
// expression.
type booleanPredicateBuilder struct {
	// cursorId is the cursor for the associated table.
	cursorId int
	// openRegister is the next available register
	openRegister int
	// jumpAddress is the address the result of the boolean expression should
	// conditionally jump to.
	jumpAddress int
	// commands is a list of commands representing the expression.
	commands []vm.Command
	// litRegisters is a mapping of scalar values to the register containing
	// them. litRegisters should be guaranteed since they have a minimal cost
	// due to being calculated outside of any scans/loops.
	litRegisters map[int]int
	// colRegisters is a mapping of table column index to register containing
	// the column value. colRegisters may not be guaranteed since a projection
	// may not require them, in which case colRegisters should be calculated as
	// part of the predicate.
	colRegisters map[int]int
	// pkRegister is unset when 0. Otherwise, pkRegister is the register
	// containing the table row id. pkRegister may not be guaranteed depending
	// on the projection in which case the register should be calculated as part
	// of the expression evaluation.
	pkRegister int
}

func (p *booleanPredicateBuilder) Build(
	cursorId int,
	openRegister int,
	jumpAddress int,
	litRegisters map[int]int,
	colRegisters map[int]int,
	pkRegister int,
	e compiler.Expr,
) error {
	p.cursorId = cursorId
	p.openRegister = openRegister
	p.jumpAddress = jumpAddress
	p.litRegisters = litRegisters
	p.colRegisters = colRegisters
	p.pkRegister = pkRegister
	_, err := p.build(e, 0)
	return err
}

func (p *booleanPredicateBuilder) build(e compiler.Expr, level int) (int, error) {
	switch ce := e.(type) {
	case *compiler.BinaryExpr:
		ol, err := p.build(ce.Left, level+1)
		if err != nil {
			return 0, err
		}
		or, err := p.build(ce.Right, level+1)
		if err != nil {
			return 0, err
		}
		r := p.getNextRegister()
		switch ce.Operator {
		case compiler.OpAdd:
			p.commands = append(p.commands, &vm.AddCmd{P1: ol, P2: or, P3: r})
			if level == 0 {
				p.commands = append(p.commands, &vm.IfNotCmd{P1: r, P2: p.getJumpAddress()})
			}
			return r, nil
		case compiler.OpDiv:
			p.commands = append(p.commands, &vm.DivideCmd{P1: ol, P2: or, P3: r})
			if level == 0 {
				p.commands = append(p.commands, &vm.IfNotCmd{P1: r, P2: p.getJumpAddress()})
			}
			return r, nil
		case compiler.OpMul:
			p.commands = append(p.commands, &vm.MultiplyCmd{P1: ol, P2: or, P3: r})
			if level == 0 {
				p.commands = append(p.commands, &vm.IfNotCmd{P1: r, P2: p.getJumpAddress()})
			}
			return r, nil
		case compiler.OpExp:
			p.commands = append(p.commands, &vm.ExponentCmd{P1: ol, P2: or, P3: r})
			if level == 0 {
				p.commands = append(p.commands, &vm.IfNotCmd{P1: r, P2: p.getJumpAddress()})
			}
			return r, nil
		case compiler.OpSub:
			p.commands = append(p.commands, &vm.SubtractCmd{P1: ol, P2: or, P3: r})
			if level == 0 {
				p.commands = append(p.commands, &vm.IfNotCmd{P1: r, P2: p.getJumpAddress()})
			}
			return r, nil
		case compiler.OpEq:
			if level == 0 {
				p.commands = append(
					p.commands,
					&vm.NotEqualCmd{P1: ol, P2: p.getJumpAddress(), P3: or},
				)
				return 0, nil
			}
			// TODO should be able to make this work.
			return 0, errors.New("cannot have a non root '=' expression")
		case compiler.OpLt:
			if level == 0 {
				p.commands = append(
					p.commands,
					&vm.LteCmd{P1: ol, P2: p.getJumpAddress(), P3: or},
				)
				return 0, nil
			}
			// TODO should be able to make this work.
			return 0, errors.New("cannot have a non root '<' expression")
		case compiler.OpGt:
			if level == 0 {
				p.commands = append(
					p.commands,
					&vm.GteCmd{P1: ol, P2: p.getJumpAddress(), P3: or},
				)
				return 0, nil
			}
			// TODO should be able to make this work.
			return 0, errors.New("cannot have a non root '>' expression")
		default:
			panic("no vm command for operator")
		}
	case *compiler.ColumnRef:
		colRefReg := p.valueRegisterFor(ce)
		if level == 0 {
			p.commands = append(p.commands, &vm.IfNotCmd{P1: colRefReg, P2: p.getJumpAddress()})
		}
		return colRefReg, nil
	case *compiler.IntLit:
		litReg := p.litRegisters[ce.Value]
		if level == 0 {
			p.commands = append(p.commands, &vm.IfNotCmd{P1: litReg, P2: p.getJumpAddress()})
		}
		return litReg, nil
	}
	panic("unhandled expression in predicate builder")
}

func (p *booleanPredicateBuilder) getJumpAddress() int {
	return p.jumpAddress + len(p.commands) + 2
}

func (p *booleanPredicateBuilder) valueRegisterFor(ce *compiler.ColumnRef) int {
	if ce.IsPrimaryKey {
		if p.pkRegister == 0 {
			r := p.getNextRegister()
			p.commands = append(p.commands, &vm.RowIdCmd{P1: p.cursorId, P2: r})
			return r
		}
		return p.pkRegister
	}
	cr := p.colRegisters[ce.ColIdx]
	if cr == 0 {
		r := p.getNextRegister()
		p.commands = append(p.commands, &vm.ColumnCmd{P1: p.cursorId, P2: ce.ColIdx, P3: r})
		return r
	}
	return cr
}

func (p *booleanPredicateBuilder) getNextRegister() int {
	r := p.openRegister
	p.openRegister += 1
	return r
}
