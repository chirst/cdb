package planner

import (
	"errors"
	"fmt"
	"slices"

	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/vm"
)

// updateCatalog is the required catalog methods for the update planner.
type updateCatalog interface {
	GetVersion() string
	GetRootPageNumber(string) (int, error)
	GetColumns(string) ([]string, error)
	GetPrimaryKeyColumn(string) (string, error)
}

// updatePanner houses the query planner and execution planner for a update
// statement.
type updatePlanner struct {
	queryPlanner     *updateQueryPlanner
	executionPlanner *updateExecutionPlanner
}

// updateQueryPlanner generates a queryPlan for the given update statement.
type updateQueryPlanner struct {
	catalog   updateCatalog
	stmt      *compiler.UpdateStmt
	queryPlan *updateNode
}

// updateExecutionPlanner generates a byte code routine for the given queryPlan.
type updateExecutionPlanner struct {
	queryPlan     *updateNode
	executionPlan *vm.ExecutionPlan
}

// NewUpdate create a update planner.
func NewUpdate(catalog updateCatalog, stmt *compiler.UpdateStmt) *updatePlanner {
	return &updatePlanner{
		queryPlanner: &updateQueryPlanner{
			catalog: catalog,
			stmt:    stmt,
		},
		executionPlanner: &updateExecutionPlanner{
			executionPlan: vm.NewExecutionPlan(
				catalog.GetVersion(),
				stmt.Explain,
			),
		},
	}
}

// QueryPlan sets up a high level plan to be passed to ExecutionPlan.
func (p *updatePlanner) QueryPlan() (*QueryPlan, error) {
	qp, err := p.queryPlanner.getQueryPlan()
	if err != nil {
		return nil, err
	}
	p.executionPlanner.queryPlan = p.queryPlanner.queryPlan
	return qp, err
}

// getQueryPlan returns a updateNode with a high level plan.
func (p *updateQueryPlanner) getQueryPlan() (*QueryPlan, error) {
	rootPage, err := p.catalog.GetRootPageNumber(p.stmt.TableName)
	if err != nil {
		return nil, errTableNotExist
	}
	updateNode := &updateNode{
		rootPage:    rootPage,
		recordExprs: []compiler.Expr{},
	}
	p.queryPlan = updateNode

	if err := p.errIfPrimaryKeySet(); err != nil {
		return nil, err
	}

	if err := p.errIfSetNotOnDestinationTable(); err != nil {
		return nil, err
	}

	if err := p.setQueryPlanRecordExpressions(); err != nil {
		return nil, err
	}

	if err := p.errIfSetExprNotSupported(); err != nil {
		return nil, err
	}

	if err := p.includeUpdate(); err != nil {
		return nil, err
	}

	return &QueryPlan{
		ExplainQueryPlan: p.stmt.ExplainQueryPlan,
		root:             updateNode,
	}, nil
}

// errIfPrimaryKeySet checks the primary key isn't being updated because it
// could cause an infinite loop if not handled properly.
func (p *updateQueryPlanner) errIfPrimaryKeySet() error {
	pkColumnName, err := p.catalog.GetPrimaryKeyColumn(p.stmt.TableName)
	if err != nil {
		return err
	}
	if _, ok := p.stmt.SetList[pkColumnName]; ok {
		return errors.New("updating primary key not supported")
	}
	return nil
}

// errIfSetNotOnDestinationTable checks the set list has column names that are
// part of the table being updated.
func (p *updateQueryPlanner) errIfSetNotOnDestinationTable() error {
	schemaColumns, err := p.catalog.GetColumns(p.stmt.TableName)
	if err != nil {
		return err
	}
	for colName := range p.stmt.SetList {
		if !slices.Contains(schemaColumns, colName) {
			return errSetColumnNotExist
		}
	}
	return nil
}

// setQueryPlanRecordExpressions populates the query plan with appropriate
// expressions for setting up to make a record.
func (p *updateQueryPlanner) setQueryPlanRecordExpressions() error {
	schemaColumns, err := p.catalog.GetColumns(p.stmt.TableName)
	if err != nil {
		return err
	}
	pkColName, err := p.catalog.GetPrimaryKeyColumn(p.stmt.TableName)
	if err != nil {
		return err
	}
	idx := 0
	for _, schemaColumn := range schemaColumns {
		if setListExpression, ok := p.stmt.SetList[schemaColumn]; ok {
			p.queryPlan.recordExprs = append(
				p.queryPlan.recordExprs,
				setListExpression,
			)
		} else {
			p.queryPlan.recordExprs = append(
				p.queryPlan.recordExprs,
				&compiler.ColumnRef{
					Table:        p.stmt.TableName,
					Column:       schemaColumn,
					IsPrimaryKey: pkColName == schemaColumn,
					ColIdx:       idx,
				},
			)
		}
		if schemaColumn != pkColName {
			idx += 1
		}
	}
	return nil
}

// errIfSetExprNotSupported is temporary until more expressions can be supported
// in the execution plan.
func (p *updateQueryPlanner) errIfSetExprNotSupported() error {
	for _, e := range p.queryPlan.recordExprs {
		switch e.(type) {
		case *compiler.IntLit:
			continue
		case *compiler.StringLit:
			continue
		case *compiler.ColumnRef:
			continue
		default:
			return errors.New("set list expression not supported")
		}
	}
	return nil
}

func (p *updateQueryPlanner) includeUpdate() error {
	if p.stmt.Predicate == nil {
		return nil
	}
	p.queryPlan.predicate = p.stmt.Predicate
	t, ok := p.queryPlan.predicate.(*compiler.BinaryExpr)
	supportErr := errors.New("only pk update supported in where clause")
	if !ok {
		return supportErr
	}
	l, ok := t.Left.(*compiler.ColumnRef)
	if !ok {
		return supportErr
	}
	pkColName, err := p.catalog.GetPrimaryKeyColumn(p.stmt.TableName)
	if err != nil {
		return err
	}
	if l.Column != pkColName {
		return supportErr
	}
	_, ok = t.Right.(*compiler.IntLit)
	if !ok {
		return supportErr
	}
	if t.Operator != compiler.OpEq {
		return supportErr
	}
	return nil
}

// Execution plan is a byte code routine based off a high level query plan.
func (p *updatePlanner) ExecutionPlan() (*vm.ExecutionPlan, error) {
	if p.queryPlanner.queryPlan == nil {
		_, err := p.QueryPlan()
		if err != nil {
			return nil, err
		}
	}
	return p.executionPlanner.getExecutionPlan()
}

// getExecutionPlan transforms a query plan to a byte code routine.
func (p *updateExecutionPlanner) getExecutionPlan() (*vm.ExecutionPlan, error) {
	freeRegisterCounter := 1
	// Init
	p.executionPlan.Append(&vm.InitCmd{P2: 1})
	p.executionPlan.Append(&vm.TransactionCmd{P2: 1})
	cursorId := 1
	p.executionPlan.Append(&vm.OpenWriteCmd{P1: cursorId, P2: p.queryPlan.rootPage})

	// Go to start of table
	rewindCmd := &vm.RewindCmd{P1: cursorId} // P2 deferred
	p.executionPlan.Append(rewindCmd)

	// Loop
	loopStartAddress := len(p.executionPlan.Commands)

	// If needed, include jump for if.
	var notEqCmd *vm.NotEqualCmd
	if p.queryPlan.predicate != nil {
		p.executionPlan.Append(&vm.RowIdCmd{P1: cursorId, P2: freeRegisterCounter})
		freeRegisterCounter += 1
		// No ok checks because done in logical plan.
		pe := p.queryPlan.predicate.(*compiler.BinaryExpr)
		r := pe.Right.(*compiler.IntLit)
		p.executionPlan.Append(&vm.IntegerCmd{P1: r.Value, P2: freeRegisterCounter})
		freeRegisterCounter += 1
		notEqCmd = &vm.NotEqualCmd{
			P1: freeRegisterCounter - 2,
			P2: -1, // deferred
			P3: freeRegisterCounter - 1,
		}
		p.executionPlan.Append(notEqCmd)
	}

	// take each item in the set list and build to make a record
	loopStartRegister := freeRegisterCounter
	var pkRegister int
	for _, expression := range p.queryPlan.recordExprs {
		switch typedExpression := expression.(type) {
		case *compiler.ColumnRef:
			if typedExpression.IsPrimaryKey {
				p.executionPlan.Append(&vm.RowIdCmd{
					P1: cursorId,
					P2: freeRegisterCounter,
				})
				pkRegister = freeRegisterCounter
			} else {
				p.executionPlan.Append(&vm.ColumnCmd{
					P1: cursorId,
					P2: typedExpression.ColIdx,
					P3: freeRegisterCounter,
				})
			}
		case *compiler.IntLit:
			p.executionPlan.Append(&vm.IntegerCmd{
				P1: typedExpression.Value,
				P2: freeRegisterCounter,
			})
		case *compiler.StringLit:
			p.executionPlan.Append(&vm.StringCmd{
				P1: freeRegisterCounter,
				P4: typedExpression.Value,
			})
		default:
			return nil, errors.New("expression not supported")
		}
		freeRegisterCounter += 1
	}
	p.executionPlan.Append(&vm.MakeRecordCmd{
		P1: loopStartRegister + 1,            // plus 1 for the pk
		P2: len(p.queryPlan.recordExprs) - 1, // minus 1 for the pk
		P3: freeRegisterCounter,
	})
	p.executionPlan.Append(&vm.DeleteCmd{P1: cursorId})
	p.executionPlan.Append(&vm.InsertCmd{
		P1: cursorId,
		P2: freeRegisterCounter,
		P3: pkRegister,
	})
	p.executionPlan.Append(&vm.NextCmd{P1: cursorId, P2: loopStartAddress})
	if notEqCmd != nil {
		notEqCmd.P2 = len(p.executionPlan.Commands) - 1
	}

	// End
	p.executionPlan.Append(&vm.HaltCmd{})
	rewindCmd.P2 = len(p.executionPlan.Commands) - 1
	return p.executionPlan, nil
}

// CREATE TABLE table (? INTEGER PRIMARY KEY, ? INTEGER, ? TEXT)
//
// - create

// SELECT * FROM table
// WHERE ? = ?
//
// - project
//   - filter
//     - scan

// SELECT constant
// WHERE ? = ?
//
// - project
//	 - filter
// 	   - constant

// SELECT COUNT(*) FROM table
//
// - count (similar to scan and breaks rule and no project)

// INSERT INTO table (?, ?) VALUES (?, ?)
//
// - insert
//   - constant?

// UPDATE TABLE table
// SET ? = ?
// WHERE ? = ?
//
// - update
//   - filter
//     - scan

func generateUpdate() {
	logicalPlan := &planV2{
		commands:        []vm.Command{},
		constInts:       make(map[int]int),
		constStrings:    make(map[string]int),
		freeRegister:    1,
		transactionType: 2,
		cursorId:        1,
	}
	un := &updateNodeV2{
		plan: logicalPlan,
	}
	fn := &filterNodeV2{
		plan: logicalPlan,
	}
	fn.parent = un
	un.child = fn
	sn := &scanNodeV2{
		plan: logicalPlan,
	}
	sn.parent = fn
	fn.child = sn
	logicalPlan.root = un
	logicalPlan.compile()
	for i := range logicalPlan.commands {
		fmt.Printf("%d %#v\n", i+1, logicalPlan.commands[i])
	}
}

type planV2 struct {
	root            nodeV2
	commands        []vm.Command
	constInts       map[int]int    // int to register
	constStrings    map[string]int // string to register
	freeRegister    int
	transactionType int // 0 none, 1 read, 2 write
	cursorId        int
}

// declareConstInt gets or sets a register with the const value and returns the
// register. It is guaranteed the value will be in the register for the duration
// of the plan.
func (p *planV2) declareConstInt(i int) int {
	_, ok := p.constInts[i]
	if !ok {
		p.constInts[i] = p.freeRegister
		p.freeRegister += 1
	}
	return p.constInts[i]
}

// declareConstString gets or sets a register with the const value and returns
// the register. It is guaranteed the value will be in the register for the
// duration of the plan.
func (p *planV2) declareConstString(s string) int {
	_, ok := p.constStrings[s]
	if !ok {
		p.constStrings[s] = p.freeRegister
		p.freeRegister += 1
	}
	return p.constStrings[s]
}

func (p *planV2) compile() {
	initCmd := &vm.InitCmd{}
	p.commands = append(p.commands, initCmd)
	p.root.produce()
	p.commands = append(p.commands, &vm.HaltCmd{})
	initCmd.P2 = len(p.commands) + 1
	p.pushTransaction()
	p.pushConstants()
	p.commands = append(p.commands, &vm.GotoCmd{P1: 2})
}

func (p *planV2) pushTransaction() {
	switch p.transactionType {
	case 0:
		return
	case 1:
		p.commands = append(p.commands, &vm.TransactionCmd{P2: 1})
	case 2:
		p.commands = append(p.commands, &vm.TransactionCmd{P2: 2})
	default:
		panic("unexpected transaction type")
	}
}

func (p *planV2) pushConstants() {
	for v := range p.constInts {
		p.commands = append(p.commands, &vm.IntegerCmd{P1: v, P2: p.constInts[v]})
	}
	for v := range p.constStrings {
		p.commands = append(p.commands, &vm.StringCmd{P1: p.constStrings[v], P4: v})
	}
}

type nodeV2 interface {
	produce()
	consume()
}

type updateNodeV2 struct {
	child nodeV2
	plan  *planV2
}

func (u *updateNodeV2) produce() {
	u.child.produce()
}

func (u *updateNodeV2) consume() {
	startRecordRegister := u.plan.freeRegister
	u.plan.commands = append(u.plan.commands, &vm.RowIdCmd{
		P1: u.plan.cursorId,
		P2: u.plan.freeRegister,
	})
	rowIdRegister := u.plan.freeRegister
	u.plan.freeRegister += 1
	u.plan.commands = append(u.plan.commands, &vm.CopyCmd{
		P1: u.plan.declareConstInt(1),
		P2: u.plan.freeRegister,
	})
	u.plan.freeRegister += 1
	u.plan.commands = append(u.plan.commands, &vm.ColumnCmd{
		P1: u.plan.cursorId,
		P2: 0,
		P3: u.plan.freeRegister,
	})
	endRecordRegister := u.plan.freeRegister
	u.plan.freeRegister += 1
	u.plan.commands = append(u.plan.commands, &vm.MakeRecordCmd{
		P1: startRecordRegister,
		P2: endRecordRegister,
		P3: u.plan.freeRegister,
	})
	recordRegister := u.plan.freeRegister
	u.plan.freeRegister += 1
	u.plan.commands = append(u.plan.commands, &vm.DeleteCmd{
		P1: u.plan.cursorId,
	})
	u.plan.commands = append(u.plan.commands, &vm.InsertCmd{
		P1: u.plan.cursorId,
		P2: recordRegister,
		P3: rowIdRegister,
	})
}

type filterNodeV2 struct {
	child  nodeV2
	parent nodeV2
	plan   *planV2
}

func (f *filterNodeV2) produce() {
	f.child.produce()
}

func (f *filterNodeV2) consume() {
	f.plan.commands = append(f.plan.commands, &vm.ColumnCmd{
		P1: f.plan.cursorId,
		P2: 1,
		P3: f.plan.freeRegister,
	})
	notEqualCmd := &vm.NotEqualCmd{
		P1: f.plan.freeRegister,
		P3: f.plan.declareConstInt(1),
	}
	f.plan.commands = append(f.plan.commands, notEqualCmd)
	f.parent.consume()
	notEqualCmd.P2 = len(f.plan.commands) + 1
}

type scanNodeV2 struct {
	parent nodeV2
	plan   *planV2
}

func (s *scanNodeV2) produce() {
	s.consume()
}

func (s *scanNodeV2) consume() {
	rewindCmd := &vm.RewindCmd{P1: s.plan.cursorId}
	s.plan.commands = append(s.plan.commands, rewindCmd)
	loopBeginAddress := len(s.plan.commands) + 1
	s.parent.consume()
	s.plan.commands = append(s.plan.commands, &vm.NextCmd{
		P1: s.plan.cursorId,
		P2: loopBeginAddress,
	})
	rewindCmd.P2 = len(s.plan.commands) + 1
}
