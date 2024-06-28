// vm (virtual machine) is capable of running routines made up of commands that
// access the kv layer. The commands are formed by the planner from the ast
// (abstract syntax tree).
package vm

import (
	"fmt"
	"strconv"

	"github.com/chirst/cdb/kv"
)

type vm struct {
	kv *kv.KV
}

func New(kv *kv.KV) *vm {
	return &vm{
		kv: kv,
	}
}

// routine contains values that are destroyed when a plan is finished executing
type routine struct {
	registers        map[int]any
	resultRows       *[][]*string
	cursors          map[int]*kv.Cursor
	readTransaction  bool
	writeTransaction bool
}

type Command interface {
	execute(vm *vm, routine *routine) cmdRes
	explain(addr int) []*string
}

type cmdRes struct {
	doHalt      bool
	nextAddress int
	err         error
}

type cmd struct {
	P1 int
	P2 int
	P3 int
	P4 string
	P5 int
}

type ExecuteResult struct {
	Err        error
	Text       string
	ResultRows [][]*string
}

type ExecutionPlan struct {
	Explain      bool
	Commands     []Command
	ResultHeader []*string
}

func (v *vm) Execute(plan *ExecutionPlan) *ExecuteResult {
	if plan.Explain {
		return &ExecuteResult{
			ResultRows: v.explain(plan),
		}
	}
	routine := &routine{
		registers:        map[int]any{},
		resultRows:       &[][]*string{},
		cursors:          map[int]*kv.Cursor{},
		readTransaction:  false,
		writeTransaction: false,
	}
	if plan.ResultHeader != nil {
		*routine.resultRows = append(*routine.resultRows, plan.ResultHeader)
	}
	i := 0
	var currentCommand Command
	for i < len(plan.Commands) {
		currentCommand = plan.Commands[i]
		res := currentCommand.execute(v, routine)
		if res.err != nil {
			return &ExecuteResult{Err: res.err}
		}
		if res.doHalt {
			break
		}
		if res.nextAddress == 0 {
			i = i + 1
		} else {
			i = res.nextAddress
		}
	}
	return &ExecuteResult{
		ResultRows: *routine.resultRows,
	}
}

func formatExplain(addr int, c string, P1, P2, P3 int, P4 string, P5 int, comment string) []*string {
	aa := strconv.Itoa(addr)
	p1a := strconv.Itoa(P1)
	p2a := strconv.Itoa(P2)
	p3a := strconv.Itoa(P3)
	p5a := strconv.Itoa(P5)
	return []*string{
		&aa,
		&c,
		&p1a,
		&p2a,
		&p3a,
		&P4,
		&p5a,
		&comment,
	}
}

func (*vm) makeStr(s string) *string {
	return &s
}

func (v *vm) explain(plan *ExecutionPlan) [][]*string {
	resultRows := [][]*string{
		{
			v.makeStr("addr"),
			v.makeStr("opcode"),
			v.makeStr("P1"),
			v.makeStr("P2"),
			v.makeStr("P3"),
			v.makeStr("P4"),
			v.makeStr("P5"),
			v.makeStr("comment"),
		},
	}
	i := 0
	var currentCommand Command
	for i < len(plan.Commands) {
		currentCommand = plan.Commands[i]
		resultRows = append(resultRows, currentCommand.explain(i))
		i = i + 1
	}
	return resultRows
}

// InitCmd jumps to the instruction at address P2.
type InitCmd cmd

func (c *InitCmd) execute(vm *vm, routine *routine) cmdRes {
	return cmdRes{
		nextAddress: c.P2,
	}
}

func (c *InitCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Start at addr[%d]", c.P2)
	return formatExplain(addr, "Init", c.P1, c.P2, c.P3, c.P4, c.P5, comment)
}

// HaltCmd ends the routine which closes all cursors and commits transactions.
type HaltCmd cmd

func (c *HaltCmd) execute(vm *vm, routine *routine) cmdRes {
	if routine.readTransaction {
		vm.kv.EndReadTransaction()
	}
	if routine.writeTransaction {
		err := vm.kv.EndWriteTransaction()
		return cmdRes{
			doHalt: true,
			err:    err,
		}
	}
	return cmdRes{
		doHalt: true,
	}
}

func (c *HaltCmd) explain(addr int) []*string {
	comment := "End read transaction and exit"
	if c.P2 == 1 {
		comment = "End write transaction and exit"
	}
	return formatExplain(addr, "Halt", c.P1, c.P2, c.P3, c.P4, c.P5, comment)
}

// TransactionCmd starts a read transaction if P2 is 0. If P2 is 1
// TransactionCmd starts a write transaction.
type TransactionCmd cmd

func (c *TransactionCmd) execute(vm *vm, routine *routine) cmdRes {
	if c.P2 == 0 {
		routine.readTransaction = true
		vm.kv.BeginReadTransaction()
		return cmdRes{}
	}
	if c.P2 == 1 {
		routine.writeTransaction = true
		vm.kv.BeginWriteTransaction()
		return cmdRes{}
	}
	return cmdRes{
		err: fmt.Errorf("unhandled transactionCmd with P2: %d", c.P2),
	}
}

func (c *TransactionCmd) explain(addr int) []*string {
	comment := "Begin a read transaction"
	if c.P2 == 1 {
		comment = "Begin a write transaction"
	}
	return formatExplain(addr, "Transaction", c.P1, c.P2, c.P3, c.P4, c.P5, comment)
}

// OpenReadCmd opens a read cursor with identifier P1 at page P2
type OpenReadCmd cmd

func (c *OpenReadCmd) execute(vm *vm, routine *routine) cmdRes {
	routine.cursors[c.P1] = vm.kv.NewCursor(c.P2)
	return cmdRes{}
}

func (c *OpenReadCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Open read cursor with id %d at root page %d", c.P1, c.P2)
	return formatExplain(addr, "OpenRead", c.P1, c.P2, c.P3, c.P4, c.P5, comment)
}

// RewindCmd goes to the first entry in the table for cursor P1. If the table is
// empty it jumps to P2.
type RewindCmd cmd

func (c *RewindCmd) execute(vm *vm, routine *routine) cmdRes {
	hasValues := routine.cursors[c.P1].GotoFirstRecord()
	if !hasValues {
		return cmdRes{
			nextAddress: c.P2,
		}
	}
	return cmdRes{}
}

func (c *RewindCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Move cursor %d to the start of the table. If the table is empty jump to addr[%d]", c.P1, c.P2)
	return formatExplain(addr, "Rewind", c.P1, c.P2, c.P3, c.P4, c.P5, comment)
}

// rowId store in register P2 an integer which is the key of the entry the
// cursor P1 is on
type RowIdCmd cmd

func (c *RowIdCmd) execute(vm *vm, routine *routine) cmdRes {
	ek := routine.cursors[c.P1].GetKey()
	dk, err := kv.DecodeKey(ek)
	if err != nil {
		return cmdRes{
			err: err,
		}
	}
	routine.registers[c.P2] = dk
	return cmdRes{}
}

func (c *RowIdCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Store id cursor %d is currently pointing to in register[%d]", c.P1, c.P2)
	return formatExplain(addr, "RowId", c.P1, c.P2, c.P3, c.P4, c.P5, comment)
}

// ColumnCmd stores in register P3 the value pointed to for the P2-th column for
// the P1 cursor.
type ColumnCmd cmd

func (c *ColumnCmd) execute(vm *vm, routine *routine) cmdRes {
	v := routine.cursors[c.P1].GetValue()
	cols, err := kv.Decode(v)
	if err != nil {
		return cmdRes{err: err}
	}
	routine.registers[c.P3] = cols[c.P2]
	return cmdRes{}
}

func (c *ColumnCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Store the value for the %d-th column in register[%d] for cursor %d", c.P2, c.P3, c.P1)
	return formatExplain(addr, "Column", c.P1, c.P2, c.P3, c.P4, c.P5, comment)
}

// ResultRowCmd stores P1 through P1+P2-1 as a single row of results
type ResultRowCmd cmd

func (c *ResultRowCmd) execute(vm *vm, routine *routine) cmdRes {
	row := []*string{}
	for i := c.P1; i < c.P1+c.P2; i += 1 {
		switch v := routine.registers[i].(type) {
		case int:
			vs := strconv.Itoa(v)
			row = append(row, &vs)
		case string:
			row = append(row, &v)
		case nil:
			row = append(row, nil)
		default:
			return cmdRes{err: fmt.Errorf("unhandled result row %#v", v)}
		}
	}
	*routine.resultRows = append(*routine.resultRows, row)
	return cmdRes{}
}

func (c *ResultRowCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Make a row from registers[%d..%d]", c.P1, c.P1+c.P2-1)
	return formatExplain(addr, "ResultRow", c.P1, c.P2, c.P3, c.P4, c.P5, comment)
}

// NextCmd advances the cursor P1. If the cursor has reached the end fall
// through. If there is more for the cursor to process jump to P2.
type NextCmd cmd

func (c *NextCmd) execute(vm *vm, routine *routine) cmdRes {
	if routine.cursors[c.P1].GotoNext() {
		return cmdRes{
			nextAddress: c.P2,
		}
	}
	return cmdRes{}
}

func (c *NextCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Advance cursor %d if there are items jump to addr[%d] else fall through", c.P1, c.P2)
	return formatExplain(addr, "Next", c.P1, c.P2, c.P3, c.P4, c.P5, comment)
}

// MakeRecordCmd makes a byte array record for registers P1 through P1+P2-1 and
// stores the record in register P3.
type MakeRecordCmd cmd

func (c *MakeRecordCmd) execute(vm *vm, routine *routine) cmdRes {
	span := []any{}
	for i := c.P1; i < c.P1+c.P2; i += 1 {
		span = append(span, routine.registers[i])
	}
	v, err := kv.Encode(span)
	if err != nil {
		return cmdRes{err: err}
	}
	routine.registers[c.P3] = v
	return cmdRes{}
}

func (c *MakeRecordCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Convert registers[%d..%d] to bytes and store in register[%d]", c.P1, c.P1+c.P2-1, c.P3)
	return formatExplain(addr, "MakeRecord", c.P1, c.P2, c.P3, c.P4, c.P5, comment)
}

// CreateBTreeCmd creates a new btree and stores the root page number in P2
type CreateBTreeCmd cmd

func (c *CreateBTreeCmd) execute(vm *vm, routine *routine) cmdRes {
	rootPageNumber := vm.kv.NewBTree()
	routine.registers[c.P2] = rootPageNumber
	return cmdRes{}
}

func (c *CreateBTreeCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Create new btree and store root page number in register[%d]", c.P2)
	return formatExplain(addr, "CreateBTree", c.P1, c.P2, c.P3, c.P4, c.P5, comment)
}

// OpenWriteCmd opens a write cursor named P1 on table with root page P2
type OpenWriteCmd cmd

func (c *OpenWriteCmd) execute(vm *vm, routine *routine) cmdRes {
	return cmdRes{}
}

func (c *OpenWriteCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Open write cursor named %d on table with root page %d", c.P1, c.P2)
	return formatExplain(addr, "OpenWrite", c.P1, c.P2, c.P3, c.P4, c.P5, comment)
}

// NewRowIdCmd generates a new row id for table root page P1 and writes the new
// id to register P2
type NewRowIdCmd cmd

func (c *NewRowIdCmd) execute(vm *vm, routine *routine) cmdRes {
	rid, err := vm.kv.NewRowID(c.P1)
	if err != nil {
		return cmdRes{err: err}
	}
	routine.registers[c.P2] = rid
	return cmdRes{}
}

func (c *NewRowIdCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Generate row id for table %d and store in register[%d]", c.P1, c.P2)
	return formatExplain(addr, "NewRowID", c.P1, c.P2, c.P3, c.P4, c.P5, comment)
}

// InsertCmd write to cursor P1 with data in P2 and key in P3
type InsertCmd cmd

func (c *InsertCmd) execute(vm *vm, routine *routine) cmdRes {
	bp3i, ok := routine.registers[c.P3].(int)
	if !ok {
		return cmdRes{
			err: fmt.Errorf("failed to convert %v to int", bp3i),
		}
	}
	bp3, err := kv.EncodeKey(bp3i)
	if err != nil {
		return cmdRes{
			err: err,
		}
	}
	bp2, ok := routine.registers[c.P2].([]byte)
	if !ok {
		return cmdRes{
			err: fmt.Errorf("failed to convert %v to byte slice", bp2),
		}
	}
	vm.kv.Set(c.P1, bp3, bp2)
	return cmdRes{}
}

func (c *InsertCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Insert cursor %d with value in register[%d] and key register[%d]", c.P1, c.P2, c.P3)
	return formatExplain(addr, "Insert", c.P1, c.P2, c.P3, c.P4, c.P5, comment)
}

// ParseSchemaCmd refreshes the catalog
type ParseSchemaCmd cmd

func (c *ParseSchemaCmd) execute(vm *vm, routine *routine) cmdRes {
	err := vm.kv.ParseSchema()
	return cmdRes{
		err: err,
	}
}

func (c *ParseSchemaCmd) explain(addr int) []*string {
	comment := "Refresh catalog"
	return formatExplain(addr, "ParseSchema", c.P1, c.P2, c.P3, c.P4, c.P5, comment)
}

// StringCmd stores string P4 in register P1
type StringCmd cmd

func (c *StringCmd) execute(vm *vm, routine *routine) cmdRes {
	routine.registers[c.P1] = c.P4
	return cmdRes{}
}

func (c *StringCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Store string \"%s\" in register[%d]", c.P4, c.P1)
	return formatExplain(addr, "String", c.P1, c.P2, c.P3, c.P4, c.P5, comment)
}

// CopyCmd copies P1 into P2
type CopyCmd cmd

func (c *CopyCmd) execute(vm *vm, routine *routine) cmdRes {
	routine.registers[c.P2] = routine.registers[c.P1]
	return cmdRes{}
}

func (c *CopyCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Copy register[%d] into register[%d]", c.P1, c.P2)
	return formatExplain(addr, "Copy", c.P1, c.P2, c.P3, c.P4, c.P5, comment)
}
