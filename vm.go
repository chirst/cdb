// vm (Virtual Machine) is capable of running routines made up of commands that
// access the kv layer. The commands are formed from the ast (Abstract Syntax
// Tree).
package main

import (
	"fmt"
	"strconv"
)

type vm struct {
	kv      *kv
	cursors map[int]*cursor
}

func newVm(kv *kv) *vm {
	return &vm{
		kv:      kv,
		cursors: make(map[int]*cursor),
	}
}

type command interface {
	execute(registers map[int]any, resultRows *[][]*string, vm *vm) cmdRes
	explain(addr int) []*string
}

type cmdRes struct {
	doHalt      bool
	nextAddress int
	err         error
}

type cmd struct {
	p1 int
	p2 int
	p3 int
	p4 string
	p5 int
}

type executeResult struct {
	err        error
	text       string
	resultRows [][]*string
}

type executionPlan struct {
	explain      bool
	commands     []command
	resultHeader []*string
}

func (v *vm) execute(plan *executionPlan) *executeResult {
	if plan.explain {
		return &executeResult{
			resultRows: v.explain(plan),
		}
	}
	registers := map[int]any{}
	resultRows := &[][]*string{}
	if plan.resultHeader != nil {
		*resultRows = append(*resultRows, plan.resultHeader)
	}
	i := 0
	var currentCommand command
	for {
		if len(plan.commands) < i {
			break
		}
		currentCommand = plan.commands[i]
		res := currentCommand.execute(registers, resultRows, v)
		if res.err != nil {
			return &executeResult{err: res.err}
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
	return &executeResult{
		resultRows: *resultRows,
	}
}

func formatExplain(addr int, c string, p1, p2, p3 int, p4 string, p5 int, comment string) []*string {
	aa := strconv.Itoa(addr)
	p1a := strconv.Itoa(p1)
	p2a := strconv.Itoa(p2)
	p3a := strconv.Itoa(p3)
	p5a := strconv.Itoa(p5)
	return []*string{
		&aa,
		&c,
		&p1a,
		&p2a,
		&p3a,
		&p4,
		&p5a,
		&comment,
	}
}

func (*vm) makeStr(s string) *string {
	return &s
}

func (v *vm) explain(plan *executionPlan) [][]*string {
	resultRows := [][]*string{
		{
			v.makeStr("addr"),
			v.makeStr("opcode"),
			v.makeStr("p1"),
			v.makeStr("p2"),
			v.makeStr("p3"),
			v.makeStr("p4"),
			v.makeStr("p5"),
			v.makeStr("comment"),
		},
	}
	i := 0
	var currentCommand command
	for {
		if len(plan.commands) < i {
			break
		}
		currentCommand = plan.commands[i]
		resultRows = append(resultRows, currentCommand.explain(i))
		i = i + 1
	}
	return resultRows
}

// initCmd jumps to the instruction at address p2.
type initCmd cmd

func (c *initCmd) execute(registers map[int]any, resultRows *[][]*string, vm *vm) cmdRes {
	return cmdRes{
		nextAddress: c.p2,
	}
}

func (c *initCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Start at addr[%d]", c.p2)
	return formatExplain(addr, "Init", c.p1, c.p2, c.p3, c.p4, c.p5, comment)
}

// haltCmd ends the routine if p2 is 0 a read transaction is ended if p2 is 1 a
// write transaction is ended.
type haltCmd cmd

func (c *haltCmd) execute(registers map[int]any, resultRows *[][]*string, vm *vm) cmdRes {
	if c.p2 == 0 {
		vm.kv.EndReadTransaction()
	}
	if c.p2 == 1 {
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

func (c *haltCmd) explain(addr int) []*string {
	return formatExplain(addr, "Halt", c.p1, c.p2, c.p3, c.p4, c.p5, "Exit")
}

// transactionCmd if p2 is 0 starts a read transaction if p2 is 1 starts a write
// transaction.
type transactionCmd cmd

func (c *transactionCmd) execute(registers map[int]any, resultRows *[][]*string, vm *vm) cmdRes {
	if c.p2 == 0 {
		vm.kv.pager.beginRead()
		return cmdRes{}
	}
	if c.p2 == 1 {
		vm.kv.pager.beginWrite()
		return cmdRes{}
	}
	return cmdRes{
		err: fmt.Errorf("unhandled transactionCmd with p2: %d", c.p2),
	}
}

func (c *transactionCmd) explain(addr int) []*string {
	comment := "Begin a read transaction"
	if c.p2 == 1 {
		comment = "Begin a write transaction"
	}
	return formatExplain(addr, "Transaction", c.p1, c.p2, c.p3, c.p4, c.p5, comment)
}

// openReadCmd opens a read cursor at page p2 with the identifier p1
type openReadCmd cmd

func (c *openReadCmd) execute(registers map[int]any, resultRows *[][]*string, vm *vm) cmdRes {
	vm.cursors[c.p1] = vm.kv.NewCursor(c.p2)
	return cmdRes{}
}

func (c *openReadCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Open read cursor with id %d at root page %d", c.p1, c.p2)
	return formatExplain(addr, "OpenRead", c.p1, c.p2, c.p3, c.p4, c.p5, comment)
}

// rewindCmd goes to the first entry in the table for cursor p1. If the table is
// empty it jumps to p2.
type rewindCmd cmd

func (c *rewindCmd) execute(registers map[int]any, resultRows *[][]*string, vm *vm) cmdRes {
	hasValues := vm.cursors[c.p1].GotoFirstRecord()
	if !hasValues {
		return cmdRes{
			nextAddress: c.p2,
		}
	}
	return cmdRes{}
}

func (c *rewindCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Move cursor %d to the start of the table. If the table is empty jump to addr[%d]", c.p1, c.p2)
	return formatExplain(addr, "Rewind", c.p1, c.p2, c.p3, c.p4, c.p5, comment)
}

// rowId store in register p2 an integer which is the key of the entry the
// cursor p1 is on
type rowIdCmd cmd

func (c *rowIdCmd) execute(registers map[int]any, resultRows *[][]*string, vm *vm) cmdRes {
	ek := vm.cursors[c.p1].GetKey()
	dk := DecodeKey(ek)
	registers[c.p2] = dk
	return cmdRes{}
}

func (c *rowIdCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Store id cursor %d is currently pointing to in register[%d]", c.p1, c.p2)
	return formatExplain(addr, "RowId", c.p1, c.p2, c.p3, c.p4, c.p5, comment)
}

// columnCmd stores in register p3 the value pointed to for the p2-th column for the p1 cursor.
type columnCmd cmd

func (c *columnCmd) execute(registers map[int]any, resultRows *[][]*string, vm *vm) cmdRes {
	v := vm.cursors[c.p1].GetValue()
	cols, err := Decode(v)
	if err != nil {
		return cmdRes{err: err}
	}
	registers[c.p3] = cols[c.p2]
	return cmdRes{}
}

func (c *columnCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Store the value for the %d-th column in register[%d] for cursor %d", c.p2, c.p3, c.p1)
	return formatExplain(addr, "Column", c.p1, c.p2, c.p3, c.p4, c.p5, comment)
}

// resultRowCmd stores p1 through p1+p2-1 as a single row of results
type resultRowCmd cmd

func (c *resultRowCmd) execute(registers map[int]any, resultRows *[][]*string, vm *vm) cmdRes {
	row := []*string{}
	i := c.p1
	end := c.p1 + c.p2 - 1
	for i <= end {
		switch v := registers[i].(type) {
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
		i = i + 1
	}
	*resultRows = append(*resultRows, row)
	return cmdRes{}
}

func (c *resultRowCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Make a row from registers[%d..%d]", c.p1, c.p2)
	return formatExplain(addr, "ResultRow", c.p1, c.p2, c.p3, c.p4, c.p5, comment)
}

// nextCmd advances the cursor p1. If the cursor has reached the end fall
// through. If there is more for the cursor to process jump to p2.
type nextCmd cmd

func (c *nextCmd) execute(registers map[int]any, resultRows *[][]*string, vm *vm) cmdRes {
	if vm.cursors[c.p1].GotoNext() {
		return cmdRes{
			nextAddress: c.p2,
		}
	}
	return cmdRes{}
}

func (c *nextCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Advance cursor %d. If there are items jump to addr[%d]", c.p1, c.p2)
	return formatExplain(addr, "Next", c.p1, c.p2, c.p3, c.p4, c.p5, comment)
}

// makeRecordCmd makes a byte array record for registers p1 through p2 and
// stores the record in register p3.
type makeRecordCmd cmd

func (c *makeRecordCmd) execute(registers map[int]any, resultRows *[][]*string, vm *vm) cmdRes {
	span := []any{}
	for i := c.p1; i <= c.p1+c.p2; i += 1 {
		span = append(span, registers[i])
	}
	v, err := Encode(span)
	if err != nil {
		return cmdRes{err: err}
	}
	registers[c.p3] = v
	return cmdRes{}
}

func (c *makeRecordCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Convert register [%d..%d] to bytes and store in register[%d]", c.p1, c.p2, c.p3)
	return formatExplain(addr, "MakeRecord", c.p1, c.p2, c.p3, c.p4, c.p5, comment)
}

// createBTreeCmd creates a new btree and stores the root page number in p2
type createBTreeCmd cmd

func (c *createBTreeCmd) execute(registers map[int]any, resultRows *[][]*string, vm *vm) cmdRes {
	rootPageNumber := vm.kv.NewBTree()
	registers[c.p2] = rootPageNumber
	return cmdRes{}
}

func (c *createBTreeCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Create new btree and store root page number in register[%d]", c.p2)
	return formatExplain(addr, "CreateBTree", c.p1, c.p2, c.p3, c.p4, c.p5, comment)
}

// openWriteCmd opens a write cursor named p1 on table with root page p2
type openWriteCmd cmd

func (c *openWriteCmd) execute(registers map[int]any, resultRows *[][]*string, vm *vm) cmdRes {
	return cmdRes{}
}

func (c *openWriteCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Open write cursor named %d on table with root page %d", c.p1, c.p2)
	return formatExplain(addr, "OpenWrite", c.p1, c.p2, c.p3, c.p4, c.p5, comment)
}

// newRowIdCmd generate a new row id for table root page p1 and write to p2
type newRowIdCmd cmd

func (c *newRowIdCmd) execute(registers map[int]any, resultRows *[][]*string, vm *vm) cmdRes {
	rid, err := vm.kv.NewRowID(c.p1)
	if err != nil {
		return cmdRes{err: err}
	}
	registers[c.p2] = rid
	return cmdRes{}
}

func (c *newRowIdCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Generate row id for table %d and store in register[%d]", c.p1, c.p2)
	return formatExplain(addr, "NewRowID", c.p1, c.p2, c.p3, c.p4, c.p5, comment)
}

// insertCmd write to cursor p1 with data in p2 and key in p3
type insertCmd cmd

func (c *insertCmd) execute(registers map[int]any, resultRows *[][]*string, vm *vm) cmdRes {
	bp3i, ok := registers[c.p3].(int)
	if !ok {
		return cmdRes{
			err: fmt.Errorf("failed to convert %v to int", bp3i),
		}
	}
	bp3 := EncodeKey(uint16(bp3i))
	bp2, ok := registers[c.p2].([]byte)
	if !ok {
		return cmdRes{
			err: fmt.Errorf("failed to convert %v to byte slice", bp2),
		}
	}
	vm.kv.Set(uint16(c.p1), bp3, bp2)
	return cmdRes{}
}

func (c *insertCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Insert cursor %d with value in register[%d] and key register[%d]", c.p1, c.p2, c.p3)
	return formatExplain(addr, "Insert", c.p1, c.p2, c.p3, c.p4, c.p5, comment)
}

// parseSchemaCmd refresh in memory schema
type parseSchemaCmd cmd

func (c *parseSchemaCmd) execute(registers map[int]any, resultRows *[][]*string, vm *vm) cmdRes {
	err := vm.kv.ParseSchema()
	return cmdRes{
		err: err,
	}
}

func (c *parseSchemaCmd) explain(addr int) []*string {
	comment := "Refresh in memory schema"
	return formatExplain(addr, "ParseSchema", c.p1, c.p2, c.p3, c.p4, c.p5, comment)
}

// stringCmd stores string in p4 in register p1
type stringCmd cmd

func (c *stringCmd) execute(registers map[int]any, resultRows *[][]*string, vm *vm) cmdRes {
	registers[c.p1] = c.p4
	return cmdRes{}
}

func (c *stringCmd) explain(addr int) []*string {
	comment := "Store string in p4 in p1"
	return formatExplain(addr, "String", c.p1, c.p2, c.p3, c.p4, c.p5, comment)
}

// copyCmd copies p1 into p2
type copyCmd cmd

func (c *copyCmd) execute(registers map[int]any, resultRows *[][]*string, vm *vm) cmdRes {
	registers[c.p2] = registers[c.p1]
	return cmdRes{}
}

func (c *copyCmd) explain(addr int) []*string {
	comment := "Copy p1 into p2"
	return formatExplain(addr, "Copy", c.p1, c.p2, c.p3, c.p4, c.p5, comment)
}
