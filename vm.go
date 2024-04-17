// vm (Virtual Machine) is capable of running routines made up of commands that
// access the kv layer. The commands are formed from the ast (Abstract Syntax
// Tree).
package main

import (
	"fmt"
	"strconv"
)

type vm struct{}

func newVm() *vm {
	return &vm{}
}

type command interface {
	execute(registers map[int]any, resultRows *[][]*string) cmdRes
	explain(addr int) []*string
}

type cmdRes struct {
	doHalt      bool
	nextAddress int
}

type cmd struct {
	p1 int
	p2 int
	p3 int
	p4 int
	p5 int
}

type executeResult struct {
	err        error
	text       string
	resultRows [][]*string
}

type executionPlan struct {
	explain  bool
	commands map[int]command
}

func (v *vm) execute(plan *executionPlan) *executeResult {
	if plan.explain {
		return &executeResult{
			resultRows: v.explain(plan),
		}
	}
	registers := map[int]any{}
	resultRows := &[][]*string{}
	i := 1
	var currentCommand command
	for {
		if len(plan.commands) < i {
			break
		}
		currentCommand = plan.commands[i]
		res := currentCommand.execute(registers, resultRows)
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

func formatExplain(addr int, c string, p1, p2, p3, p4, p5 int, comment string) []*string {
	addra := strconv.Itoa(addr)
	p1a := strconv.Itoa(p1)
	p2a := strconv.Itoa(p2)
	p3a := strconv.Itoa(p3)
	p4a := strconv.Itoa(p4)
	p5a := strconv.Itoa(p5)
	return []*string{
		&addra,
		&c,
		&p1a,
		&p2a,
		&p3a,
		&p4a,
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
	i := 1
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

func (c *initCmd) execute(registers map[int]any, resultRows *[][]*string) cmdRes {
	return cmdRes{
		nextAddress: c.p2,
	}
}

func (c *initCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Start at addr[%d]", c.p2)
	return formatExplain(addr, "Init", c.p1, c.p2, c.p3, c.p4, c.p5, comment)
}

// haltCmd ends the routine.
type haltCmd cmd

func (c *haltCmd) execute(registers map[int]any, resultRows *[][]*string) cmdRes {
	return cmdRes{
		doHalt: true,
	}
}

func (c *haltCmd) explain(addr int) []*string {
	return formatExplain(addr, "Halt", c.p1, c.p2, c.p3, c.p4, c.p5, "Exit")
}

// transactionCmd starts a read or write transaction
type transactionCmd cmd

func (c *transactionCmd) execute(registers map[int]any, resultRows *[][]*string) cmdRes {
	return cmdRes{}
}

func (c *transactionCmd) explain(addr int) []*string {
	return formatExplain(addr, "Transaction", c.p1, c.p2, c.p3, c.p4, c.p5, "Begin read transaction")
}

// gotoCmd goes to the address at p2
type gotoCmd cmd

func (c *gotoCmd) execute(registers map[int]any, resultRows *[][]*string) cmdRes {
	return cmdRes{
		nextAddress: c.p2,
	}
}

func (c *gotoCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Jump to addr[%d]", c.p2)
	return formatExplain(addr, "Goto", c.p1, c.p2, c.p3, c.p4, c.p5, comment)
}

// openReadCmd opens a read cursor at page p2 with the identifier p1
type openReadCmd cmd

func (c *openReadCmd) execute(registers map[int]any, resultRows *[][]*string) cmdRes {
	return cmdRes{}
}

func (c *openReadCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Open read cursor with id %d at root page %d", c.p1, c.p2)
	return formatExplain(addr, "OpenRead", c.p1, c.p2, c.p3, c.p4, c.p5, comment)
}

// rewindCmd goes to the first entry in the table for cursor p1. If the table is
// empty it jumps to p2.
type rewindCmd cmd

func (c *rewindCmd) execute(registers map[int]any, resultRows *[][]*string) cmdRes {
	return cmdRes{}
}

func (c *rewindCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Move cursor %d to the start of the table. If the table is empty jump to addr[%d]", c.p1, c.p2)
	return formatExplain(addr, "Rewind", c.p1, c.p2, c.p3, c.p4, c.p5, comment)
}

// rowId store in register p2 an integer which is the key of the entry the
// cursor p1 is on
type rowIdCmd cmd

func (c *rowIdCmd) execute(registers map[int]any, resultRows *[][]*string) cmdRes {
	return cmdRes{}
}

func (c *rowIdCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Store id cursor %d is currently pointing to in register[%d]", c.p1, c.p2)
	return formatExplain(addr, "RowId", c.p1, c.p2, c.p3, c.p4, c.p5, comment)
}

// columnCmd stores in register p3 the value pointed to for the p2-th column.
type columnCmd cmd

func (c *columnCmd) execute(registers map[int]any, resultRows *[][]*string) cmdRes {
	return cmdRes{}
}

func (c *columnCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Store the value for the %d-th column in register[%d] for cursor %d", c.p2, c.p3, c.p1)
	return formatExplain(addr, "Column", c.p1, c.p2, c.p3, c.p4, c.p5, comment)
}

// resultRowCmd stores p1 through p1+p2-1 as a single row of results
type resultRowCmd cmd

func (c *resultRowCmd) execute(registers map[int]any, resultRows *[][]*string) cmdRes {
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
		default:
			// TODO err
			panic("unhandled result row")
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

func (c *nextCmd) execute(registers map[int]any, resultRows *[][]*string) cmdRes {
	return cmdRes{}
}

func (c *nextCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Advance cursor %d. If there are items jump to addr[%d]", c.p1, c.p2)
	return formatExplain(addr, "Next", c.p1, c.p2, c.p3, c.p4, c.p5, comment)
}

// integerCmd stores the integer in p1 in register p2.
type integerCmd cmd

func (c *integerCmd) execute(registers map[int]any, resultRows *[][]*string) cmdRes {
	registers[c.p2] = c.p1
	return cmdRes{}
}

func (c *integerCmd) explain(addr int) []*string {
	comment := fmt.Sprintf("Store integer %d in register[%d]", c.p1, c.p2)
	return formatExplain(addr, "Integer", c.p1, c.p2, c.p3, c.p4, c.p5, comment)
}
