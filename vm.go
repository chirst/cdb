// vm (Virtual Machine) is capable of running routines made up of commands that
// access the kv layer. The commands are formed from the ast (Abstract Syntax
// Tree).
package main

import "fmt"

type command interface {
	execute(registers map[int]any, resultRows *[][]any) cmdRes
	explain() string
}

type cmdRes struct {
	doHalt      bool
	nextAddress int
}

type cmd struct {
	p1 int
	p2 int
	p3 int
	// p4 int
	// p5 int
}

type executeResult struct {
	err        error
	text       string
	resultRows [][]any
}

type executionPlan struct {
	explain  bool
	commands map[int]command
}

func run(plan executionPlan) executeResult {
	if plan.explain {
		return executeResult{
			text: explain(plan),
		}
	}
	return execute(plan)
}

func execute(plan executionPlan) executeResult {
	registers := map[int]any{}
	resultRows := &[][]any{}
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
	return executeResult{
		resultRows: *resultRows,
	}
}

func formatExplain(c string, p1, p2, p3 int, comment string) string {
	return fmt.Sprintf("%-13s %-4d %-4d %-4d %s", c, p1, p2, p3, comment)
}

func explain(plan executionPlan) string {
	ret := ""
	i := 1
	var currentCommand command
	ret = ret + fmt.Sprint("addr opcode        p1   p2   p3   comment\n")
	ret = ret + fmt.Sprint("---- ------------- ---- ---- ---- -------------\n")
	for {
		if len(plan.commands) < i {
			break
		}
		currentCommand = plan.commands[i]
		currentCommand.explain()
		ret = ret + fmt.Sprintf("%-4d %s\n", i, currentCommand.explain())
		i = i + 1
	}
	return ret
}

// initCmd jumps to the instruction at address p2.
type initCmd cmd

func (c *initCmd) execute(registers map[int]any, resultRows *[][]any) cmdRes {
	return cmdRes{
		nextAddress: c.p2,
	}
}

func (c *initCmd) explain() string {
	comment := fmt.Sprintf("Start at addr[%d]", c.p2)
	return formatExplain("Init", c.p1, c.p2, c.p3, comment)
}

// haltCmd ends the routine.
type haltCmd cmd

func (c *haltCmd) execute(registers map[int]any, resultRows *[][]any) cmdRes {
	return cmdRes{
		doHalt: true,
	}
}

func (c *haltCmd) explain() string {
	return formatExplain("Halt", c.p1, c.p2, c.p3, "Exit")
}

// transactionCmd starts a read or write transaction
type transactionCmd cmd

func (c *transactionCmd) execute(registers map[int]any, resultRows *[][]any) cmdRes {
	return cmdRes{}
}

func (c *transactionCmd) explain() string {
	return formatExplain("Transaction", c.p1, c.p2, c.p3, "Begin read transaction")
}

// gotoCmd goes to the address at p2
type gotoCmd cmd

func (c *gotoCmd) execute(registers map[int]any, resultRows *[][]any) cmdRes {
	return cmdRes{
		nextAddress: c.p2,
	}
}

func (c *gotoCmd) explain() string {
	comment := fmt.Sprintf("Jump to addr[%d]", c.p2)
	return formatExplain("Goto", c.p1, c.p2, c.p3, comment)
}

// openReadCmd opens a read cursor at page p2 with the identifier p1
type openReadCmd cmd

func (c *openReadCmd) execute(registers map[int]any, resultRows *[][]any) cmdRes {
	return cmdRes{}
}

func (c *openReadCmd) explain() string {
	comment := fmt.Sprintf("Open read cursor with id %d at root page %d", c.p1, c.p2)
	return formatExplain("OpenRead", c.p1, c.p2, c.p3, comment)
}

// rewindCmd goes to the first entry in the table for cursor p1. If the table is
// empty it jumps to p2.
type rewindCmd cmd

func (c *rewindCmd) execute(registers map[int]any, resultRows *[][]any) cmdRes {
	return cmdRes{}
}

func (c *rewindCmd) explain() string {
	comment := fmt.Sprintf("Move cursor %d to the start of the table. If the table is empty jump to addr[%d]", c.p1, c.p2)
	return formatExplain("Rewind", c.p1, c.p2, c.p3, comment)
}

// rowId store in register p2 an integer which is the key of the entry the
// cursor p1 is on
type rowIdCmd cmd

func (c *rowIdCmd) execute(registers map[int]any, resultRows *[][]any) cmdRes {
	return cmdRes{}
}

func (c *rowIdCmd) explain() string {
	comment := fmt.Sprintf("Store id cursor %d is currently pointing to in register[%d]", c.p1, c.p2)
	return formatExplain("RowId", c.p1, c.p2, c.p3, comment)
}

// columnCmd stores in register p3 the value pointed to for the p2-th column.
type columnCmd cmd

func (c *columnCmd) execute(registers map[int]any, resultRows *[][]any) cmdRes {
	return cmdRes{}
}

func (c *columnCmd) explain() string {
	comment := fmt.Sprintf("Store the value for the %d-th column in register[%d] for cursor %d", c.p2, c.p3, c.p1)
	return formatExplain("Column", c.p1, c.p2, c.p3, comment)
}

// resultRowCmd stores p1 through p1+p2-1 as a single row of results
type resultRowCmd cmd

func (c *resultRowCmd) execute(registers map[int]any, resultRows *[][]any) cmdRes {
	row := []any{}
	i := c.p1
	end := c.p1 + c.p2 - 1
	for i <= end {
		row = append(row, registers[i])
		i = i + 1
	}
	*resultRows = append(*resultRows, row)
	return cmdRes{}
}

func (c *resultRowCmd) explain() string {
	comment := fmt.Sprintf("Make a row from registers[%d..%d]", c.p1, c.p2)
	return formatExplain("ResultRow", c.p1, c.p2, c.p3, comment)
}

// nextCmd advances the cursor p1. If the cursor has reached the end fall
// through. If there is more for the cursor to process jump to p2.
type nextCmd cmd

func (c *nextCmd) execute(registers map[int]any, resultRows *[][]any) cmdRes {
	return cmdRes{}
}

func (c *nextCmd) explain() string {
	comment := fmt.Sprintf("Advance cursor %d. If there are items jump to addr[%d]", c.p1, c.p2)
	return formatExplain("Next", c.p1, c.p2, c.p3, comment)
}

// integerCmd stores the integer in p1 in register p2.
type integerCmd cmd

func (c *integerCmd) execute(registers map[int]any, resultRows *[][]any) cmdRes {
	registers[c.p2] = c.p1
	return cmdRes{}
}

func (c *integerCmd) explain() string {
	comment := fmt.Sprintf("Store integer %d in register[%d]", c.p1, c.p2)
	return formatExplain("Integer", c.p1, c.p2, c.p3, comment)
}
