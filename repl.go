// repl (read eval print loop) adapts db to the command line.
package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type repl struct {
	db *db
}

func newRepl(db *db) *repl {
	return &repl{db: db}
}

func (r *repl) run() {
	fmt.Println("Welcome to cdb. Type .exit to exit")
	reader := bufio.NewScanner(os.Stdin)
	for r.getInput(reader) {
		input := reader.Text()
		if len(input) == 0 {
			continue
		}
		if input[0] == '.' {
			if input == ".exit" {
				os.Exit(0)
			}
		}
		result := r.db.execute(input)
		if result.err != nil {
			fmt.Printf("Err: %s\n", result.err.Error())
			continue
		}
		if result.text != "" {
			fmt.Println(result.text)
		}
		if len(result.resultRows) != 0 {
			fmt.Println(r.printRows(result.resultRows))
		}
	}
}

func (*repl) getInput(reader *bufio.Scanner) bool {
	fmt.Printf("cdb > ")
	return reader.Scan()
}

func (r *repl) printRows(resultRows [][]*string) string {
	ret := ""
	widths := r.getWidths(resultRows)
	for i, row := range resultRows {
		if i == 0 {
			ret += r.printHeader(row, widths)
		} else {
			ret += r.printRow(row, widths)
		}
		ret = ret + "\n"
	}
	if len(resultRows) == 1 {
		ret = ret + "(0 rows)\n"
	}
	return ret
}

func (*repl) getWidths(resultRows [][]*string) []int {
	widths := make([]int, len(resultRows[0]))
	for i := range widths {
		widths[i] = 0
	}
	for _, row := range resultRows {
		for i, column := range row {
			size := len("NULL")
			if column != nil {
				size = len(*column)
			}
			if widths[i] < size {
				widths[i] = size
			}
		}
	}
	return widths
}

func (*repl) printHeader(row []*string, widths []int) string {
	ret := ""
	for i, column := range row {
		v := "NULL"
		if column != nil {
			v = *column
		}
		ret = ret + fmt.Sprintf(" %-*s ", widths[i], v)
		if i != len(row)-1 {
			ret = ret + "|"
		}
	}
	ret = ret + "\n"
	for i := range row {
		ret = ret + fmt.Sprintf("-%s-", strings.Repeat("-", widths[i]))
		if i != len(row)-1 {
			ret = ret + "+"
		}
	}
	return ret
}

func (*repl) printRow(row []*string, widths []int) string {
	ret := ""
	for i, column := range row {
		v := "NULL"
		if column != nil {
			v = *column
		}
		ret = ret + fmt.Sprintf(" %-*s ", widths[i], v)
		if i != len(row)-1 {
			ret = ret + "|"
		}
	}
	return ret
}
