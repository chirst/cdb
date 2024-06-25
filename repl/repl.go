// repl (read eval print loop) adapts db to the command line.
package repl

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/chirst/cdb/db"
)

type repl struct {
	db *db.DB
}

func New(db *db.DB) *repl {
	return &repl{db: db}
}

func (r *repl) Run() {
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
		result := r.db.Execute(input)
		if result.Err != nil {
			fmt.Printf("Err: %s\n", result.Err.Error())
			continue
		}
		if result.Text != "" {
			fmt.Println(result.Text)
		}
		if len(result.ResultRows) != 0 {
			fmt.Println(r.printRows(result.ResultRows))
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
