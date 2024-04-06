// repl (read eval print loop) adapts db to the command line.
package main

import (
	"bufio"
	"fmt"
	"os"
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
		results := r.db.execute(input)
		for _, result := range results {
			if result.err != nil {
				fmt.Printf("Err: %s", result.err.Error())
				continue
			}
			if result.text != "" {
				fmt.Print(result.text)
			}
			if result.text == "" {
				r.printRows(result.resultRows)
			}
		}
	}
}

func (repl) getInput(reader *bufio.Scanner) bool {
	fmt.Printf("cdb > ")
	return reader.Scan()
}

func (repl) printRows(resultRows [][]any) {
	fmt.Println("| ? |")
	fmt.Println("+---+")
	for _, row := range resultRows {
		sr := "|"
		for _, col := range row {
			sr = sr + fmt.Sprintf(" %v |", col)
		}
		sr = sr + "\n"
		fmt.Print(sr)
	}
}
