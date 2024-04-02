package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	db := newDb()
	fmt.Println("Welcome to cdb. Type .exit to exit")
	reader := bufio.NewScanner(os.Stdin)
	for getInput(reader) {
		input := reader.Text()
		if len(input) == 0 {
			continue
		}
		if input[0] == '.' {
			if input == ".exit" {
				os.Exit(0)
			}
		}
		results := db.execute(input)
		for _, result := range results {
			if result.err != nil {
				fmt.Printf("Err: %s", result.err.Error())
				continue
			}
			if result.text != "" {
				fmt.Print(result.text)
			}
			if result.text == "" {
				printRows(result.resultRows)
			}
		}
	}
}

func getInput(reader *bufio.Scanner) bool {
	fmt.Printf("cdb > ")
	return reader.Scan()
}

func printRows(resultRows [][]any) {
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

// cs := map[int]command{
// 	1:  &initCmd{p2: 9},
// 	2:  &openReadCmd{p1: 0, p2: 1},
// 	3:  &rewindCmd{p1: 0, p2: 8},
// 	4:  &rowIdCmd{p1: 0, p2: 1},
// 	5:  &columnCmd{p1: 0, p2: 1, p3: 2},
// 	6:  &resultRowCmd{p1: 1, p2: 2},
// 	7:  &nextCmd{p1: 0, p2: 4},
// 	8:  &haltCmd{},
// 	9:  &transactionCmd{},
// 	10: &gotoCmd{p2: 2},
// }
// explain(cs)

// pager should have a catalogue in memory. When a write transaction is started
// there should be a bit set by the opcode indicating whether or not the
// transaction will update the catalogue. This means a write transaction started
// and bit flipped. Right before the write transaction is closed the schema
// cache will be repopulated. On startup the schema cache will also need to be
// hydrated sometime soon after any pending journals are dealt with. This should
// mean not having to make a bunch of stuff to create tables right away.

// init
// transaction for write
// createBTree store root page number in p2
// OpenWrite on schema table
// NewRowid for schema table
// String for 'table' r1
// String for 'Foo' r2
// String for 'Foo' r3
// Copy btree page into r4
// MakeRecord for r1..r4
// Insert record
// halt
//
// table, Foo, Foo, 2, CREATE TABLE Product (ProductID INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT)
//
// CREATE TABLE sqlite_schema(
//   type text,
//   name text,
//   tbl_name text,
//   rootpage integer,
//   sql text
// );
//
