package main

import (
	"testing"
)

func TestExecute(t *testing.T) {
	input := "EXPLAIN SELECT 1;"
	expectation := "addr opcode        p1   p2   p3   comment\n" +
		"---- ------------- ---- ---- ---- -------------\n" +
		"1    Init          0    2    0    Start at addr[2]\n" +
		"2    Integer       1    1    0    Store integer 1 in register[1]\n" +
		"3    ResultRow     1    1    0    Make a row from registers[1..1]\n" +
		"4    Halt          0    0    0    Exit\n"
	db := newDb()
	res := db.execute(input)
	if res[0].text != expectation {
		t.Errorf("got:\n%s want:\n%s", res[0].text, expectation)
	}
}
