package repl

import "testing"

func makeStr(s string) *string {
	return &s
}

func TestPrint(t *testing.T) {
	repl := New(nil)
	resultHeader := []*string{
		makeStr("id"),
		makeStr("name"),
	}
	resultRows := [][]*string{
		{
			makeStr("1"),
			makeStr("gud name"),
		},
		{
			makeStr("2"),
			makeStr("gudder name"),
		},
		{
			makeStr("3"),
			makeStr("guddest name"),
		},
		{
			makeStr("4"),
			nil,
		},
	}
	result := repl.printRows(resultHeader, resultRows)
	e := "" +
		" id | name         \n" +
		"----+--------------\n" +
		" 1  | gud name     \n" +
		" 2  | gudder name  \n" +
		" 3  | guddest name \n" +
		" 4  | NULL         \n"
	if result != e {
		t.Errorf("\nwant\n%s\ngot\n%s\n", e, result)
	}
}

func TestPrintCount(t *testing.T) {
	repl := New(nil)
	resultHeader := []*string{nil}
	resultRows := [][]*string{
		{
			makeStr("1"),
		},
	}
	result := repl.printRows(resultHeader, resultRows)
	e := "" +
		" NULL \n" +
		"------\n" +
		" 1    \n"
	if result != e {
		t.Errorf("\nwant\n%s\ngot\n%s\n", e, result)
	}
}
