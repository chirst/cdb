package repl

import "testing"

func makeStr(s string) *string {
	return &s
}

func TestPrint(t *testing.T) {
	repl := New(nil)
	resultRows := [][]*string{
		{
			makeStr("id"),
			makeStr("name"),
		},
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
	result := repl.printRows(resultRows)
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
