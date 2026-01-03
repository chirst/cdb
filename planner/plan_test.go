package planner

import "testing"

func TestExplainQueryPlan(t *testing.T) {
	root := &projectNode{
		child: &joinNode{
			operation: "join",
			left: &joinNode{
				operation: "join",
				left: &scanNode{
					tableName: "foo",
				},
				right: &joinNode{
					operation: "join",
					left: &scanNode{
						tableName: "bar",
					},
					right: &scanNode{
						tableName: "baz",
					},
				},
			},
			right: &scanNode{
				tableName: "buzz",
			},
		},
	}
	qp := newQueryPlan(root, true, transactionTypeRead)
	formattedResult := qp.ToString()
	expectedResult := "" +
		" ── project\n" +
		"     └─ join\n" +
		"         ├─ join\n" +
		"         |   ├─ scan table foo\n" +
		"         |   └─ join\n" +
		"         |       ├─ scan table bar\n" +
		"         |       └─ scan table baz\n" +
		"         └─ scan table buzz\n"
	if formattedResult != expectedResult {
		t.Fatalf("got\n%s\nwant\n%s", formattedResult, expectedResult)
	}
}
