package planner

import "testing"

func TestExplainQueryPlan(t *testing.T) {
	root := &projectNode{
		child: &joinNode{
			operation: "join",
			left: &joinNode{
				operation: "join",
				left:      &scanNode{},
				right: &joinNode{
					operation: "join",
					left:      &scanNode{},
					right:     &scanNode{},
				},
			},
			right: &scanNode{},
		},
	}
	qp := newQueryPlan(root, true, transactionTypeRead, 0)
	formattedResult := qp.ToString()
	expectedResult := "" +
		" ── project\n" +
		"     └─ join\n" +
		"         ├─ join\n" +
		"         |   ├─ scan table\n" +
		"         |   └─ join\n" +
		"         |       ├─ scan table\n" +
		"         |       └─ scan table\n" +
		"         └─ scan table\n"
	if formattedResult != expectedResult {
		t.Fatalf("got\n%s\nwant\n%s", formattedResult, expectedResult)
	}
}
