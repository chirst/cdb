package planner

import "testing"

func TestExplainQueryPlan(t *testing.T) {
	root := &projectNode{
		projections: []projection{
			{colName: "id"},
			{colName: "first_name"},
			{colName: "last_name"},
		},
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
						tableName: "baz",
					},
					right: &scanNode{
						tableName: "buzz",
					},
				},
			},
			right: &scanNode{
				tableName: "bar",
			},
		},
	}
	qp := QueryPlan{root: root}
	formattedResult := qp.ToString()
	expectedResult := "" +
		"     └─ project(id, first_name, last_name)\n" +
		"         └─ join\n" +
		"             ├─ join\n" +
		"             |   ├─ scan table foo\n" +
		"             |   └─ join\n" +
		"             |       ├─ scan table baz\n" +
		"             |       └─ scan table buzz\n" +
		"             └─ scan table bar\n"
	if formattedResult != expectedResult {
		t.Fatalf("got\n%s\nwant\n%s", formattedResult, expectedResult)
	}
}
