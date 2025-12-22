package planner

// func TestExplainQueryPlan(t *testing.T) {
// 	root := &projectNodeV2{
// 		child: &joinNodeV2{
// 			operation: "join",
// 			left: &joinNodeV2{
// 				operation: "join",
// 				left: &scanNodeV2{
// 					tableName: "foo",
// 				},
// 				right: &joinNodeV2{
// 					operation: "join",
// 					left: &scanNodeV2{
// 						tableName: "baz",
// 					},
// 					right: &scanNodeV2{
// 						tableName: "buzz",
// 					},
// 				},
// 			},
// 			right: &scanNodeV2{
// 				tableName: "bar",
// 			},
// 		},
// 	}
// 	qp := newQueryPlan(root, true)
// 	formattedResult := qp.ToString()
// 	expectedResult := "" +
// 		" ── project\n" +
// 		"     └─ join\n" +
// 		"         ├─ join\n" +
// 		"         |   ├─ scan table foo\n" +
// 		"         |   └─ join\n" +
// 		"         |       ├─ scan table baz\n" +
// 		"         |       └─ scan table buzz\n" +
// 		"         └─ scan table bar\n"
// 	if formattedResult != expectedResult {
// 		t.Fatalf("got\n%s\nwant\n%s", formattedResult, expectedResult)
// 	}
// }
