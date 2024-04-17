// select generates a query plan for a selectStmt from an AST (Abstract Syntax
// Tree) generated by the parser. This plan is then fed to the vm (Virtual
// Machine) to be ran.
package main

import "github.com/chirst/cdb/compiler"

func newLogicalPlanner() *logicalPlanner {
	return &logicalPlanner{
		catalog: newCatalog(),
	}
}

func (l *logicalPlanner) forSelect(s *compiler.SelectStmt) *projection {
	p := &projection{}
	tablePageNumber := 0
	var tableColumns []string
	if s.From != nil && s.From.TableName != "" {
		tpn, err := l.catalog.getPageNumber(s.From.TableName)
		if err != nil {
			// handle
		}
		tcs, err := l.catalog.getColumns(s.From.TableName)
		if err != nil {
			// handle
		}
		tablePageNumber = tpn
		tableColumns = tcs
	}
	if tablePageNumber == 0 { // no table meaning this is a literal
		var ls []literal
		for _, rc := range s.ResultColumns {
			ls = append(ls, literal{numeric: rc.Expr.Literal.NumericLiteral})
		}
		p.childSet = set{
			literals: ls,
		}
	} else {
		// only supports select * for now
		for _, rc := range s.ResultColumns {
			if rc.All {
				p.fields = tableColumns
			}
		}
		p.childSet = set{
			rootPage: tablePageNumber,
		}
	}
	return p
}

type logicalPlanner struct {
	catalog *catalog
}

// Projection is the root of a logical query plan.
type projection struct {
	// Fields to project from the set.
	fields []string
	// Set that is being projected.
	childSet set
}

type set struct {
	// Page number of corresponding index or table.
	rootPage int
	// Literal values when no table is specified.
	literals []literal
}

type literal struct {
	numeric int
	// text    string
}

type physicalPlanner struct{}

func newPhysicalPlanner() *physicalPlanner {
	return &physicalPlanner{}
}

func (*physicalPlanner) forSelect(projection *projection, explain bool) *executionPlan {
	var commands map[int]command
	if projection.childSet.rootPage == 0 {
		commands = map[int]command{
			1: &initCmd{p2: 2},
			2: &integerCmd{p1: 1, p2: 1},
			3: &resultRowCmd{p1: 1, p2: 1},
			4: &haltCmd{},
		}
	} else {
		commands = map[int]command{
			1:  &initCmd{p2: 2},
			2:  &transactionCmd{},
			3:  &openReadCmd{p2: 2},
			4:  &rewindCmd{},
			5:  &rowIdCmd{},
			6:  &columnCmd{},
			7:  &columnCmd{},
			8:  &resultRowCmd{},
			9:  &nextCmd{},
			10: &haltCmd{},
		}
	}
	return &executionPlan{
		explain:  explain,
		commands: commands,
	}
}

/*
SELECT * FROM Product;
addr opcode        p1   p2   p3   p4            p5 comment
---- ------------- ---- ---- ---- ------------- -- -------------
0    Init          0    9    0                  00 Start at 9                   Very first opcode. If p2 is not zero then jump to p2. There are other details but they don't matter.
1    OpenRead      0    2    0    3             00 root=2 iDb=0; Product        Open read cursor at root page p2. p1 is an identifier for the cursor.
2    Rewind        0    8    0                  00                              Next use of Rowid, Column, or Next will be the first entry in the table or index. If the table is empty jump to p2.
3    Rowid         0    1    0                  00 r[1]=rowid                   Store in register p2 an integer which is the key of the table entry the cursor is on.
4    Column        0    1    2                  00 r[2]=Product.Name            Store in register p3 the value pointed to for the p2-th column. Using the MakeRecord instruction
5    Column        0    2    3                  00 r[3]=Product.Description     Store in register p3 the value pointed to for the p2-th column. Using the MakeRecord instruction
6    ResultRow     1    3    0                  00 output=r[1..3]               Registers p1 through p1+p2-1 contain a single row of results.
7    Next          0    3    0                  01                              Advance cursor p1 so it points at its next item. If there are no more items fall through. If there are items jump to p2.
8    Halt          0    0    0                  00                              Exit
9    Transaction   0    0    1    0             01 usesStmtJournal=0            If p2 is zero then read transaction started. If p2 is non zero then write transaction. Other details don't matter.
10   Goto          0    1    0                  00                              Jump to address p2.
*/
