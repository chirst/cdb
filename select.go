// select generates a query plan for a selectStmt from an AST (Abstract Syntax
// Tree) generated by the parser. This plan is then fed to the vm (Virtual
// Machine) to be ran.
package main

import "github.com/chirst/cdb/compiler"

type selectPlanner struct {
	catalog *catalog
}

func newSelectPlanner() *selectPlanner {
	return &selectPlanner{
		catalog: newCatalog(),
	}
}

func (p *selectPlanner) getPlan(s *compiler.SelectStmt) (*executionPlan, error) {
	lp, err := p.getLogicalPlan(s)
	if err != nil {
		return nil, err
	}
	return p.getPhysicalPlan(lp, s.Explain)
}

func (l *selectPlanner) getLogicalPlan(s *compiler.SelectStmt) (*projection, error) {
	p := &projection{}
	tablePageNumber := 0
	var tableColumns []string
	if s.From != nil && s.From.TableName != "" {
		tpn, err := l.catalog.getPageNumber(s.From.TableName)
		if err != nil {
			return nil, err
		}
		tcs, err := l.catalog.getColumns(s.From.TableName)
		if err != nil {
			return nil, err
		}
		tablePageNumber = tpn
		tableColumns = tcs
	}
	if s.ResultColumn.All {
		p.fields = tableColumns
	}
	p.childSet = set{
		rootPage: tablePageNumber,
	}
	return p, nil
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
}

func (*selectPlanner) getPhysicalPlan(projection *projection, explain bool) (*executionPlan, error) {
	// commands := map[int]command{}
	// commands[1] = &initCmd{p2: 2}
	// commands[2] = &transactionCmd{}
	// commands[3] = &openReadCmd{p2: 2}
	// commands[4] = &rewindCmd{p2: 9}
	// commands[5] = &rowIdCmd{p2: 1}
	// commands[6] = &columnCmd{p2: 1, p3: 2}
	// commands[7] = &resultRowCmd{p1: 1, p2: 2}
	// commands[8] = &nextCmd{p2: 5}
	// commands[9] = &haltCmd{}
	// return &executionPlan{
	// 	explain:  explain,
	// 	commands: commands,
	// }, nil
	commands := map[int]command{}
	commands[1] = &initCmd{p2: 2} // go to command 2
	commands[2] = &stringCmd{p1: 1, p4: "id"}
	commands[3] = &stringCmd{p1: 2, p4: "type"}
	commands[4] = &stringCmd{p1: 3, p4: "name"}
	commands[5] = &stringCmd{p1: 4, p4: "table_name"}
	commands[6] = &stringCmd{p1: 5, p4: "rootpage"}
	commands[7] = &stringCmd{p1: 6, p4: "sql"}
	commands[8] = &resultRowCmd{p1: 1, p2: 6}      // create result row from the 1-6 registers
	commands[9] = &transactionCmd{p2: 0}           // start read transaction
	commands[10] = &openReadCmd{p1: 1, p2: 1}      // open read cursor with id 1 on table 1
	commands[11] = &rewindCmd{p1: 1, p2: 20}       // go to first record for table 1 if the table is empty go to command 20
	commands[12] = &rowIdCmd{p1: 1, p2: 1}         // store in register[1] the key of the current record
	commands[13] = &columnCmd{p1: 1, p2: 1, p3: 2} // store in register[2] the value of the 1st column
	commands[14] = &columnCmd{p1: 1, p2: 2, p3: 3} // store in register[3] the value of the 2nd column
	commands[15] = &columnCmd{p1: 1, p2: 3, p3: 4} // store in register[4] the value of the 3rd column
	commands[16] = &columnCmd{p1: 1, p2: 4, p3: 5} // store in register[5] the value of the 4th column
	commands[17] = &columnCmd{p1: 1, p2: 5, p3: 6} // store in register[6] the value of the 5th column
	commands[18] = &resultRowCmd{p1: 1, p2: 6}     // create result row from the 1-6 registers
	commands[19] = &nextCmd{p2: 5}                 // advance cursor and go to command 5 if the cursor is empty fall through
	commands[20] = &haltCmd{}                      // end transactions
	return &executionPlan{
		explain:  explain,
		commands: commands,
	}, nil
}
