package main

import (
	"github.com/chirst/cdb/compiler"
)

type createPlanner struct {
	catalog *catalog
}

func newCreatePlanner() *createPlanner {
	return &createPlanner{
		catalog: newCatalog(),
	}
}

func (*createPlanner) getPlan(s *compiler.CreateStmt) (*executionPlan, error) {
	schema := tableSchema{
		Columns: []tableColumn{
			{
				Name:    "first",
				ColType: "text",
			},
			{
				Name:    "last",
				ColType: "text",
			},
		},
	}
	jSchema, err := schema.ToJSON()
	if err != nil {
		return nil, err
	}
	commands := map[int]command{}
	commands[1] = &initCmd{p2: 2}                         // go to command 2.
	commands[2] = &transactionCmd{p2: 1}                  // start write transaction
	commands[3] = &createBTreeCmd{p2: 1}                  // create b tree for the new table and store root page number in register[1]
	commands[4] = &openWriteCmd{p1: 1, p2: 1}             // open write cursor to write the new table to the catalog
	commands[5] = &newRowIdCmd{p1: 1, p2: 2}              // store new row id in register[2]
	commands[6] = &stringCmd{p1: 3, p4: "table"}          // type store in register[3]
	commands[7] = &stringCmd{p1: 4, p4: "foo"}            // name  store in register[4]
	commands[8] = &stringCmd{p1: 5, p4: "foo"}            // tablename store in register[5]
	commands[9] = &copyCmd{p1: 1, p2: 6}                  // rootpage store in register[6]
	commands[10] = &stringCmd{p1: 7, p4: string(jSchema)} // schema store in register[7]
	commands[11] = &makeRecordCmd{p1: 3, p2: 4, p3: 8}    // make record from register[3-3+4] and store in register[8]
	commands[12] = &insertCmd{p1: 1, p2: 8, p3: 2}        // insert with cursor 1 with key register[2] and value register[8]
	commands[13] = &parseSchemaCmd{}                      // refresh catalog cache with new values
	commands[14] = &haltCmd{p2: 1}                        // end transactions
	return &executionPlan{
		explain:  s.Explain,
		commands: commands,
	}, nil
}

// schema table definition
// type 'table'
// name 'foo'
// table_name 'foo'
// rootpage int
// sql text

// | addr | opcode      | p1  | p2  | p3  | p4                                         | p5  | comment |
// | ---- | ----------- | --- | --- | --- | ------------------------------------------ | --- | ------- |
// | 0    | Init        | 0   | 28  | 0   |                                            | 0   |         |
// | 1    | ReadCookie  | 0   | 3   | 2   |                                            | 0   |         |
// | 2    | If          | 3   | 5   | 0   |                                            | 0   |         |
// | 3    | SetCookie   | 0   | 2   | 4   |                                            | 0   |         |
// | 4    | SetCookie   | 0   | 5   | 1   |                                            | 0   |         |
// | 5    | CreateBtree | 0   | 2   | 1   |                                            | 0   |         |
// | 6    | OpenWrite   | 0   | 1   | 0   | 5                                          | 0   |         |
// | 7    | NewRowid    | 0   | 1   | 0   |                                            | 0   |         |
// | 8    | Blob        | 6   | 3   | 0   |                                            | 0   |         |
// | 9    | Insert      | 0   | 3   | 1   |                                            | 8   |         |
// | 10   | Close       | 0   | 0   | 0   |                                            | 0   |         |
// | 11   | Close       | 0   | 0   | 0   |                                            | 0   |         |
// | 12   | Null        | 0   | 4   | 5   |                                            | 0   |         |
// | 13   | Noop        | 2   | 0   | 4   |                                            | 0   |         |
// | 14   | OpenWrite   | 1   | 1   | 0   | 5                                          | 0   |         |
// | 15   | SeekRowid   | 1   | 17  | 1   |                                            | 0   |         |
// | 16   | Rowid       | 1   | 5   | 0   |                                            | 0   |         |
// | 17   | IsNull      | 5   | 25  | 0   |                                            | 0   |         |
// | 18   | String8     | 0   | 6   | 0   | table                                      | 0   |         |
// | 19   | String8     | 0   | 7   | 0   | foo                                        | 0   |         |
// | 20   | String8     | 0   | 8   | 0   | foo                                        | 0   |         |
// | 21   | SCopy       | 2   | 9   | 0   |                                            | 0   |         |
// | 22   | String8     | 0   | 10  | 0   | CREATE TABLE foo(
//   id int,
//   first text
// ) | 0   |         |
// | 23   | MakeRecord  | 6   | 5   | 4   | BBBDB                                      | 0   |         |
// | 24   | Insert      | 1   | 4   | 5   |                                            | 0   |         |
// | 25   | SetCookie   | 0   | 1   | 1   |                                            | 0   |         |
// | 26   | ParseSchema | 0   | 0   | 0   | tbl_name='foo' AND type!='trigger'         | 0   |         |
// | 27   | Halt        | 0   | 0   | 0   |                                            | 0   |         |
// | 28   | Transaction | 0   | 1   | 0   | 0                                          | 1   |         |
// | 29   | Goto        | 0   | 1   | 0   |                                            | 0   |         |
