// vm (Virtual Machine) is capable of running routines made up of commands that
// access the kv layer. The commands are formed from the ast (Abstract Syntax
// Tree).
package main

import "fmt"

type command interface {
	execute() cmdRes
	explain() string
}

type cmdRes struct {
	doHalt      bool
	nextAddress int
}

type cmd struct {
	p1 int
	p2 int
	p3 int
	// p4 int
	// p5 int
}

func execute(commands map[int]command) {
	i := 1
	var currentCommand command
	for {
		if len(commands) < i {
			break
		}
		currentCommand = commands[i]
		res := currentCommand.execute()
		if res.doHalt {
			break
		}
		if res.nextAddress == 0 {
			i = i + 1
		} else {
			i = res.nextAddress
		}
	}
}

func formatExplain(c string, p1, p2, p3 int, comment string) string {
	return fmt.Sprintf("%-13s %-4d %-4d %-4d %s", c, p1, p2, p3, comment)
}

func explain(commands map[int]command) {
	i := 1
	var currentCommand command
	fmt.Println("addr opcode        p1   p2   p3   comment")
	fmt.Println("---- ------------- ---- ---- ---- -------------")
	for {
		if len(commands) < i {
			break
		}
		currentCommand = commands[i]
		currentCommand.explain()
		fmt.Printf("%-4d %s\n", i, currentCommand.explain())
		i = i + 1
	}
}

// initCmd jumps to the instruction at address p2.
type initCmd cmd

func (c *initCmd) execute() cmdRes {
	return cmdRes{
		nextAddress: c.p2,
	}
}

func (c *initCmd) explain() string {
	comment := fmt.Sprintf("Start at addr[%d]", c.p2)
	return formatExplain("Init", c.p1, c.p2, c.p3, comment)
}

// haltCmd ends the routine.
type haltCmd cmd

func (c *haltCmd) execute() cmdRes {
	return cmdRes{
		doHalt: true,
	}
}

func (c *haltCmd) explain() string {
	return formatExplain("Halt", c.p1, c.p2, c.p3, "Exit")
}

// transactionCmd starts a read or write transaction
type transactionCmd cmd

func (c *transactionCmd) execute() cmdRes {
	return cmdRes{}
}

func (c *transactionCmd) explain() string {
	return formatExplain("Transaction", c.p1, c.p2, c.p3, "Begin read transaction")
}

// gotoCmd goes to the address at p2
type gotoCmd cmd

func (c *gotoCmd) execute() cmdRes {
	return cmdRes{
		nextAddress: c.p2,
	}
}

func (c *gotoCmd) explain() string {
	comment := fmt.Sprintf("Jump to addr[%d]", c.p2)
	return formatExplain("Goto", c.p1, c.p2, c.p3, comment)
}

// openReadCmd opens a read cursor at page p2 with the identifier p1
type openReadCmd cmd

func (c *openReadCmd) execute() cmdRes {
	return cmdRes{}
}

func (c *openReadCmd) explain() string {
	comment := fmt.Sprintf("Open read cursor with id %d at root page %d", c.p1, c.p2)
	return formatExplain("OpenRead", c.p1, c.p2, c.p3, comment)
}

// rewindCmd goes to the first entry in the table for cursor p1. If the table is
// empty it jumps to p2.
type rewindCmd cmd

func (c *rewindCmd) execute() cmdRes {
	return cmdRes{}
}

func (c *rewindCmd) explain() string {
	comment := fmt.Sprintf("Move cursor %d to the start of the table. If the table is empty jump to addr[%d]", c.p1, c.p2)
	return formatExplain("Rewind", c.p1, c.p2, c.p3, comment)
}

// rowId store in register p2 an integer which is the key of the entry the
// cursor p1 is on
type rowIdCmd cmd

func (c *rowIdCmd) execute() cmdRes {
	return cmdRes{}
}

func (c *rowIdCmd) explain() string {
	comment := fmt.Sprintf("Store id cursor %d is currently pointing to in register[%d]", c.p1, c.p2)
	return formatExplain("RowId", c.p1, c.p2, c.p3, comment)
}

// columnCmd stores in register p3 the value pointed to for the p2-th column.
type columnCmd cmd

func (c *columnCmd) execute() cmdRes {
	return cmdRes{}
}

func (c *columnCmd) explain() string {
	comment := fmt.Sprintf("Store the value for the %d-th column in register[%d] for cursor %d", c.p2, c.p3, c.p1)
	return formatExplain("Column", c.p1, c.p2, c.p3, comment)
}

// resultRowCmd stores p1 through p1+p2 as a single row of results
type resultRowCmd cmd

func (c *resultRowCmd) execute() cmdRes {
	return cmdRes{}
}

func (c *resultRowCmd) explain() string {
	comment := fmt.Sprintf("Make a row from registers[%d..%d]", c.p1, c.p2)
	return formatExplain("ResultRow", c.p1, c.p2, c.p3, comment)
}

// nextCmd advances the cursor p1. If the cursor has reached the end fall
// through. If there is more for the cursor to process jump to p2.
type nextCmd cmd

func (c *nextCmd) execute() cmdRes {
	return cmdRes{}
}

func (c *nextCmd) explain() string {
	comment := fmt.Sprintf("Advance cursor %d. If there are items jump to addr[%d]", c.p1, c.p2)
	return formatExplain("Next", c.p1, c.p2, c.p3, comment)
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

SELECT * FROM Product WHERE ProductID = 1;
addr opcode        p1   p2   p3   p4            p5 comment
---- ------------- ---- ---- ---- ------------- -- -------------
0    Init          0    9    0                  00 Start at 9
1    OpenRead      0    2    0    3             00 root=2 iDb=0; Product
2    Integer       1    1    0                  00 r[1]=1
3    SeekRowid     0    8    1                  00 intkey=r[1]
4    Rowid         0    2    0                  00 r[2]=rowid
5    Column        0    1    3                  00 r[3]=Product.Name
6    Column        0    2    4                  00 r[4]=Product.Description
7    ResultRow     2    3    0                  00 output=r[2..4]
8    Halt          0    0    0                  00
9    Transaction   0    0    1    0             01 usesStmtJournal=0
10   Goto          0    1    0                  00
*/

/*
EXPLAIN CREATE TABLE Product (
  ProductID INTEGER PRIMARY KEY AUTOINCREMENT,
  Name TEXT
);

addr opcode        p1   p2   p3   p4            p5 comment
---- ------------- ---- ---- ---- ------------- -- -------------
0    Init          0    54   0                  00 Start at 54
1    ReadCookie    0    3    2                  00
2    If            3    5    0                  00
3    SetCookie     0    2    4                  00
4    SetCookie     0    5    1                  00
5    CreateBtree   0    2    1                  00 r[2]=root iDb=0 flags=1
6    OpenWrite     0    1    0                  5 00 root=1 iDb=0
7    NewRowid      0    1    0                  00 r[1]=rowid
8    Blob          6    3    0                00 r[3]= (len=6)
9    Insert        0    3    1                  08 intkey=r[1] data=r[3]
10   Close         0    0    0                  00
11   Close         0    0    0                  00
12   Null          0    4    5                  00 r[4..5]=NULL
13   OpenWrite     1    1    0    5             00 root=1 iDb=0; sqlite_master
14   SeekRowid     1    16   1                  00 intkey=r[1]
15   Rowid         1    5    0                  00 r[5]=rowid
16   IsNull        5    25   0                  00 if r[5]==NULL goto 25
17   String8       0    6    0    table         00 r[6]='table'
18   String8       0    7    0    Product       00 r[7]='Product'
19   String8       0    8    0    Product       00 r[8]='Product'
20   Copy          2    9    0                  00 r[9]=r[2]
21   String8       0    10   0    CREATE TABLE Product (ProductID INTEGER PRIMARY KEY AUTOINCREMENT,Name TEXT) 00 r[10]='CREATE TABLE Product (ProductID INTEGER PRIMARY KEY AUTOINCREMENT,Name TEXT)'
22   Delete        1    68   5   				00
23   MakeRecord    6    5    11   BBBDB         00 r[11]=mkrec(r[6..10])
24   Insert        1    11   5                  00 intkey=r[5] data=r[11]
25   SetCookie     0    1    1                  00
26   ReadCookie    0    14   2                  00
27   If            14   30   0                  00
28   SetCookie     0    2    4                  00
29   SetCookie     0    5    1                  00
30   CreateBtree   0    13   1                  00 r[13]=root iDb=0 flags=1
31   OpenWrite     0    1    0    5             00 root=1 iDb=0
32   NewRowid      0    12   0                  00 r[12]=rowid
33   Blob          6    14   0                00 r[14]= (len=6)
34   Insert        0    14   12                 08 intkey=r[12] data=r[14]
35   Close         0    0    0                  00
36   Close         0    0    0                  00
37   Null          0    15   16                 00 r[15..16]=NULL
38   OpenWrite     2    1    0    5             00 root=1 iDb=0; sqlite_master
39   SeekRowid     2    41   12                 00 intkey=r[12]
40   Rowid         2    16   0                  00 r[16]=rowid
41   IsNull        16   50   0                  00 if r[16]==NULL goto 50
42   String8       0    17   0    table         00 r[17]='table'
43   String8       0    18   0    sqlite_sequence 00 r[18]='sqlite_sequence'
44   String8       0    19   0    sqlite_sequence 00 r[19]='sqlite_sequence'
45   Copy          13   20   0                  00 r[20]=r[13]
46   String8       0    21   0    CREATE TABLE sqlite_sequence(name,seq) 00 r[21]='CREATE TABLE sqlite_sequence(name,seq)'
47   Delete        2    68   16                 00
48   MakeRecord    17   5    22   BBBDB         00 r[22]=mkrec(r[17..21])
49   Insert        2    22   16                 00 intkey=r[16] data=r[22]
50   SetCookie     0    1    1                  00
51   ParseSchema   0    0    0    tbl_name='sqlite_sequence' AND type!='trigger' 00
52   ParseSchema   0    0    0    tbl_name='Product' AND type!='trigger' 00
53   Halt          0    0    0                  00
54   Transaction   0    1    0    0             01 usesStmtJournal=0            p2 is non zero so it is a write transaction
55   Goto          0    1    0                  00
*/

/*
EXPLAIN INSERT INTO Product(Name) VALUES ('Entity Framework Extensions');

addr opcode        p1   p2   p3   p4            p5 comment
---- ------------- ---- ---- ---- ------------- -- -------------
0    Init          0    16   0                  00 Start at 16
1    OpenWrite     0    2    0    2             00 root=2 iDb=0; Product
2    NewRowid      0    5    2                  00 r[5]=rowid
3    MemMax        2    5    0                  00 r[2]=max(r[2],r[5])
4    SoftNull      6    0    0                  00 r[6]=NULL
5    String8       0    7    0    Entity Framework Extensions 00 r[7]='Entity Framework Extensions'
6    MakeRecord    6    2    8    DB 00 r[8]=mkrec(r[6..7])
7    Insert        0    8    5    Product 39 intkey=r[5] data=r[8]
8    Le            4    15   2                  00 if r[2]<=r[4] goto 15
9    OpenWrite     0    3    0    2             00 root=3 iDb=0; sqlite_sequence
10   NotNull       3    12   0                  00 if r[3]!=NULL goto 12
11   NewRowid      0    3    0                  00 r[3]=rowid
12   MakeRecord    1    2    9                  00 r[9]=mkrec(r[1..2])
13   Insert        0    9    3                  08 intkey=r[3] data=r[9]
14   Close         0    0    0                  00
15   Halt          0    0    0                  00
16   Transaction   0    1    1    0             01 usesStmtJournal=0
17   OpenRead      0    3    0    2             00 root=3 iDb=0; sqlite_sequence
18   String8       0    1    0    Product       00 r[1]='Product'
19   Null          0    2    4                  00 r[2..4]=NULL
20   Rewind        0    29   0                  00
21   Column        0    0    2                  00 r[2]=
22   Ne            1    28   2                  10 if r[2]!=r[1] goto 28
23   Rowid         0    3    0                  00 r[3]=rowid
24   Column        0    1    2                  00 r[2]=
25   AddImm        2    0    0                  00 r[2]=r[2]+0
26   Copy          2    4    0                  00 r[4]=r[2]
27   Goto          0    30   0                  00
28   Next          0    21   0                  00
29   Integer       0    2    0                  00 r[2]=0
30   Close         0    0    0                  00
31   Goto          0    1    0                  00
*/
