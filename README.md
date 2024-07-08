# CDB
CDB is a SQL database built for learning about the inner workings of databases. CDB
was heavily inspired by [SQLite](https://www.sqlite.org/),
[CockroachDB](https://github.com/cockroachdb/cockroach), and the
[CMU Database Group](https://www.youtube.com/c/cmudatabasegroup). CDB implements
a subset of SQL described below.

## Language reference

### System tables
`cdb_schema` which holds the database schema.

### SELECT
```mermaid
graph LR
begin(( ))
explain([EXPLAIN])
select([SELECT])
all[*]
from([FROM])

begin --> explain
begin --> select
explain --> select
select --> all
all --> from
from --> table
```

### CREATE
```mermaid
graph LR
begin(( ))
explain([EXPLAIN])
create([CREATE])
table([TABLE])
colType([INTEGER or TEXT])
lparen["("]
colSep[","]
rparen[")"]
tableIdent["Table Identifier"]
colIdent["Column Identifier"]

begin --> explain
begin --> create
explain --> create
create --> table
table --> tableIdent
tableIdent --> lparen
lparen --> colIdent
colIdent --> colType
colType --> colSep
colType --> rparen
colSep --> rparen
colSep --> colIdent
```

### INSERT
```mermaid
graph LR
begin(( ))
explain([EXPLAIN])
insert([INSERT])
into([INTO])
tableIdent["Table Identifier"]
lparen["("]
rparen[")"]
colSep[","]
values([VALUES])
lparen2["("]
rparen2[")"]
colSep2[","]
literal["literal"]
valSep[","]

begin --> explain
begin --> insert
explain --> insert
insert --> into
into --> tableIdent
tableIdent --> lparen
lparen --> colIdent
colIdent --> colSep
colSep --> colIdent
colSep --> rparen
rparen --> values
values --> lparen2
lparen2 --> literal
literal --> colSep2
colSep2 --> literal
literal --> rparen2
colSep2 --> rparen2
rparen2 --> valSep
valSep --> lparen2
```

## Flags
Run `cdb -h` for command line flags.

## Architecture
```mermaid
---
title: Packages
---
graph LR
    subgraph Adapters
    Driver
    REPL
    end
    Driver --> DB
    REPL --> DB
    DB --> Compiler
    subgraph Compiler
    Lexer --> Parser --> AST
    end
    AST --> Planner
    Planner --> VM
    Planner --> Catalog
    VM --> KV
    subgraph KV
    Cursor
    Catalog
    Encoder
    end
    KV --> Pager
    subgraph Pager
    Storage
    end
```
### REPL (Read Eval Print Loop)
The REPL works with the DB (Database) layer and is responsible for two things.
Passing down the SQL strings that are read by the REPL to the DB. Printing out
execution results that are returned from the DB layer. The REPL can be thought
of as an adapter layer.

### Driver
The Driver plays the same role as the REPL in that it adapts the DB to be used
in a Go program. This is done by implementing the Go standard library 
`database/sql/driver.Driver` interface.

### DB (Database)
The DB (Database) layer is an interface that is called by adapters like the
REPL. In theory, the DB layer could be called directly by a consumer of the
package or by something like a TCP connection adapter.

### Compiler
The Compiler is responsible for converting a raw SQL string to a AST (Abstract
syntax tree). In doing this, the compiler performs two major steps known as
lexing and parsing.

### Planner
The Planner is what is known as a query planner. The planner takes the AST
generated by the compiler and performs steps to generate an optimal "byte code"
routine consisting of commands defined in the VM. This routine can be examined
by prefixing any SQL statement with the `EXPLAIN` keyword.

### VM (Virtual Machine)
The VM defines a set of commands that can be executed or explained. Each command
performs basic calls into the KV layer that make up a query execution. This
mechanism makes queries predictable and consistent.

### KV (Key Value)
The KV layer implements a data structure known as a
[B+ tree](https://en.wikipedia.org/wiki/B%2B_tree) this tree enables the
database to perform fast lookups. At this layer, data is encoded into byte
slices by the `Encoder`. The KV layer implements a cursor abstraction, which
enables queries to scan and seek the B trees associated with a
table or index. Additionally this layer maintains the `Catalog`, an in memory
representation of the database schema.

### Pager
The Pager sits on top of a contiguous block of bytes defined in the `Storage`
interface. This block is typically a single file enabling the database to
persist data, but it can be an in memory representation. The pager abstracts
this block into pages which represent nodes in the KV layer's B tree. The pager
is capable of caching the pages. The pager implements a read write mutex for
concurrency control. The pager implements atomic writes to its storage through
what is known as the journal file.
