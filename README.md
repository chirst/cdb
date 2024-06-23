# CDB
cdb is a database built for learning about the inner workings of databases. cdb was heavily inspired by SQLite, Cockroach database, and the CMU database lectures. cdb implments a subset of SQL described below.

## Language reference

### Special tables
`cdb_schema` which holds the database schema.

### SELECT
```mermaid
flowchart LR
begin[ ]
explain([EXPLAIN])
select([SELECT])
from([FROM])

begin --> explain
begin --> select
explain --> select
select --> *
* --> from
from --> table
```

### CREATE
```mermaid
flowchart LR
begin[ ]
explain([EXPLAIN])
create([CREATE])
table([TABLE])
colType([INTEGER or TEXT])
lparen("(")
colSep(",")
rparen(")")
tableIdent("Table Identifier")
colIdent("Column Identifier")

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
flowchart LR
begin[ ]
explain([EXPLAIN])
insert([INSERT])
into([INTO])
tableIdent("Table Identifier")
lparen("(")
rparen(")")
colSep(",")
values([VALUES])
lparen2("(")
rparen2("(")
colSep2(",")
literal("literal")

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
```
