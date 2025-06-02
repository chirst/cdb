package main

import (
	"C"
	"flag"
	"log"
	"strconv"

	"github.com/chirst/cdb/db"
	"github.com/chirst/cdb/repl"
)

const fFlagHelp = "Specify the database file name"
const mFlagHelp = "Run the database in memory with no persistence"

func main() {
	dbfName := flag.String("f", "cdb", fFlagHelp)
	isMemory := flag.Bool("m", false, mFlagHelp)
	flag.Parse()
	db, err := db.New(*isMemory, *dbfName)
	if err != nil {
		log.Fatal(err)
	}
	repl.New(db).Run()
}

// References to _databases created by the C interface this is a mapping of
// filename to database instance.
var _databases = make(map[string]*db.DB)

// References to _plans created by the C interface this is a mapping of
// prepareId to prepared statements.
var _plans = make(map[int]*db.PreparedStatement)

// cdb_new_db opens a database with the given filename. A filename of ":memory:"
// will open a database that does not persist data after it is closed. A non
// zero int is returned in case an error occurs. The database can be closed with
// cdb_close_db.
//
//export cdb_new_db
func cdb_new_db(filename *C.char) C.int {
	fng := C.GoString(filename)
	if _, ok := _databases[fng]; ok {
		return C.int(0)
	}
	d, err := db.New(fng == ":memory:", fng)
	_databases[fng] = d
	if err != nil {
		return C.int(1)
	}
	return C.int(0)
}

// cdb_close_db closes the database with the given filename.
//
//export cdb_close_db
func cdb_close_db(filename *C.char) {
	fng := C.GoString(filename)
	delete(_databases, fng)
}

// cdb_prepare prepares a statement that can be bound and executed for the given
// filename and sql. The prepareId is a handle used for further operations on
// the prepared statement. Note the prepared statement must be cleaned up with
// cdb_close_statement.
//
//export cdb_prepare
func cdb_prepare(prepareId *C.int, filename *C.char, sql *C.char) C.int {
	gfn := C.GoString(filename)
	gSql := C.GoString(sql)
	dbi, ok := _databases[gfn]
	if !ok {
		return C.int(1)
	}
	ps, err := dbi.NewPreparedStatement(gSql)
	if err != nil {
		return C.int(1)
	}
	for i := 1; ; i += 1 {
		_, ok := _plans[i]
		if !ok {
			_plans[i] = ps
			*prepareId = C.int(i)
			return C.int(0)
		}
	}
}

// cdb_close_statement cleans up a prepared statement.
//
//export cdb_close_statement
func cdb_close_statement(prepareId C.int) {
	p := int(prepareId)
	delete(_plans, p)
}

// cdb_bind_int binds an int as the next available argument for the given
// prepared statement.
//
//export cdb_bind_int
func cdb_bind_int(prepareId C.int, bound C.int) C.int {
	p, ok := _plans[int(prepareId)]
	if !ok {
		return C.int(1)
	}
	p.Args = append(p.Args, (bound))
	return C.int(0)
}

// cdb_bind_string binds a string as the next available argument for the given
// prepared statement.
//
//export cdb_bind_string
func cdb_bind_string(prepareId C.int, bound *C.char) C.int {
	p, ok := _plans[int(prepareId)]
	if !ok {
		return C.int(1)
	}
	bs := C.GoString(bound)
	p.Args = append(p.Args, bs)
	return C.int(0)
}

// cdb_execute evaluates the given prepared statement.
//
//export cdb_execute
func cdb_execute(prepareId C.int) C.int {
	p, ok := _plans[int(prepareId)]
	if !ok {
		return C.int(1)
	}
	result := p.DB.Execute(p.Statement, p.Args)
	p.Result = &result
	return C.int(0)
}

// cdb_result_err puts 1 in hasError when the statement has an error. The error
// message is put in errMessage.
//
//export cdb_result_err
func cdb_result_err(prepareId C.int, hasError *C.int, errMessage **C.char) C.int {
	*hasError = C.int(0)
	p, ok := _plans[int(prepareId)]
	if !ok {
		return C.int(1)
	}
	if p.Result.Err != nil {
		*hasError = C.int(1)
		*errMessage = C.CString(p.Result.Err.Error())
	}
	return C.int(0)
}

// cdb_result_row moves a cursor to the next row. If there is no row
// cdb_result_row will put 1 into hasRow otherwise 0.
//
//export cdb_result_row
func cdb_result_row(prepareId C.int, hasRow *C.int) C.int {
	*hasRow = C.int(0)
	p, ok := _plans[int(prepareId)]
	if !ok {
		return C.int(1)
	}
	p.ResultIdx += 1
	if p.ResultIdx < len(p.Result.ResultRows) {
		*hasRow = C.int(1)
		return C.int(0)
	}
	return C.int(0)
}

// cdb_result_col_int puts the int for the current row at the 0 based column
// index for the result param.
//
//export cdb_result_col_int
func cdb_result_col_int(prepareId C.int, colIdx C.int, result *C.int) C.int {
	p, ok := _plans[int(prepareId)]
	if !ok {
		return C.int(1)
	}
	r := p.Result.ResultRows[p.ResultIdx][int(colIdx)]
	ri, err := strconv.Atoi(*r)
	if err != nil {
		return C.int(1)
	}
	*result = C.int(ri)
	return C.int(0)
}

// cdb_result_col_string puts the string for the current row at the 0 based
// column index into the result param.
//
//export cdb_result_col_string
func cdb_result_col_string(prepareId C.int, colIdx C.int, result **C.char) C.int {
	p, ok := _plans[int(prepareId)]
	if !ok {
		return C.int(1)
	}
	r := p.Result.ResultRows[p.ResultIdx][int(colIdx)]
	*result = C.CString(*r)
	return C.int(0)
}
