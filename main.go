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

//export cdb_prepare
func cdb_prepare(filename *C.char, sql *C.char) C.int {
	gfn := C.GoString(filename)
	gSql := C.GoString(sql)
	// TODO handle err
	ps, _ := _databases[gfn].NewPreparedStatement(gSql)
	for i := 1; ; i += 1 {
		_, ok := _plans[i]
		if !ok {
			_plans[i] = ps
			return C.int(i)
		}
	}
}

//export cdb_close_statement
func cdb_close_statement(prepareId C.int) {
	p := int(prepareId)
	delete(_plans, p)
}

//export cdb_bind_int
func cdb_bind_int(prepareId C.int, bound C.int) {
	p := _plans[int(prepareId)]
	p.Args = append(p.Args, (bound))
}

//export cdb_bind_string
func cdb_bind_string(prepareId C.int, bound *C.char) {
	p := _plans[int(prepareId)]
	bs := C.GoString(bound)
	p.Args = append(p.Args, bs)
}

//export cdb_execute
func cdb_execute(prepareId C.int) {
	p := _plans[int(prepareId)]
	result := p.DB.Execute(p.Statement, p.Args)
	p.Result = &result
}

//export cdb_result_err
func cdb_result_err(prepareId C.int) C.int {
	p := _plans[int(prepareId)]
	if p.Result.Err != nil {
		return C.int(1)
	}
	return C.int(0)
}

//export cdb_result_row
func cdb_result_row(prepareId C.int) C.int {
	p := _plans[int(prepareId)]
	p.ResultIdx += 1
	if p.ResultIdx < len(p.Result.ResultRows) {
		return C.int(1)
	}
	return C.int(0)
}

//export cdb_result_col_int
func cdb_result_col_int(prepareId C.int, colIdx C.int) C.int {
	p := _plans[int(prepareId)]
	r := p.Result.ResultRows[p.ResultIdx][int(colIdx)]
	// TODO handle err
	ri, _ := strconv.Atoi(*r)
	return C.int(ri)
}

//export cdb_result_col_string
func cdb_result_col_string(prepareId C.int, colIdx C.int) *C.char {
	p := _plans[int(prepareId)]
	r := p.Result.ResultRows[p.ResultIdx][int(colIdx)]
	return C.CString(*r)
}
