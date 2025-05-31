package main

import "C"
import "github.com/chirst/cdb/db"

// References to _databases created by the C interface this is a mapping of
// filename to database instance.
var _databases = make(map[string]*db.DB)
var _plans = make(map[int]string)

//export cdb_new_db
func cdb_new_db(filename *C.char) {
	fng := C.GoString(filename)
	if _databases[fng] != nil {
		return
	}
	// TODO handle err
	_databases[fng], _ = db.New(fng == ":memory:", fng)
}

//export cdb_close_db
func cdb_close_db(filename *C.char) {}

//export cdb_prepare
func cdb_prepare(filename *C.char, sql *C.char) C.int {
	goInt := 1
	return C.int(goInt)
}

//export cdb_bind_int
func cdb_bind_int(prepareId C.int) {}

//export cdb_execute
func cdb_execute(prepareId C.int) {}
