// Package driver enables cdb to be used with the go database/sql package.
package driver

// TODO there are several context methods that are not implemented.
// TODO transactions statements are not supported.

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"

	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/db"
)

func init() {
	d := new()
	sql.Register("cdb", d)
}

func new() *cdbDriver {
	return &cdbDriver{}
}

type cdbDriver struct{}

// Open implements driver.Driver. Name is the name of the database file. If the
// name is :memory: the database will not use a file and will not persist
// changes.
func (c *cdbDriver) Open(name string) (driver.Conn, error) {
	isMemory := name == ":memory:"
	d, err := db.New(isMemory, name)
	if err != nil {
		return nil, err
	}
	cn := &cdbConn{
		cdb: d,
	}
	return cn, nil
}

type cdbConn struct {
	cdb *db.DB
}

// Begin implements driver.Conn.
func (c *cdbConn) Begin() (driver.Tx, error) {
	panic("Transactions not implemented")
}

// Close implements driver.Conn.
func (c *cdbConn) Close() error {
	return nil
}

// Prepare implements driver.Conn.
func (c *cdbConn) Prepare(query string) (driver.Stmt, error) {
	statements := c.cdb.Tokenize(query)
	if len(statements) != 1 {
		return nil, errors.New("driver supports only one statement at a time")
	}
	return &cdbStmt{
		cdb:       c.cdb,
		query:     query,
		statement: statements[0],
	}, nil
}

type cdbStmt struct {
	cdb       *db.DB
	query     string
	statement compiler.Statement
}

// Close implements driver.Stmt.
func (c *cdbStmt) Close() error {
	return nil
}

// Exec implements driver.Stmt.
func (c *cdbStmt) Exec(args []driver.Value) (driver.Result, error) {
	result := c.cdb.Execute(c.statement, toAny(args))
	if result.Err != nil {
		return nil, result.Err
	}
	cr := &cdbResult{}
	return cr, nil
}

// NumInput implements driver.Stmt.
func (c *cdbStmt) NumInput() int {
	// Per driver.Stmt docs a -1 means the driver will skip a sanity check for
	// the number of arguments prepared vs passed to be executed. This sanity
	// check could be supported, but would mean the prepare statement would have
	// to start returning the number of parameters.
	return -1
}

// Query implements driver.Stmt.
func (c *cdbStmt) Query(args []driver.Value) (driver.Rows, error) {
	result := c.cdb.Execute(c.statement, toAny(args))
	if result.Err != nil {
		return nil, result.Err
	}
	cr := &cdbRows{
		cols: result.ResultHeader,
		rows: result.ResultRows,
	}
	return cr, nil
}

func toAny(args []driver.Value) []any {
	aarg := []any{}
	for _, arg := range args {
		aarg = append(aarg, arg)
	}
	return aarg
}

type cdbResult struct{}

// LastInsertId implements driver.Result.
func (c *cdbResult) LastInsertId() (int64, error) {
	return 0, nil
}

// RowsAffected implements driver.Result.
func (c *cdbResult) RowsAffected() (int64, error) {
	return 0, nil
}

type cdbRows struct {
	cols   []string
	rows   [][]*string
	rowIdx int
}

// Close implements driver.Rows.
func (c *cdbRows) Close() error {
	return nil
}

// Columns implements driver.Rows.
func (c *cdbRows) Columns() []string {
	return c.cols
}

// Next implements driver.Rows.
func (c *cdbRows) Next(dest []driver.Value) error {
	if c.rowIdx == len(c.rows) {
		return io.EOF
	}
	for i, v := range c.rows[c.rowIdx] {
		// TODO the value is a string pointer, but might be better as a typed
		// value. It is a string pointer so it can be null.
		dest[i] = *v
	}
	c.rowIdx += 1
	return nil
}
