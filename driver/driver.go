// Package driver enables cdb to be used with the go database/sql package.
package driver

// TODO
// - Question what the prepare step should do.
// - Think about making database return typed response instead of all strings.
// - Implement and test half finished methods.
// - Consider context methods.

import (
	"database/sql"
	"database/sql/driver"
	"io"

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
	st := &cdbStmt{
		cdb:   c.cdb,
		query: query,
	}
	return st, nil
}

type cdbStmt struct {
	cdb   *db.DB
	query string
}

// Close implements driver.Stmt.
func (c *cdbStmt) Close() error {
	return nil
}

// Exec implements driver.Stmt.
func (c *cdbStmt) Exec(args []driver.Value) (driver.Result, error) {
	result := c.cdb.Execute(c.query)
	if result.Err != nil {
		return nil, result.Err
	}
	cr := &cdbResult{}
	return cr, nil
}

// NumInput implements driver.Stmt.
func (c *cdbStmt) NumInput() int {
	return 0
}

// Query implements driver.Stmt.
func (c *cdbStmt) Query(args []driver.Value) (driver.Rows, error) {
	result := c.cdb.Execute(c.query)
	if result.Err != nil {
		return nil, result.Err
	}
	cr := &cdbRows{
		cols: result.ResultHeader,
		rows: result.ResultRows,
	}
	return cr, nil
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
		dest[i] = *v
	}
	c.rowIdx += 1
	return nil
}
