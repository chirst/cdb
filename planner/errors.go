package planner

import "errors"

var (
	errInvalidPKColumnType = errors.New("primary key must be INTEGER type")
	errTableExists         = errors.New("table exists")
	errMoreThanOnePK       = errors.New("more than one primary key specified")
	errTableNotExist       = errors.New("table does not exist")
	errValuesNotMatch      = errors.New("values list did not match columns list")
	errMissingColumnName   = errors.New("missing column")
	errSetColumnNotExist   = errors.New("set column not part of table")
)
