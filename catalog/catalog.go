package catalog

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"slices"
)

// CT prefixed types correspond to cdb types and serve as the ID in CdbType. The
// CT types precedence works off the number of the type. Where a higher number
// is a higher precedence.
const (
	CTUnknown = iota
	CTInt
	CTVar
	CTStr
)

// CdbType represents a type in cdb.
type CdbType struct {
	// ID corresponds to CT prefixed types CTUnknown, CTInt, CTVar, CTStr...
	ID int
	// VarPosition is special information for CTVar allowing the type to be
	// resolved in the virtual machine since a lone variable cannot be resolved
	// in the planner.
	VarPosition int
}

// TODO remove early exits to each function and blend cdb_schema as any other
// object.
// TODO need to look at encapsulation.

// Catalog holds information about the database schema
type Catalog struct {
	schema *schema
	// version handles concurrency control when the planner prepares statements.
	// Statements being run by the virtual machine will have their version
	// checked with current catalog when the executing statement acquires it's
	// file lock. If the version is out of date the statement will roll back,
	// be recompiled, and be re-executed.
	version string
}

func NewCatalog() *Catalog {
	c := &Catalog{
		schema: &schema{},
	}
	c.setNewVersion()
	return c
}

func (c *Catalog) GetRootPageNumber(tableOrIndexName string) (int, error) {
	if tableOrIndexName == "cdb_schema" {
		return 1, nil
	}
	for _, o := range c.schema.objects {
		if o.Name == tableOrIndexName {
			return o.RootPageNumber, nil
		}
	}
	return 0, fmt.Errorf("cannot get root of %s", tableOrIndexName)
}

func (c *Catalog) GetColumns(tableName string) ([]string, error) {
	if tableName == "cdb_schema" {
		return []string{"id", "type", "name", "table_name", "rootpage", "sql"}, nil
	}
	for _, o := range c.schema.objects {
		if o.Name == tableName && o.TableName == tableName {
			ts := TableSchemaFromString(o.JsonSchema)
			ret := []string{}
			for _, c := range ts.Columns {
				ret = append(ret, c.Name)
			}
			return ret, nil
		}
	}
	return nil, fmt.Errorf("cannot get columns for table %s", tableName)
}

func (c *Catalog) GetPrimaryKeyColumn(tableName string) (string, error) {
	if tableName == "cdb_schema" {
		return "id", nil
	}
	for _, o := range c.schema.objects {
		if o.Name == tableName && o.TableName == tableName {
			ts := TableSchemaFromString(o.JsonSchema)
			for _, col := range ts.Columns {
				if col.PrimaryKey {
					return col.Name, nil
				}
			}
			// Table has no PK
			return "", nil
		}
	}
	return "", fmt.Errorf("cannot get pk for table %s", tableName)
}

func (c *Catalog) TableExists(tableName string) bool {
	if tableName == "cdb_schema" {
		return true
	}
	return slices.ContainsFunc(c.schema.objects, func(o Object) bool {
		return o.ObjectType == "table" && o.TableName == tableName
	})
}

func (c *Catalog) GetColumnType(tableName string, columnName string) (CdbType, error) {
	if tableName == "cdb_schema" {
		switch columnName {
		case "id":
			return CdbType{ID: CTInt}, nil
		case "type":
			return CdbType{ID: CTStr}, nil
		case "name":
			return CdbType{ID: CTStr}, nil
		case "table_name":
			return CdbType{ID: CTStr}, nil
		case "rootpage":
			return CdbType{ID: CTInt}, nil
		case "sql":
			return CdbType{ID: CTStr}, nil
		}
		return CdbType{ID: CTUnknown}, fmt.Errorf("no type for table %s col %s", tableName, columnName)
	}

	for _, o := range c.schema.objects {
		if o.Name == tableName && o.TableName == tableName {
			ts := TableSchemaFromString(o.JsonSchema)
			for _, col := range ts.Columns {
				if col.Name == columnName {
					switch col.ColType {
					case "INTEGER":
						return CdbType{ID: CTInt}, nil
					case "TEXT":
						return CdbType{ID: CTStr}, nil
					default:
						return CdbType{ID: CTUnknown}, fmt.Errorf("no type for %s", col.ColType)
					}
				}
			}
		}
	}
	return CdbType{ID: CTUnknown}, fmt.Errorf("no type for table %s col %s", tableName, columnName)
}

// GetVersion returns a unique version identifier that is updated when the
// catalog is updated.
func (c *Catalog) GetVersion() string {
	return c.version
}

func (c *Catalog) SetSchema(o []Object) {
	c.schema.objects = o
	c.setNewVersion()
}

func (c *Catalog) setNewVersion() {
	chars := "abcdefghijklmnopqrstuvwxyz"
	v := make([]byte, 16)
	for i := range v {
		v[i] = chars[rand.Intn(len(chars))]
	}
	c.version = string(v)
}

// schema is a cached representation of the database schema
type schema struct {
	// objects are a in memory representation of the schema table
	objects []Object
}

type Object struct {
	// ObjectType is something like table, index, or trigger.
	ObjectType string `json:"objectType"`
	// Name is the Name of the object
	Name string `json:"name"`
	// TableName is the name of the table this object is associated with
	TableName string `json:"tableName"`
	// RootPageNumber is the root page number of the table or index
	RootPageNumber int `json:"rootPageNumber"`
	// JsonSchema is different for each object. For a table it is tableSchema
	JsonSchema string `json:"jsonSchema"`
}

type TableSchema struct {
	Columns []TableColumn `json:"columns"`
}

type TableColumn struct {
	Name       string `json:"name"`
	ColType    string `json:"type"`
	PrimaryKey bool   `json:"primaryKey"`
}

func (ts *TableSchema) ToJSON() ([]byte, error) {
	j, err := json.Marshal(ts)
	if err != nil {
		return []byte{}, err
	}
	return j, nil
}

func (ts *TableSchema) FromJSON(j []byte) error {
	return json.Unmarshal(j, ts)
}

func TableSchemaFromString(s string) *TableSchema {
	v := &TableSchema{}
	json.Unmarshal([]byte(s), &v)
	return v
}
