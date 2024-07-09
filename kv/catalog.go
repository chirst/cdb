package kv

import (
	"encoding/json"
	"fmt"
	"slices"
)

// catalog holds information about the database schema
type catalog struct {
	schema *schema
}

func newCatalog() *catalog {
	return &catalog{
		schema: &schema{},
	}
}

func (c *catalog) GetRootPageNumber(tableOrIndexName string) (int, error) {
	if tableOrIndexName == "cdb_schema" {
		return 1, nil
	}
	for _, o := range c.schema.objects {
		if o.name == tableOrIndexName {
			return o.rootPageNumber, nil
		}
	}
	return 0, fmt.Errorf("cannot get root of %s", tableOrIndexName)
}

func (c *catalog) GetColumns(tableName string) ([]string, error) {
	if tableName == "cdb_schema" {
		return []string{"id", "type", "name", "table_name", "rootpage", "sql"}, nil
	}
	for _, o := range c.schema.objects {
		if o.name == tableName && o.tableName == tableName {
			ts := TableSchemaFromString(o.jsonSchema)
			ret := []string{}
			for _, c := range ts.Columns {
				ret = append(ret, c.Name)
			}
			return ret, nil
		}
	}
	return nil, fmt.Errorf("cannot get columns for table %s", tableName)
}

func (c *catalog) TableExists(tableName string) bool {
	return slices.ContainsFunc(c.schema.objects, func(o object) bool {
		return o.objectType == "table" && o.tableName == tableName
	})
}

// schema is a cached representation of the database schema
type schema struct {
	// objects are a in memory representation of the schema table
	objects []object
}

type object struct {
	// objectType is something like table, index, or trigger.
	objectType string
	// name is the name of the object
	name string
	// tableName is the name of the table this object is associated with
	tableName string
	// rootPageNumber is the root page number of the table or index
	rootPageNumber int
	// jsonSchema is different for each object. For a table it is tableSchema
	jsonSchema string
}

type TableSchema struct {
	Columns []TableColumn `json:"columns"`
}

type TableColumn struct {
	Name    string `json:"name"`
	ColType string `json:"type"`
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
