package main

import "encoding/json"

// catalog holds information about the database schema
type catalog struct {
	schema *schema
}

func newCatalog() *catalog {
	return &catalog{
		schema: &schema{},
	}
}

func (*catalog) getPageNumber(tableOrIndexName string) (int, error) {
	return 2, nil
}

func (*catalog) getColumns(tableOrIndexName string) ([]string, error) {
	return []string{"id", "name"}, nil
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

type tableSchema struct {
	Columns []tableColumn `json:"columns"`
}

type tableColumn struct {
	Name    string `json:"name"`
	ColType string `json:"type"`
}

func (ts *tableSchema) ToJSON() ([]byte, error) {
	j, err := json.Marshal(ts)
	if err != nil {
		return []byte{}, err
	}
	return j, nil
}

func (ts *tableSchema) FromJSON(j []byte) error {
	return json.Unmarshal(j, ts)
}
