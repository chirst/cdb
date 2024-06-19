package main

import "encoding/json"

type catalog struct {
	schema *schema
}

// TODO could have the kv make a catalog and potentially be the owner of the
// object Catalog would pretty much be one to one in memory representation of
// the schema table. Would be populated on start and refreshed by the vm parse
// schema command.
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

type schema struct {
	tables []tableSchema
}

type tableColumn struct {
	Name    string `json:"name"`
	ColType string `json:"type"`
}

type tableSchema struct {
	Columns []tableColumn `json:"columns"`
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
