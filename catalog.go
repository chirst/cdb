package main

import "encoding/json"

type catalog struct{}

func newCatalog() *catalog {
	return &catalog{}
}

func (*catalog) getPageNumber(tableOrIndexName string) (int, error) {
	return 2, nil
}

func (*catalog) getColumns(tableOrIndexName string) ([]string, error) {
	return []string{"id", "name"}, nil
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
