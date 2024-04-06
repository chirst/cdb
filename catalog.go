package main

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
