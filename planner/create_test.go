package planner

import (
	"errors"
	"reflect"
	"testing"

	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/kv"
	"github.com/chirst/cdb/vm"
)

type mockCreateCatalog struct {
	tableExistsRes bool
}

func (*mockCreateCatalog) GetColumns(tableOrIndexName string) ([]string, error) {
	return []string{}, nil
}

func (*mockCreateCatalog) GetRootPageNumber(tableOrIndexName string) (int, error) {
	return 2, nil
}

func (m *mockCreateCatalog) TableExists(tableName string) bool {
	return m.tableExistsRes
}

func (*mockCreateCatalog) GetVersion() string {
	return "v"
}

func TestCreateWithNoIDColumn(t *testing.T) {
	stmt := &compiler.CreateStmt{
		StmtBase:  &compiler.StmtBase{},
		TableName: "foo",
		ColDefs: []compiler.ColDef{
			{
				ColName: "first",
				ColType: "TEXT",
			},
		},
	}
	mc := &mockCreateCatalog{}
	expectedSchema := &kv.TableSchema{
		Columns: []kv.TableColumn{
			{
				Name:    "first",
				ColType: "TEXT",
			},
		},
	}
	expectedJSONSchema, err := expectedSchema.ToJSON()
	if err != nil {
		t.Fatalf("failed to convert expected schema to json %s", err.Error())
	}
	expectedCommands := []vm.Command{
		&vm.InitCmd{P2: 1},
		&vm.TransactionCmd{P2: 1},
		&vm.CreateBTreeCmd{P2: 1},
		&vm.OpenWriteCmd{P1: 1, P2: 1},
		&vm.NewRowIdCmd{P1: 1, P2: 2},
		&vm.StringCmd{P1: 3, P4: "table"},
		&vm.StringCmd{P1: 4, P4: "foo"},
		&vm.StringCmd{P1: 5, P4: "foo"},
		&vm.CopyCmd{P1: 1, P2: 6},
		&vm.StringCmd{P1: 7, P4: string(expectedJSONSchema)},
		&vm.MakeRecordCmd{P1: 3, P2: 5, P3: 8},
		&vm.InsertCmd{P1: 1, P2: 8, P3: 2},
		&vm.ParseSchemaCmd{},
		&vm.HaltCmd{},
	}
	plan, err := NewCreate(mc, stmt).ExecutionPlan()
	if err != nil {
		t.Fatal(err.Error())
	}
	for i, c := range expectedCommands {
		if !reflect.DeepEqual(c, plan.Commands[i]) {
			t.Errorf("got %#v want %#v", plan.Commands[i], c)
		}
	}
}

func TestCreateWithAlternateNamedIDColumn(t *testing.T) {
	stmt := &compiler.CreateStmt{
		StmtBase:  &compiler.StmtBase{},
		TableName: "foo",
		ColDefs: []compiler.ColDef{
			{
				ColName: "ID",
				ColType: "INTEGER",
			},
			{
				ColName: "first",
				ColType: "TEXT",
			},
		},
	}
	mc := &mockCreateCatalog{}
	expectedSchema := &kv.TableSchema{
		Columns: []kv.TableColumn{
			{
				Name:    "ID",
				ColType: "INTEGER",
			},
			{
				Name:    "first",
				ColType: "TEXT",
			},
		},
	}
	expectedJSONSchema, err := expectedSchema.ToJSON()
	if err != nil {
		t.Fatalf("failed to convert expected schema to json %s", err.Error())
	}
	expectedCommands := []vm.Command{
		&vm.InitCmd{P2: 1},
		&vm.TransactionCmd{P2: 1},
		&vm.CreateBTreeCmd{P2: 1},
		&vm.OpenWriteCmd{P1: 1, P2: 1},
		&vm.NewRowIdCmd{P1: 1, P2: 2},
		&vm.StringCmd{P1: 3, P4: "table"},
		&vm.StringCmd{P1: 4, P4: "foo"},
		&vm.StringCmd{P1: 5, P4: "foo"},
		&vm.CopyCmd{P1: 1, P2: 6},
		&vm.StringCmd{P1: 7, P4: string(expectedJSONSchema)},
		&vm.MakeRecordCmd{P1: 3, P2: 5, P3: 8},
		&vm.InsertCmd{P1: 1, P2: 8, P3: 2},
		&vm.ParseSchemaCmd{},
		&vm.HaltCmd{},
	}
	plan, err := NewCreate(mc, stmt).ExecutionPlan()
	if err != nil {
		t.Fatal(err.Error())
	}
	for i, c := range expectedCommands {
		if !reflect.DeepEqual(c, plan.Commands[i]) {
			t.Errorf("got %#v want %#v", plan.Commands[i], c)
		}
	}
}

func TestCreatePrimaryKeyWithTextType(t *testing.T) {
	stmt := &compiler.CreateStmt{
		StmtBase:  &compiler.StmtBase{},
		TableName: "foo",
		ColDefs: []compiler.ColDef{
			{
				ColName:    "ID",
				ColType:    "TEXT",
				PrimaryKey: true,
			},
		},
	}
	mc := &mockCreateCatalog{}
	_, err := NewCreate(mc, stmt).ExecutionPlan()
	if !errors.Is(err, errInvalidPKColumnType) {
		t.Fatalf("got error %s expected error %s", err, errInvalidPKColumnType)
	}
}

func TestCreateWithExistingTable(t *testing.T) {
	stmt := &compiler.CreateStmt{
		StmtBase:  &compiler.StmtBase{},
		TableName: "foo",
		ColDefs: []compiler.ColDef{
			{
				ColName: "id",
				ColType: "INTEGER",
			},
		},
	}
	mc := &mockCreateCatalog{tableExistsRes: true}
	_, err := NewCreate(mc, stmt).ExecutionPlan()
	if !errors.Is(err, errTableExists) {
		t.Fatalf("got error %s expected error %s", err, errTableExists)
	}
}

func TestCreateWithMoreThanOnePrimaryKey(t *testing.T) {
	stmt := &compiler.CreateStmt{
		StmtBase:  &compiler.StmtBase{},
		TableName: "foo",
		ColDefs: []compiler.ColDef{
			{
				ColName:    "bar",
				ColType:    "INTEGER",
				PrimaryKey: true,
			},
			{
				ColName:    "baz",
				ColType:    "INTEGER",
				PrimaryKey: true,
			},
		},
	}
	mc := &mockCreateCatalog{}
	_, err := NewCreate(mc, stmt).ExecutionPlan()
	if !errors.Is(err, errMoreThanOnePK) {
		t.Fatalf("got error %s expected error %s", err, errMoreThanOnePK)
	}
}
