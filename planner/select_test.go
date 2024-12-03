package planner

import (
	"reflect"
	"testing"

	"github.com/chirst/cdb/compiler"
	"github.com/chirst/cdb/vm"
)

type mockSelectCatalog struct {
	columns              []string
	primaryKeyColumnName string
}

func (m *mockSelectCatalog) GetColumns(s string) ([]string, error) {
	if len(m.columns) == 0 {
		return []string{"id", "name"}, nil
	}
	return m.columns, nil
}

func (*mockSelectCatalog) GetRootPageNumber(s string) (int, error) {
	return 2, nil
}

func (*mockSelectCatalog) GetVersion() string {
	return "v"
}

func (m *mockSelectCatalog) GetPrimaryKeyColumn(tableName string) (string, error) {
	return m.primaryKeyColumnName, nil
}

func TestGetPlan(t *testing.T) {
	expectedCommands := []vm.Command{
		&vm.InitCmd{P2: 1},
		&vm.TransactionCmd{P1: 0},
		&vm.OpenReadCmd{P1: 1, P2: 2},
		&vm.RewindCmd{P1: 1, P2: 9},
		&vm.ColumnCmd{P1: 1, P2: 0, P3: 1},
		&vm.RowIdCmd{P1: 1, P2: 2},
		&vm.ColumnCmd{P1: 1, P2: 1, P3: 3},
		&vm.ResultRowCmd{P1: 1, P2: 3},
		&vm.NextCmd{P1: 1, P2: 4},
		&vm.HaltCmd{},
	}
	ast := &compiler.SelectStmt{
		StmtBase: &compiler.StmtBase{},
		From: &compiler.From{
			TableName: "foo",
		},
		ResultColumns: []compiler.ResultColumn{
			{
				All: true,
			},
		},
	}
	mockCatalog := &mockSelectCatalog{}
	mockCatalog.primaryKeyColumnName = "id"
	mockCatalog.columns = []string{"name", "id", "age"}
	plan, err := NewSelect(mockCatalog, ast).ExecutionPlan()
	if err != nil {
		t.Errorf("expected no err got err %s", err)
	}
	for i, c := range expectedCommands {
		if !reflect.DeepEqual(c, plan.Commands[i]) {
			t.Errorf("got %#v want %#v", plan.Commands[i], c)
		}
	}
}

func TestGetPlanSelectColumn(t *testing.T) {
	expectedCommands := []vm.Command{
		&vm.InitCmd{P2: 1},
		&vm.TransactionCmd{P1: 0},
		&vm.OpenReadCmd{P1: 1, P2: 2},
		&vm.RewindCmd{P1: 1, P2: 7},
		&vm.RowIdCmd{P1: 1, P2: 1},
		&vm.ResultRowCmd{P1: 1, P2: 1},
		&vm.NextCmd{P1: 1, P2: 4},
		&vm.HaltCmd{},
	}
	ast := &compiler.SelectStmt{
		StmtBase: &compiler.StmtBase{},
		From: &compiler.From{
			TableName: "foo",
		},
		ResultColumns: []compiler.ResultColumn{
			{
				Expression: &compiler.ColumnRef{
					Column: "id",
				},
			},
		},
	}
	mockCatalog := &mockSelectCatalog{}
	mockCatalog.primaryKeyColumnName = "id"
	mockCatalog.columns = []string{"name", "id", "age"}
	plan, err := NewSelect(mockCatalog, ast).ExecutionPlan()
	if err != nil {
		t.Errorf("expected no err got err %s", err)
	}
	for i, c := range expectedCommands {
		if !reflect.DeepEqual(c, plan.Commands[i]) {
			t.Errorf("got %#v want %#v", plan.Commands[i], c)
		}
	}
}

func TestGetPlanSelectMultiColumn(t *testing.T) {
	expectedCommands := []vm.Command{
		&vm.InitCmd{P2: 1},
		&vm.TransactionCmd{P1: 0},
		&vm.OpenReadCmd{P1: 1, P2: 2},
		&vm.RewindCmd{P1: 1, P2: 8},
		&vm.RowIdCmd{P1: 1, P2: 1},
		&vm.ColumnCmd{P1: 1, P2: 1, P3: 2},
		&vm.ResultRowCmd{P1: 1, P2: 2},
		&vm.NextCmd{P1: 1, P2: 4},
		&vm.HaltCmd{},
	}
	ast := &compiler.SelectStmt{
		StmtBase: &compiler.StmtBase{},
		From: &compiler.From{
			TableName: "foo",
		},
		ResultColumns: []compiler.ResultColumn{
			{
				Expression: &compiler.ColumnRef{
					Column: "id",
				},
			},
			{
				Expression: &compiler.ColumnRef{
					Column: "age",
				},
			},
		},
	}
	mockCatalog := &mockSelectCatalog{}
	mockCatalog.primaryKeyColumnName = "id"
	mockCatalog.columns = []string{"name", "id", "age"}
	plan, err := NewSelect(mockCatalog, ast).ExecutionPlan()
	if err != nil {
		t.Errorf("expected no err got err %s", err)
	}
	for i, c := range expectedCommands {
		if !reflect.DeepEqual(c, plan.Commands[i]) {
			t.Errorf("got %#v want %#v", plan.Commands[i], c)
		}
	}
}

func TestGetPlanTableAll(t *testing.T) {
	expectedCommands := []vm.Command{
		&vm.InitCmd{P2: 1},
		&vm.TransactionCmd{P1: 0},
		&vm.OpenReadCmd{P1: 1, P2: 2},
		&vm.RewindCmd{P1: 1, P2: 8},
		&vm.RowIdCmd{P1: 1, P2: 1},
		&vm.ColumnCmd{P1: 1, P2: 0, P3: 2},
		&vm.ResultRowCmd{P1: 1, P2: 2},
		&vm.NextCmd{P1: 1, P2: 4},
		&vm.HaltCmd{},
	}
	ast := &compiler.SelectStmt{
		StmtBase: &compiler.StmtBase{},
		From: &compiler.From{
			TableName: "foo",
		},
		ResultColumns: []compiler.ResultColumn{
			{
				AllTable: "foo",
			},
		},
	}
	mockCatalog := &mockSelectCatalog{}
	mockCatalog.primaryKeyColumnName = "id"
	mockCatalog.columns = []string{"id", "name"}
	plan, err := NewSelect(mockCatalog, ast).ExecutionPlan()
	if err != nil {
		t.Errorf("expected no err got err %s", err)
	}
	for i, c := range expectedCommands {
		if !reflect.DeepEqual(c, plan.Commands[i]) {
			t.Errorf("got %#v want %#v", plan.Commands[i], c)
		}
	}
}

func TestGetPlanPKMiddleOrdinal(t *testing.T) {
	expectedCommands := []vm.Command{
		&vm.InitCmd{P2: 1},
		&vm.TransactionCmd{P1: 0},
		&vm.OpenReadCmd{P1: 1, P2: 2},
		&vm.RewindCmd{P1: 1, P2: 8},
		&vm.RowIdCmd{P1: 1, P2: 1},
		&vm.ColumnCmd{P1: 1, P2: 0, P3: 2},
		&vm.ResultRowCmd{P1: 1, P2: 2},
		&vm.NextCmd{P1: 1, P2: 4},
		&vm.HaltCmd{},
	}
	ast := &compiler.SelectStmt{
		StmtBase: &compiler.StmtBase{},
		From: &compiler.From{
			TableName: "foo",
		},
		ResultColumns: []compiler.ResultColumn{
			{
				All: true,
			},
		},
	}
	mockCatalog := &mockSelectCatalog{}
	mockCatalog.primaryKeyColumnName = "id"
	plan, err := NewSelect(mockCatalog, ast).ExecutionPlan()
	if err != nil {
		t.Errorf("expected no err got err %s", err)
	}
	for i, c := range expectedCommands {
		if !reflect.DeepEqual(c, plan.Commands[i]) {
			t.Errorf("got %#v want %#v", plan.Commands[i], c)
		}
	}
}

func TestGetCountAggregate(t *testing.T) {
	expectedCommands := []vm.Command{
		&vm.InitCmd{P2: 1},
		&vm.TransactionCmd{P1: 0},
		&vm.OpenReadCmd{P1: 1, P2: 2},
		&vm.CountCmd{P1: 1, P2: 1},
		&vm.ResultRowCmd{P1: 1, P2: 1},
		&vm.HaltCmd{},
	}
	ast := &compiler.SelectStmt{
		StmtBase: &compiler.StmtBase{},
		From: &compiler.From{
			TableName: "foo",
		},
		ResultColumns: []compiler.ResultColumn{
			{
				Expression: &compiler.FunctionExpr{FnType: compiler.FnCount},
			},
		},
	}
	mockCatalog := &mockSelectCatalog{}
	plan, err := NewSelect(mockCatalog, ast).ExecutionPlan()
	if err != nil {
		t.Errorf("expected no err got err %s", err)
	}
	for i, c := range expectedCommands {
		if !reflect.DeepEqual(c, plan.Commands[i]) {
			t.Errorf("got %#v want %#v", plan.Commands[i], c)
		}
	}
}

func TestGetPlanNoPrimaryKey(t *testing.T) {
	expectedCommands := []vm.Command{
		&vm.InitCmd{P2: 1},
		&vm.TransactionCmd{P1: 0},
		&vm.OpenReadCmd{P1: 1, P2: 2},
		&vm.RewindCmd{P1: 1, P2: 8},
		&vm.ColumnCmd{P1: 1, P2: 0, P3: 1},
		&vm.ColumnCmd{P1: 1, P2: 1, P3: 2},
		&vm.ResultRowCmd{P1: 1, P2: 2},
		&vm.NextCmd{P1: 1, P2: 4},
		&vm.HaltCmd{},
	}
	ast := &compiler.SelectStmt{
		StmtBase: &compiler.StmtBase{},
		From: &compiler.From{
			TableName: "foo",
		},
		ResultColumns: []compiler.ResultColumn{
			{
				All: true,
			},
		},
	}
	mockCatalog := &mockSelectCatalog{}
	plan, err := NewSelect(mockCatalog, ast).ExecutionPlan()
	if err != nil {
		t.Errorf("expected no err got err %s", err)
	}
	for i, c := range expectedCommands {
		if !reflect.DeepEqual(c, plan.Commands[i]) {
			t.Errorf("got %#v want %#v", plan.Commands[i], c)
		}
	}
}
