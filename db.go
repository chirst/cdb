// db serves as an interface for the database where raw SQL goes in and
// convenient data structures come out. db is intended to be consumed by things
// like a repl (read eval print loop), a program, or a transport protocol such
// as http.
package main

import "github.com/chirst/cdb/compiler"

type db struct{}

func newDb() *db {
	return &db{}
}

func (*db) execute(sql string) []executeResult {
	l := compiler.NewLexer(sql)
	tokens := l.Lex()
	p := compiler.NewParser(tokens)
	statements, err := p.Parse()
	if err != nil {
		// bail
	}
	var plans []*executionPlan
	for _, s := range statements {
		logicalPlanner := newLogicalPlanner()
		physicalPlanner := newPhysicalPlanner()
		var executionPlan *executionPlan
		if ss, ok := s.(*compiler.SelectStmt); ok {
			lp := logicalPlanner.forSelect(ss)
			executionPlan = physicalPlanner.forSelect(lp, ss.Explain)
		}
		if executionPlan == nil {
			panic("statement not implemented")
		}
		plans = append(plans, executionPlan)
	}
	vm := newVm()
	var executeResults []executeResult
	for _, p := range plans {
		executeResults = append(executeResults, *vm.execute(p))
	}
	return executeResults
}
