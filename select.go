// select generates a query plan for a selectStmt from an AST (Abstract Syntax
// Tree) generated by the parser. This plan is then fed to the vm (Virtual
// Machine) to be ran.
package main

func getSelectPlan(s *selectStmt) *executionPlan {
	commands := map[int]command{
		1: &initCmd{p2: 2},
		2: &integerCmd{p1: 1, p2: 1},
		3: &resultRowCmd{p1: 1, p2: 1},
		4: &haltCmd{},
	}
	return &executionPlan{
		explain:  s.explain,
		commands: commands,
	}
}

type logicalPlan struct {
}

func getLogicalSelectPlan(s *selectStmt) *logicalPlan {
	return &logicalPlan{}
}
