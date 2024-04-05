// db serves as an interface for the database where raw SQL goes in and
// convenient data structures come out. db is intended to be consumed by things
// like a repl (read eval print loop), a program, or a transport protocol such
// as http.
package main

type db struct{}

func newDb() *db {
	return &db{}
}

func (*db) execute(sql string) []executeResult {
	l := newLexer(sql)
	tokens := l.lex()
	p := newParser(tokens)
	statements, err := p.parse()
	if err != nil {
		// bail
	}
	var plans []executionPlan
	for _, s := range statements {
		var p *executionPlan
		if ss, ok := s.(*selectStmt); ok {
			p = getSelectPlan(ss)
		}
		if p == nil {
			panic("statement not implemented")
		}
		plans = append(plans, *p)
	}
	var executeResults []executeResult
	for _, p := range plans {
		executeResults = append(executeResults, run(p))
	}
	return executeResults
}
