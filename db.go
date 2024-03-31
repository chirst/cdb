// db serves as an interface for the database where raw SQL goes in and
// convenient data structures come out. db is intended to be consumed by things
// like a repl (read eval print loop), a program, or a transport protocol such
// as http.
package main

import (
	"fmt"
)

type db struct{}

func newDb() *db {
	return &db{}
}

type executeResult struct{}

func (*db) execute(sql string) []executeResult {
	l := newLexer(sql)
	tokens := l.lex()
	p := newParser(tokens)
	statements, err := p.parse()
	if err != nil {
		// bail
	}
	var plans []map[int]command
	for _, s := range statements {
		p, err := getPlanForStatement(s)
		if err != nil {
			// bail
		}
		plans = append(plans, p)
	}
	var executeResults []executeResult
	for _, p := range plans {
		execute(p)
		// executeResults = append(executeResults, execute(p))
	}
	return executeResults
}

func getPlanForStatement(s any) (map[int]command, error) {
	if selectS, ok := s.(selectStmt); ok {
		return getPlanFor(selectS), nil
	}
	return nil, fmt.Errorf("unexpected type %v", s)
}
