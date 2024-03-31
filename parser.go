// parser takes tokens from the lexer and produces an AST (Abstract Syntax
// Tree). The AST is consumed to make a query plan ran by the vm (Virtual
// Machine).
package main

import "fmt"

type parser struct {
	tokens []token
	start  int
	end    int
}

func (p *parser) parse() (stmtList, error) {
	ret := stmtList{}
	for {
		r, err := p.parseStmt()
		if err != nil {
			return nil, err
		}
		ret = append(ret, r)
		p.start = p.end
		if len(p.tokens)-1 <= p.end {
			return ret, nil
		}
	}
}

func (p *parser) parseStmt() (any, error) {
	t := p.tokens[p.start]
	switch t.value {
	case "SELECT":
		return p.parseSelect()
	}
	return nil, fmt.Errorf("unexpected token %s", t.value)
}

func (p *parser) parseSelect() (*selectStmt, error) {
	stmt := &selectStmt{}
	if p.tokens[p.end].value != "SELECT" {
		return nil, fmt.Errorf("unexpected token %s", p.tokens[p.end].value)
	}
	r := p.nextNonSpace()
	if r.tokenType != PUNCTUATOR {
		return nil, fmt.Errorf("unexpected token %s", r.value)
	}
	stmt.resultColumns = append(stmt.resultColumns, resultColumn{
		all: true,
	})

	f := p.nextNonSpace()
	if f.tokenType != KEYWORD || f.value != "FROM" {
		return nil, fmt.Errorf("unexpected token %s", f.value)
	}

	t := p.nextNonSpace()
	if t.tokenType != IDENTIFIER {
		return nil, fmt.Errorf("unexpected token %s", t.value)
	}
	stmt.from = &tableOrSubQuery{
		tableName: t.value,
	}
	return stmt, nil
}

func (p *parser) nextNonSpace() token {
	p.end = p.end + 1
	for p.tokens[p.end].tokenType == WHITESPACE {
		p.end = p.end + 1
	}
	return p.tokens[p.end]
}

func (p *parser) peekNextNonSpace() token {
	tmpEnd := p.end
	for p.tokens[tmpEnd].tokenType == WHITESPACE {
		tmpEnd = tmpEnd + 1
	}
	return p.tokens[tmpEnd]
}
