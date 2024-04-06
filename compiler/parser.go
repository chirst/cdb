// parser takes tokens from the lexer and produces an AST (Abstract Syntax
// Tree). The AST is consumed to make a query plan ran by the vm (Virtual
// Machine).
package compiler

import (
	"fmt"
	"strconv"
)

type parser struct {
	tokens []token
	start  int
	end    int
}

func NewParser(tokens []token) *parser {
	return &parser{tokens: tokens}
}

func (p *parser) Parse() (StmtList, error) {
	ret := StmtList{}
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

func (p *parser) parseStmt() (Stmt, error) {
	t := p.tokens[p.start]
	sb := &StmtBase{}
	if t.value == "EXPLAIN" {
		sb.Explain = true
		t = p.nextNonSpace()
	}
	switch t.value {
	case "SELECT":
		return p.parseSelect(sb)
	}
	return nil, fmt.Errorf("unexpected token %s", t.value)
}

func (p *parser) parseSelect(sb *StmtBase) (*SelectStmt, error) {
	stmt := &SelectStmt{StmtBase: sb}
	if p.tokens[p.end].value != "SELECT" {
		return nil, fmt.Errorf("unexpected token %s", p.tokens[p.end].value)
	}
	r := p.nextNonSpace()
	if r.tokenType != PUNCTUATOR && r.tokenType != LITERAL {
		return nil, fmt.Errorf("unexpected token %s", r.value)
	}
	resultCol := ResultColumn{
		All: r.value == "*",
	}
	if r.tokenType == LITERAL {
		numericLiteral, err := strconv.Atoi(r.value)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %s to numeric literal", r.value)
		}
		resultCol.Expr = &Expr{
			Literal: &Literal{
				NumericLiteral: numericLiteral,
			},
		}
	}
	stmt.ResultColumns = append(stmt.ResultColumns, resultCol)

	f := p.nextNonSpace()
	if f.tokenType == EOF || f.value == ";" {
		return stmt, nil
	}
	if f.tokenType != KEYWORD || f.value != "FROM" {
		return nil, fmt.Errorf("unexpected token %s", f.value)
	}

	t := p.nextNonSpace()
	if t.tokenType != IDENTIFIER {
		return nil, fmt.Errorf("unexpected token %s", t.value)
	}
	stmt.From = &From{
		TableName: t.value,
	}
	return stmt, nil
}

func (p *parser) nextNonSpace() token {
	p.end = p.end + 1
	if p.end > len(p.tokens)-1 {
		return token{EOF, ""}
	}
	for p.tokens[p.end].tokenType == WHITESPACE {
		p.end = p.end + 1
		if p.end > len(p.tokens)-1 {
			return token{EOF, ""}
		}
	}
	return p.tokens[p.end]
}

func (p *parser) peekNextNonSpace() token {
	tmpEnd := p.end
	if tmpEnd > len(p.tokens)-1 {
		return token{EOF, ""}
	}
	for p.tokens[tmpEnd].tokenType == WHITESPACE {
		tmpEnd = tmpEnd + 1
		if tmpEnd > len(p.tokens)-1 {
			return token{EOF, ""}
		}
	}
	return p.tokens[tmpEnd]
}
