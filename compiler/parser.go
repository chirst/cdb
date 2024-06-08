// parser takes tokens from the lexer and produces an AST (Abstract Syntax
// Tree). The AST is consumed to make a query plan ran by the vm (Virtual
// Machine).
package compiler

import (
	"fmt"
)

type parser struct {
	tokens []token
	start  int
	end    int
}

func NewParser(tokens []token) *parser {
	return &parser{tokens: tokens}
}

func (p *parser) Parse() (Stmt, error) {
	return p.parseStmt()
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
	case "CREATE":
		return p.parseCreate(sb)
	case "INSERT":
		return p.parseInsert(sb)
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
	stmt.ResultColumn = ResultColumn{
		All: r.value == "*",
	}

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

func (p *parser) parseCreate(sb *StmtBase) (*CreateStmt, error) {
	stmt := &CreateStmt{StmtBase: sb}
	if p.tokens[p.end].value != "CREATE" {
		return nil, fmt.Errorf("unexpected token %s", p.tokens[p.end].value)
	}
	t := p.nextNonSpace()
	if t.value != "TABLE" {
		return nil, fmt.Errorf("unexpected token %s", p.tokens[p.end].value)
	}
	tn := p.nextNonSpace()
	if tn.tokenType != IDENTIFIER {
		return nil, fmt.Errorf("expected identifier")
	}
	stmt.TableName = tn.value
	lp := p.nextNonSpace()
	if lp.value != "(" {
		return nil, fmt.Errorf("unexpected token %s", p.tokens[p.end].value)
	}
	stmt.ColDefs = []ColDef{}
	for {
		colName := p.nextNonSpace()
		if colName.tokenType != IDENTIFIER {
			return nil, fmt.Errorf("expected identifier")
		}
		colType := p.nextNonSpace()
		if colType.value != "INTEGER" && colType.value != "TEXT" {
			return nil, fmt.Errorf("expected column type")
		}
		stmt.ColDefs = append(stmt.ColDefs, ColDef{
			ColName: colName.value,
			ColType: colType.value,
		})
		sep := p.nextNonSpace()
		if sep.value != "," {
			if sep.value == ")" {
				break
			} else {
				return nil, fmt.Errorf("unexpected token %s", p.tokens[p.end].value)
			}
		}
	}
	return stmt, nil
}

func (p *parser) parseInsert(sb *StmtBase) (*InsertStmt, error) {
	stmt := &InsertStmt{StmtBase: sb}
	if p.tokens[p.end].value != "INSERT" {
		return nil, fmt.Errorf("unexpected token %s", p.tokens[p.end].value)
	}
	if p.nextNonSpace().value != "INTO" {
		return nil, fmt.Errorf("unexpected token %s", p.tokens[p.end].value)
	}
	tn := p.nextNonSpace()
	if tn.tokenType != IDENTIFIER {
		return nil, fmt.Errorf("expected identifier")
	}
	stmt.TableName = tn.value
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
