package compiler

// parser takes tokens from the lexer and produces an AST (Abstract Syntax
// Tree). The AST is consumed to make a query plan ran by the vm (Virtual
// Machine).

import (
	"errors"
	"fmt"
	"strconv"
)

const (
	tokenErr   = "unexpected token %s"
	identErr   = "expected identifier but got %s"
	columnErr  = "expected column type but got %s"
	literalErr = "expected literal but got %s"
)

type parser struct {
	tokens []Token
	start  int
	end    int
}

func NewParser(tokens []Token) *parser {
	return &parser{tokens: tokens}
}

func (p *parser) Parse() (Stmt, error) {
	return p.parseStmt()
}

func (p *parser) parseStmt() (Stmt, error) {
	t := p.tokens[p.start]
	for {
		if t.TokenType != TkWhitespace {
			break
		}
		p.end = p.end + 1
		t = p.tokens[p.end]
	}
	sb := &StmtBase{}
	if t.Value == kwExplain {
		nv := p.nextNonSpace()
		if nv.Value == kwQuery {
			tp := p.nextNonSpace()
			if tp.Value == kwPlan {
				sb.ExplainQueryPlan = true
				t = p.nextNonSpace()
			} else {
				return nil, fmt.Errorf(tokenErr, p.tokens[p.end].Value)
			}
		} else {
			sb.Explain = true
			t = nv
		}
	}
	switch t.Value {
	case kwSelect:
		return p.parseSelect(sb)
	case kwCreate:
		return p.parseCreate(sb)
	case kwInsert:
		return p.parseInsert(sb)
	}
	return nil, fmt.Errorf(tokenErr, t.Value)
}

func (p *parser) parseSelect(sb *StmtBase) (*SelectStmt, error) {
	stmt := &SelectStmt{StmtBase: sb}
	if p.tokens[p.end].Value != kwSelect {
		return nil, fmt.Errorf(tokenErr, p.tokens[p.end].Value)
	}
	for {
		resultColumn, err := p.parseResultColumn()
		if err != nil {
			return nil, err
		}
		stmt.ResultColumns = append(stmt.ResultColumns, *resultColumn)
		n := p.peekNextNonSpace()
		if n.Value != "," {
			break
		}
		p.nextNonSpace()
	}
	f := p.nextNonSpace()
	if f.TokenType == tkEOF || f.Value == ";" {
		return stmt, nil
	}
	w := f
	if f.Value == kwFrom {
		t := p.nextNonSpace()
		if t.TokenType != tkIdentifier {
			return nil, fmt.Errorf(tokenErr, t.Value)
		}
		stmt.From = &From{
			TableName: t.Value,
		}
		w = p.nextNonSpace()
	}

	if w.TokenType == tkEOF || w.Value == ";" {
		return stmt, nil
	}
	if w.Value == kwWhere {
		exp, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}
		stmt.Where = exp
	}
	return stmt, nil
}

// parseResultColumn parses a single result column
func (p *parser) parseResultColumn() (*ResultColumn, error) {
	resultColumn := &ResultColumn{}
	r := p.nextNonSpace()
	// There are three cases to handle here.
	// 1. *
	// 2. tableName.*
	// 3. expression AS alias
	// We simply try and identify the first two then fall into expression
	// parsing if the first two cases are not present. This is a smart way to do
	// things since expressions are not limited to result columns.
	if r.Value == "*" {
		resultColumn.All = true
		return resultColumn, nil
	} else if r.TokenType == tkIdentifier {
		if p.peekNextNonSpace().Value == "." {
			if p.peekNonSpaceBy(2).Value == "*" {
				p.nextNonSpace() // move to .
				p.nextNonSpace() // move to *
				resultColumn.AllTable = r.Value
				return resultColumn, nil
			}
		}
	}
	p.rewind()
	expr, err := p.parseExpression(0)
	if err != nil {
		return nil, err
	}
	resultColumn.Expression = expr
	err = p.parseAlias(resultColumn)
	return resultColumn, err
}

// Vaughan Pratt's top down operator precedence parsing algorithm.
// Definitions:
//   - Left binding power (LBP) an integer representing operator precedence level.
//   - Null denotation (NUD) nothing to it's left (prefix).
//   - Left denotation (LED) something to it's left (infix).
//   - Right binding power (RBP) parse prefix operator then iteratively parse
//     infix expressions.
//
// Begin with rbp 0
func (p *parser) parseExpression(rbp int) (Expr, error) {
	left, err := p.getOperand()
	if err != nil {
		return nil, err
	}
	for {
		nextToken := p.peekNextNonSpace()
		if nextToken.TokenType != tkOperator {
			return left, nil
		}
		lbp := opPrecedence[nextToken.Value]
		if lbp <= rbp {
			return left, nil
		}
		p.nextNonSpace()
		right, err := p.parseExpression(lbp)
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{
			Left:     left,
			Operator: nextToken.Value,
			Right:    right,
		}
	}
}

// getOperand is a parseExpression helper who parses token groups into atomic
// expressions serving as operands in the expression tree. A good example of
// this would be in the statement `SELECT foo.bar + 1;`. `foo.bar` is processed
// as three tokens, but needs to be "squashed" into the expression `ColumnRef`.
func (p *parser) getOperand() (Expr, error) {
	first := p.nextNonSpace()
	if first.TokenType == tkLiteral {
		return &StringLit{Value: first.Value}, nil
	}
	if first.TokenType == tkNumeric {
		intValue, err := strconv.Atoi(first.Value)
		if err != nil {
			return nil, errors.New("failed to parse numeric token")
		}
		return &IntLit{Value: intValue}, nil
	}
	if first.TokenType == tkIdentifier {
		next := p.peekNextNonSpace()
		if next.Value == "." {
			p.nextNonSpace()
			prop := p.peekNextNonSpace()
			if prop.TokenType == tkIdentifier {
				p.nextNonSpace()
				return &ColumnRef{
					Table:  first.Value,
					Column: prop.Value,
				}, nil
			}
		}
		return &ColumnRef{
			Column: first.Value,
		}, nil
	}
	if first.TokenType == tkKeyword && first.Value == kwCount {
		if v := p.nextNonSpace().Value; v != "(" {
			return nil, fmt.Errorf(tokenErr, v)
		}
		if v := p.nextNonSpace().Value; v != "*" {
			return nil, fmt.Errorf(tokenErr, v)
		}
		if v := p.nextNonSpace().Value; v != ")" {
			return nil, fmt.Errorf(tokenErr, v)
		}
		return &FunctionExpr{FnType: FnCount}, nil
	}
	// TODO support unary prefix expression
	// TODO support parens
	return nil, errors.New("failed to parse null denotation")
}

func (p *parser) parseAlias(resultColumn *ResultColumn) error {
	a := p.peekNextNonSpace().Value
	if a == kwAs {
		p.nextNonSpace()
		alias := p.nextNonSpace()
		if alias.TokenType != tkIdentifier {
			return fmt.Errorf(identErr, alias.Value)
		}
		resultColumn.Alias = alias.Value
	}
	return nil
}

func (p *parser) parseCreate(sb *StmtBase) (*CreateStmt, error) {
	stmt := &CreateStmt{StmtBase: sb}
	if p.tokens[p.end].Value != kwCreate {
		return nil, fmt.Errorf(tokenErr, p.tokens[p.end].Value)
	}
	t := p.nextNonSpace()
	if t.Value != kwTable {
		return nil, fmt.Errorf(tokenErr, p.tokens[p.end].Value)
	}
	tn := p.nextNonSpace()
	if tn.TokenType != tkIdentifier {
		return nil, fmt.Errorf(identErr, tn.Value)
	}
	stmt.TableName = tn.Value
	lp := p.nextNonSpace()
	if lp.Value != "(" {
		return nil, fmt.Errorf(tokenErr, p.tokens[p.end].Value)
	}
	stmt.ColDefs = []ColDef{}
	for {
		colName := p.nextNonSpace()
		if colName.TokenType != tkIdentifier {
			return nil, fmt.Errorf(identErr, colName.Value)
		}
		colType := p.nextNonSpace()
		if colType.Value != kwInteger && colType.Value != kwText {
			return nil, fmt.Errorf(columnErr, colType.Value)
		}
		sep := p.nextNonSpace()
		isPrimaryKey := false
		if sep.Value == kwPrimary {
			keyKw := p.nextNonSpace()
			if keyKw.Value != kwKey {
				return nil, fmt.Errorf(tokenErr, tn.Value)
			}
			isPrimaryKey = true
			sep = p.nextNonSpace()
		}
		stmt.ColDefs = append(stmt.ColDefs, ColDef{
			ColName:    colName.Value,
			ColType:    colType.Value,
			PrimaryKey: isPrimaryKey,
		})
		if sep.Value != "," {
			if sep.Value == ")" {
				break
			}
			return nil, fmt.Errorf(tokenErr, p.tokens[p.end].Value)
		}
	}
	return stmt, nil
}

func (p *parser) parseInsert(sb *StmtBase) (*InsertStmt, error) {
	stmt := &InsertStmt{StmtBase: sb}
	if p.tokens[p.end].Value != kwInsert {
		return nil, fmt.Errorf(tokenErr, p.tokens[p.end].Value)
	}
	if p.nextNonSpace().Value != kwInto {
		return nil, fmt.Errorf(tokenErr, p.tokens[p.end].Value)
	}
	tn := p.nextNonSpace()
	if tn.TokenType != tkIdentifier {
		return nil, fmt.Errorf(identErr, tn.Value)
	}
	stmt.TableName = tn.Value
	if p.nextNonSpace().Value != "(" {
		return nil, fmt.Errorf(tokenErr, p.tokens[p.end].Value)
	}
	for {
		i := p.nextNonSpace()
		if i.TokenType != tkIdentifier {
			return nil, fmt.Errorf(identErr, i.Value)
		}
		stmt.ColNames = append(stmt.ColNames, i.Value)
		sep := p.nextNonSpace()
		if sep.Value != "," {
			if sep.Value == ")" {
				break
			}
			return nil, fmt.Errorf(tokenErr, p.tokens[p.end].Value)
		}
	}
	if p.nextNonSpace().Value != kwValues {
		return nil, fmt.Errorf(tokenErr, p.tokens[p.end].Value)
	}
	return p.parseValue(stmt, 0)
}

func (p *parser) parseValue(stmt *InsertStmt, valueIdx int) (*InsertStmt, error) {
	if p.nextNonSpace().Value != "(" {
		return nil, fmt.Errorf(tokenErr, p.tokens[p.end].Value)
	}
	stmt.ColValues = append(stmt.ColValues, []string{})
	for {
		v := p.nextNonSpace()
		if v.TokenType != tkNumeric && v.TokenType != tkLiteral {
			return nil, fmt.Errorf(literalErr, v.Value)
		}
		if v.TokenType == tkLiteral && v.Value[0] == '\'' && v.Value[len(v.Value)-1] == '\'' {
			stmt.ColValues[valueIdx] = append(stmt.ColValues[valueIdx], v.Value[1:len(v.Value)-1])
		} else {
			stmt.ColValues[valueIdx] = append(stmt.ColValues[valueIdx], v.Value)
		}
		sep := p.nextNonSpace()
		if sep.Value != "," {
			if sep.Value == ")" {
				sep2 := p.nextNonSpace()
				if sep2.Value == "," {
					p.parseValue(stmt, valueIdx+1)
				}
				break
			}
			return nil, fmt.Errorf(tokenErr, p.tokens[p.end].Value)
		}
	}
	return stmt, nil
}

func (p *parser) nextNonSpace() Token {
	p.end = p.end + 1
	if p.end > len(p.tokens)-1 {
		return Token{tkEOF, ""}
	}
	for p.tokens[p.end].TokenType == TkWhitespace {
		p.end = p.end + 1
		if p.end > len(p.tokens)-1 {
			return Token{tkEOF, ""}
		}
	}
	return p.tokens[p.end]
}

func (p *parser) peekNextNonSpace() Token {
	return p.peekNonSpaceBy(1)
}

// peekNonSpaceBy will peek more than one space ahead.
func (p *parser) peekNonSpaceBy(next int) Token {
	tmpEnd := p.end + next
	if tmpEnd > len(p.tokens)-1 {
		return Token{tkEOF, ""}
	}
	for p.tokens[tmpEnd].TokenType == TkWhitespace {
		tmpEnd = tmpEnd + 1
		if tmpEnd > len(p.tokens)-1 {
			return Token{tkEOF, ""}
		}
	}
	return p.tokens[tmpEnd]
}

func (p *parser) rewind() Token {
	p.end = p.end - 1
	return p.tokens[p.end]
}
