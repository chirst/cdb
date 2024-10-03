package compiler

// parser takes tokens from the lexer and produces an AST (Abstract Syntax
// Tree). The AST is consumed to make a query plan ran by the vm (Virtual
// Machine).

import (
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
	if t.value == kwExplain {
		nv := p.nextNonSpace()
		if nv.value == kwQuery {
			tp := p.nextNonSpace()
			if tp.value == kwPlan {
				sb.ExplainQueryPlan = true
				t = p.nextNonSpace()
			} else {
				return nil, fmt.Errorf(tokenErr, p.tokens[p.end].value)
			}
		} else {
			sb.Explain = true
			t = nv
		}
	}
	switch t.value {
	case kwSelect:
		return p.parseSelect(sb)
	case kwCreate:
		return p.parseCreate(sb)
	case kwInsert:
		return p.parseInsert(sb)
	}
	return nil, fmt.Errorf(tokenErr, t.value)
}

func (p *parser) parseSelect(sb *StmtBase) (*SelectStmt, error) {
	stmt := &SelectStmt{StmtBase: sb}
	if p.tokens[p.end].value != kwSelect {
		return nil, fmt.Errorf(tokenErr, p.tokens[p.end].value)
	}
	resultColumn, err := p.parseResultColumn()
	if err != nil {
		return nil, err
	}
	stmt.ResultColumns = append(stmt.ResultColumns, *resultColumn)
	f := p.nextNonSpace()
	if f.tokenType == tkEOF || f.value == ";" {
		return stmt, nil
	}
	if f.tokenType != tkKeyword || f.value != kwFrom {
		return nil, fmt.Errorf(tokenErr, f.value)
	}

	t := p.nextNonSpace()
	if t.tokenType != tkIdentifier {
		return nil, fmt.Errorf(tokenErr, t.value)
	}
	stmt.From = &From{
		TableName: t.value,
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
	if r.value == "*" {
		resultColumn.All = true
		return resultColumn, nil
	} else if r.value == kwCount {
		// TODO count is not typical here. It should be refactored to be handled
		// by the expression logic.
		err := p.parseCountFunction(resultColumn)
		return resultColumn, err
	} else if r.tokenType == tkIdentifier {
		if p.peekNextNonSpace().value == "." {
			if p.peekNonSpaceBy(2).value == "*" {
				p.nextNonSpace() // move to .
				p.nextNonSpace() // move to *
				resultColumn.AllTable = r.value
				return resultColumn, nil
			}
		}
	}
	p.rewind()
	expr := p.parseExpression(0)
	// handle err
	// if err != nil {
	// 	return nil, err
	// }
	resultColumn.Expression = expr
	err := p.parseAlias(resultColumn)
	return resultColumn, err
}

func (p *parser) parseCountFunction(resultColumn *ResultColumn) error {
	// Handle the result column for the COUNT(*) aggregate. TODO this will
	// probably be refactored to an expression.
	if v := p.nextNonSpace().value; v != "(" {
		return fmt.Errorf(tokenErr, v)
	}
	if v := p.nextNonSpace().value; v != "*" {
		return fmt.Errorf(tokenErr, v)
	}
	if v := p.nextNonSpace().value; v != ")" {
		return fmt.Errorf(tokenErr, v)
	}
	resultColumn.Count = true
	return p.parseAlias(resultColumn)
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
func (p *parser) parseExpression(rbp int) Expr {
	// parse prefix into leftToken i.e. if the first token is a prefix operator
	// take that operator and create a unary expression. If the first token is
	// literal parse into the literal. If something else throw.
	first := p.nextNonSpace()
	var left Expr
	if first.tokenType == tkLiteral {
		left = &StringLit{Value: first.value}
	} else if first.tokenType == tkNumeric {
		intValue, err := strconv.Atoi(first.value)
		if err != nil {
			panic("sad")
		}
		left = &IntLit{Value: intValue}
	} else {
		panic("failed to parse null denotation")
	}
	for {
		nextToken := p.peekNextNonSpace()
		if nextToken.tokenType == tkOperator {
			nextPrecedence := opPrecedence[nextToken.value]
			if nextPrecedence <= rbp {
				break
			} else {
				p.nextNonSpace()
				left = &BinaryExpr{
					Left:     left,
					Operator: nextToken.value,
					Right:    p.parseExpression(nextPrecedence),
				}
				continue
			}
		} else {
			break
		}
	}
	return left
	// parse the next expression and get the lbp. If it is the end of input
	// return 0 to stop parsing. This next expression will be a infix parselet.
	// while rbp < lbp
	//   Get an infix parselet with the left being the left of the binary op.
	//   Create the new expression and assign it to left. These parselets should
	//   be recursively calling parse sometimes.
	// return left as it is the entire parsed expression
}

func (p *parser) parseAlias(resultColumn *ResultColumn) error {
	a := p.peekNextNonSpace().value
	if a == kwAs {
		p.nextNonSpace()
		alias := p.nextNonSpace()
		if alias.tokenType != tkIdentifier {
			return fmt.Errorf(identErr, alias.value)
		}
		resultColumn.Alias = alias.value
	}
	return nil
}

func (p *parser) parseCreate(sb *StmtBase) (*CreateStmt, error) {
	stmt := &CreateStmt{StmtBase: sb}
	if p.tokens[p.end].value != kwCreate {
		return nil, fmt.Errorf(tokenErr, p.tokens[p.end].value)
	}
	t := p.nextNonSpace()
	if t.value != kwTable {
		return nil, fmt.Errorf(tokenErr, p.tokens[p.end].value)
	}
	tn := p.nextNonSpace()
	if tn.tokenType != tkIdentifier {
		return nil, fmt.Errorf(identErr, tn.value)
	}
	stmt.TableName = tn.value
	lp := p.nextNonSpace()
	if lp.value != "(" {
		return nil, fmt.Errorf(tokenErr, p.tokens[p.end].value)
	}
	stmt.ColDefs = []ColDef{}
	for {
		colName := p.nextNonSpace()
		if colName.tokenType != tkIdentifier {
			return nil, fmt.Errorf(identErr, colName.value)
		}
		colType := p.nextNonSpace()
		if colType.value != kwInteger && colType.value != kwText {
			return nil, fmt.Errorf(columnErr, colType.value)
		}
		sep := p.nextNonSpace()
		isPrimaryKey := false
		if sep.value == kwPrimary {
			keyKw := p.nextNonSpace()
			if keyKw.value != kwKey {
				return nil, fmt.Errorf(tokenErr, tn.value)
			}
			isPrimaryKey = true
			sep = p.nextNonSpace()
		}
		stmt.ColDefs = append(stmt.ColDefs, ColDef{
			ColName:    colName.value,
			ColType:    colType.value,
			PrimaryKey: isPrimaryKey,
		})
		if sep.value != "," {
			if sep.value == ")" {
				break
			}
			return nil, fmt.Errorf(tokenErr, p.tokens[p.end].value)
		}
	}
	return stmt, nil
}

func (p *parser) parseInsert(sb *StmtBase) (*InsertStmt, error) {
	stmt := &InsertStmt{StmtBase: sb}
	if p.tokens[p.end].value != kwInsert {
		return nil, fmt.Errorf(tokenErr, p.tokens[p.end].value)
	}
	if p.nextNonSpace().value != kwInto {
		return nil, fmt.Errorf(tokenErr, p.tokens[p.end].value)
	}
	tn := p.nextNonSpace()
	if tn.tokenType != tkIdentifier {
		return nil, fmt.Errorf(identErr, tn.value)
	}
	stmt.TableName = tn.value
	if p.nextNonSpace().value != "(" {
		return nil, fmt.Errorf(tokenErr, p.tokens[p.end].value)
	}
	for {
		i := p.nextNonSpace()
		if i.tokenType != tkIdentifier {
			return nil, fmt.Errorf(identErr, i.value)
		}
		stmt.ColNames = append(stmt.ColNames, i.value)
		sep := p.nextNonSpace()
		if sep.value != "," {
			if sep.value == ")" {
				break
			}
			return nil, fmt.Errorf(tokenErr, p.tokens[p.end].value)
		}
	}
	if p.nextNonSpace().value != kwValues {
		return nil, fmt.Errorf(tokenErr, p.tokens[p.end].value)
	}
	return p.parseValue(stmt, 0)
}

func (p *parser) parseValue(stmt *InsertStmt, valueIdx int) (*InsertStmt, error) {
	if p.nextNonSpace().value != "(" {
		return nil, fmt.Errorf(tokenErr, p.tokens[p.end].value)
	}
	stmt.ColValues = append(stmt.ColValues, []string{})
	for {
		v := p.nextNonSpace()
		if v.tokenType != tkNumeric && v.tokenType != tkLiteral {
			return nil, fmt.Errorf(literalErr, v.value)
		}
		if v.tokenType == tkLiteral && v.value[0] == '\'' && v.value[len(v.value)-1] == '\'' {
			stmt.ColValues[valueIdx] = append(stmt.ColValues[valueIdx], v.value[1:len(v.value)-1])
		} else {
			stmt.ColValues[valueIdx] = append(stmt.ColValues[valueIdx], v.value)
		}
		sep := p.nextNonSpace()
		if sep.value != "," {
			if sep.value == ")" {
				sep2 := p.nextNonSpace()
				if sep2.value == "," {
					p.parseValue(stmt, valueIdx+1)
				}
				break
			}
			return nil, fmt.Errorf(tokenErr, p.tokens[p.end].value)
		}
	}
	return stmt, nil
}

func (p *parser) nextNonSpace() token {
	p.end = p.end + 1
	if p.end > len(p.tokens)-1 {
		return token{tkEOF, ""}
	}
	for p.tokens[p.end].tokenType == tkWhitespace {
		p.end = p.end + 1
		if p.end > len(p.tokens)-1 {
			return token{tkEOF, ""}
		}
	}
	return p.tokens[p.end]
}

func (p *parser) peekNextNonSpace() token {
	return p.peekNonSpaceBy(1)
}

// peekNonSpaceBy will peek more than one space ahead.
func (p *parser) peekNonSpaceBy(next int) token {
	tmpEnd := p.end + next
	if tmpEnd > len(p.tokens)-1 {
		return token{tkEOF, ""}
	}
	for p.tokens[tmpEnd].tokenType == tkWhitespace {
		tmpEnd = tmpEnd + 1
		if tmpEnd > len(p.tokens)-1 {
			return token{tkEOF, ""}
		}
	}
	return p.tokens[tmpEnd]
}

func (p *parser) rewind() token {
	p.end = p.end - 1
	return p.tokens[p.end]
}
