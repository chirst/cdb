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
	// Handle a result column for all or *.
	if r.value == "*" {
		resultColumn.All = true
		return resultColumn, nil
	} else if r.value == kwCount {
		// Handle the result column for the COUNT(*) aggregate. TODO this will
		// probably be refactored to an expression.
		if v := p.nextNonSpace().value; v != "(" {
			return nil, fmt.Errorf(tokenErr, v)
		}
		if v := p.nextNonSpace().value; v != "*" {
			return nil, fmt.Errorf(tokenErr, v)
		}
		if v := p.nextNonSpace().value; v != ")" {
			return nil, fmt.Errorf(tokenErr, v)
		}
		resultColumn.Count = true
		err := p.parseAlias(resultColumn)
		return resultColumn, err
	} else if r.tokenType == tkIdentifier {
		// Handle an identifier such as a table or column name
		if p.peekNextNonSpace().value == "." {
			// The identifier may be followed by "." meaning the identifier is a
			// table and after the dot is the column.
			p.nextNonSpace()
			v := p.nextNonSpace().value
			// A star after the dot is to select all the cols in a table.
			if v == "*" {
				resultColumn.AllTable = r.value
			} else {
				// Otherwise after the dot has to be a specific column name.
				resultColumn.Expression = &ColumnRef{
					Table:  r.value,
					Column: v,
				}
				err := p.parseAlias(resultColumn)
				return resultColumn, err
			}
			return resultColumn, nil
		} else if p.peekNextNonSpace().tokenType == tkWhitespace {
			// If the identifier is followed by whitespace the identifier is a
			// column name. There is no table name meaning the table will have
			// to be resolved in the planner.
			resultColumn.Expression = &ColumnRef{
				Column: r.value,
			}
			err := p.parseAlias(resultColumn)
			return resultColumn, err
		} else {
			return nil, fmt.Errorf(tokenErr, r.value)
		}
	} else if r.tokenType == tkNumeric {
		// A numeric value may begin a complex expression.
		vi, err := strconv.Atoi(r.value)
		if err != nil {
			return nil, err
		}
		resultColumn.Expression = &IntLit{
			Value: vi,
		}
		err = p.parseAlias(resultColumn)
		return resultColumn, err
	}
	return nil, fmt.Errorf(tokenErr, r.value)
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
	tmpEnd := p.end + 1
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
