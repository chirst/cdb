package main

type parser struct {
	tokens []token
	start  int
	end    int
}

func (p *parser) parse() stmtList {
	ret := stmtList{}
	for {
		ret = append(ret, p.parseStmt())
		p.start = p.end
		if len(p.tokens) <= p.end {
			return ret
		}
	}
}

func (p *parser) parseStmt() any {
	t := p.tokens[p.start]
	switch t.value {
	case "SELECT":
		return p.parseSelect()
	}
	panic("wut") // TODO
}

func (p *parser) parseSelect() selectStmt {
	stmt := selectStmt{}
	if p.tokens[p.end].value != "SELECT" {
		// return err
	}
	for {
		resultCol := p.nextNonSpace()
		if resultCol.tokenType == KEYWORD {
			continue
		}
		stmt.resultColumns = append(stmt.resultColumns, resultColumn{
			all: resultCol.tokenType == ASTERISK,
		})
	}
	return stmt
}

func (p *parser) nextNonSpace() token {
	p.end = p.end + 1
	for p.tokens[p.end].tokenType == SPACE {
		p.end = p.end + 1
	}
	return p.tokens[p.end]
}
