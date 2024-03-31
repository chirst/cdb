// lexer creates tokens from a sql string. The tokens are fed into the parser.
package main

import (
	"unicode"
	"unicode/utf8"
)

type tokenType int

type token struct {
	tokenType tokenType
	value     string
}

const (
	// KEYWORD is a reserved word. For example SELECT, FROM, or WHERE.
	KEYWORD = iota + 1
	// IDENTIFIER is a name assigned by the programmer. For example a table
	// or column name.
	IDENTIFIER
	//  WHITESPACE is a space, tab, or newline.
	WHITESPACE
	// EOF (End of file) is the end of input.
	EOF
	// SEPARATOR is punctuation such as "(", ",", ";".
	SEPARATOR
	// OPERATOR is a symbol that operates on arguments.
	OPERATOR
	// PUNCTUATOR is punctuation that is neither a separator or operator.
	PUNCTUATOR
	// LITERAL is a numeric or text value.
	LITERAL
)

func (*lexer) isKeyword(w string) bool {
	ws := map[string]bool{
		"SELECT": true,
		"FROM":   true,
	}
	return ws[w]
}

type lexer struct {
	src   string
	start int
	end   int
}

func newLexer(src string) *lexer {
	return &lexer{src: src}
}

func (l *lexer) lex() []token {
	ret := []token{}
	for {
		t := l.getToken()
		if t.tokenType == EOF {
			return ret
		}
		ret = append(ret, t)
	}
}

func (l *lexer) getToken() token {
	l.start = l.end
	r := l.peek(l.start)
	switch {
	case l.isWhiteSpace(r):
		return l.scanWhiteSpace()
	case l.isLetter(r):
		return l.scanWord()
	case l.isAsterisk(r):
		return l.scanAsterisk()
	case l.isDigit(r):
		return l.scanDigit()
	case l.isSemi(r):
		return l.scanSemi()
	}
	return token{EOF, ""}
}

func (l *lexer) peek(pos int) rune {
	if len(l.src) <= pos {
		return 0
	}
	r, _ := utf8.DecodeRuneInString(l.src[pos:])
	return r
}

func (l *lexer) next() rune {
	r := l.peek(l.end + 1)
	l.end = l.end + 1
	return r
}

func (l *lexer) scanWhiteSpace() token {
	l.next()
	for l.isWhiteSpace(l.peek(l.end)) {
		l.next()
	}
	return token{tokenType: WHITESPACE, value: l.src[l.start:l.end]}
}

func (l *lexer) scanWord() token {
	l.next()
	for l.isLetter(l.peek(l.end)) {
		l.next()
	}
	value := l.src[l.start:l.end]
	var tokenType tokenType = IDENTIFIER
	if l.isKeyword(value) {
		tokenType = KEYWORD
	}
	return token{tokenType: tokenType, value: value}
}

func (l *lexer) scanDigit() token {
	l.next()
	for l.isDigit(l.peek(l.end)) {
		l.next()
	}
	return token{tokenType: IDENTIFIER, value: l.src[l.start:l.end]}
}

func (l *lexer) scanAsterisk() token {
	l.next()
	return token{tokenType: PUNCTUATOR, value: l.src[l.start:l.end]}
}

func (l *lexer) scanSemi() token {
	l.next()
	return token{tokenType: SEPARATOR, value: l.src[l.start:l.end]}
}

func (*lexer) isWhiteSpace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\n'
}

func (*lexer) isLetter(r rune) bool {
	return unicode.IsLetter(r)
}

func (*lexer) isAsterisk(r rune) bool {
	return r == '*'
}

func (*lexer) isDigit(r rune) bool {
	return unicode.IsDigit(r)
}

func (*lexer) isSemi(r rune) bool {
	return r == ';'
}
