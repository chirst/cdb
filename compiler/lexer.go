// lexer creates tokens from a sql string. The tokens are fed into the parser.
package compiler

import (
	"slices"
	"strings"
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
	// IDENTIFIER is a word that is not a keyword like a table or column name.
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
	// LITERAL is a quoted text value like 'foo'.
	LITERAL
	// NUMERIC is a numeric value like 1, 1.2, or -3.
	NUMERIC
)

var keywords = []string{
	"EXPLAIN",
	"SELECT",
	"FROM",
	"CREATE",
	"INSERT",
	"INTO",
	"TABLE",
	"VALUES",
	"INTEGER",
	"TEXT",
}

func (*lexer) isKeyword(w string) bool {
	uw := strings.ToUpper(w)
	return slices.Contains(keywords, uw)
}

type lexer struct {
	src   string
	start int
	end   int
}

func NewLexer(src string) *lexer {
	ts := strings.Trim(src, " \t\n")
	return &lexer{src: ts}
}

func (l *lexer) Lex() []token {
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
	case l.isDigit(r):
		return l.scanDigit()
	case l.isAsterisk(r):
		return l.scanAsterisk()
	case l.isSeparator(r):
		return l.scanSeparator()
	case l.isSingleQuote(r):
		return l.scanLiteral()
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
	return token{tokenType: WHITESPACE, value: " "}
}

func (l *lexer) scanWord() token {
	l.next()
	for l.isLetter(l.peek(l.end)) || l.isUnderscore(l.peek(l.end)) {
		l.next()
	}
	value := l.src[l.start:l.end]
	if l.isKeyword(value) {
		return token{tokenType: KEYWORD, value: strings.ToUpper(value)}
	}
	return token{tokenType: IDENTIFIER, value: value}
}

func (l *lexer) scanDigit() token {
	l.next()
	for l.isDigit(l.peek(l.end)) {
		l.next()
	}
	return token{tokenType: NUMERIC, value: l.src[l.start:l.end]}
}

func (l *lexer) scanAsterisk() token {
	l.next()
	return token{tokenType: PUNCTUATOR, value: l.src[l.start:l.end]}
}

func (l *lexer) scanSeparator() token {
	l.next()
	return token{tokenType: SEPARATOR, value: l.src[l.start:l.end]}
}

func (l *lexer) scanLiteral() token {
	l.next()
	for !l.isSingleQuote(l.peek(l.end)) {
		l.next()
	}
	l.next()
	return token{tokenType: LITERAL, value: l.src[l.start:l.end]}
}

func (*lexer) isWhiteSpace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\n'
}

func (*lexer) isLetter(r rune) bool {
	return unicode.IsLetter(r)
}

func (*lexer) isUnderscore(r rune) bool {
	return r == '_'
}

func (*lexer) isAsterisk(r rune) bool {
	return r == '*'
}

func (*lexer) isDigit(r rune) bool {
	return unicode.IsDigit(r)
}

func (*lexer) isSeparator(r rune) bool {
	return r == ',' || r == '(' || r == ')' || r == ';'
}

func (*lexer) isSingleQuote(r rune) bool {
	return r == '\''
}
