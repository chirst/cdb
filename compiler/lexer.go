package compiler

// lexer creates tokens from a sql string. The tokens are fed into the parser.

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

// TokenTypes where tk is token
const (
	// tkKeyword is a reserved word. For example SELECT, FROM, or WHERE.
	tkKeyword = iota + 1
	// tkIdentifier is a word that is not a keyword like a table or column name.
	tkIdentifier
	//  tkWhitespace is a space, tab, or newline.
	tkWhitespace
	// tkEOF (End of file) is the end of input.
	tkEOF
	// tkSeparator is punctuation such as "(", ",", ";".
	tkSeparator
	// tkOperator is a symbol that operates on arguments.
	tkOperator
	// tkLiteral is a quoted text value like 'foo'.
	tkLiteral
	// tkNumeric is a numeric value like 1, 1.2, or -3.
	tkNumeric
	// tkPunctuator is punctuation that is neither a separator or operator.
	// tkPunctuator
)

// Keywords where kw is keyword
const (
	kwExplain = "EXPLAIN"
	kwQuery   = "QUERY"
	kwPlan    = "PLAN"
	kwSelect  = "SELECT"
	kwCount   = "COUNT"
	kwFrom    = "FROM"
	kwCreate  = "CREATE"
	kwInsert  = "INSERT"
	kwInto    = "INTO"
	kwTable   = "TABLE"
	kwValues  = "VALUES"
	kwInteger = "INTEGER"
	kwText    = "TEXT"
	kwPrimary = "PRIMARY"
	kwKey     = "KEY"
	kwAs      = "AS"
)

// keywords is a list of all keywords.
var keywords = []string{
	kwExplain,
	kwQuery,
	kwPlan,
	kwSelect,
	kwCount,
	kwFrom,
	kwCreate,
	kwInsert,
	kwInto,
	kwTable,
	kwValues,
	kwInteger,
	kwText,
	kwPrimary,
	kwKey,
	kwAs,
}

// Operators where op is operator.
const (
	OpSub = "-"
	OpAdd = "+"
	OpDiv = "/"
	OpMul = "*"
	OpExp = "^"
)

// operators is a list of all operators.
var operators = []string{
	OpSub,
	OpAdd,
	OpDiv,
	OpMul,
	OpExp,
}

// opPrecedence defines operator precedence. The higher the number the higher
// the precedence.
var opPrecedence = map[string]int{
	OpSub: 1,
	OpAdd: 1,
	OpDiv: 2,
	OpMul: 2,
	OpExp: 3,
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
		if t.tokenType == tkEOF {
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
	case l.isSeparator(r):
		return l.scanSeparator()
	case l.isSingleQuote(r):
		return l.scanLiteral()
	case l.isOperator(r):
		return l.scanOperator()
	}
	return token{tkEOF, ""}
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
	return token{tokenType: tkWhitespace, value: " "}
}

func (l *lexer) scanWord() token {
	l.next()
	for l.isLetter(l.peek(l.end)) || l.isUnderscore(l.peek(l.end)) {
		l.next()
	}
	value := l.src[l.start:l.end]
	if l.isKeyword(value) {
		return token{tokenType: tkKeyword, value: strings.ToUpper(value)}
	}
	return token{tokenType: tkIdentifier, value: value}
}

func (l *lexer) scanDigit() token {
	l.next()
	for l.isDigit(l.peek(l.end)) {
		l.next()
	}
	return token{tokenType: tkNumeric, value: l.src[l.start:l.end]}
}

func (l *lexer) scanSeparator() token {
	l.next()
	return token{tokenType: tkSeparator, value: l.src[l.start:l.end]}
}

func (l *lexer) scanLiteral() token {
	l.next()
	for !l.isSingleQuote(l.peek(l.end)) {
		l.next()
	}
	l.next()
	return token{tokenType: tkLiteral, value: l.src[l.start:l.end]}
}

func (l *lexer) scanOperator() token {
	l.next()
	return token{tokenType: tkOperator, value: l.src[l.start:l.end]}
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
	return r == ',' || r == '(' || r == ')' || r == ';' || r == '.'
}

func (*lexer) isSingleQuote(r rune) bool {
	return r == '\''
}

func (*lexer) isKeyword(w string) bool {
	uw := strings.ToUpper(w)
	return slices.Contains(keywords, uw)
}

func (*lexer) isOperator(o rune) bool {
	ros := []rune{}
	for _, op := range operators {
		ros = append(ros, rune(op[0]))
	}
	return slices.Contains(ros, o)
}
