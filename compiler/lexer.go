package compiler

// lexer creates tokens from a sql string. The tokens are fed into the parser.

import (
	"fmt"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Statement is a individual SQL statement.
type Statement []token

// Statements is a list of SQL statements that have been split by semi colons.
type Statements [][]token

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
	// tkParam is a placeholder variable such as ?.
	tkParam
	// tkComment is either a line or block comment. The value contains the
	// comment including the leading "--" or leading "/*" and trailing "*/".
	tkComment
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
	kwWhere   = "WHERE"
	kwIf      = "IF"
	kwNot     = "NOT"
	kwExists  = "EXISTS"
	kwUpdate  = "UPDATE"
	kwSet     = "SET"
	kwDelete  = "DELETE"
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
	kwWhere,
	kwIf,
	kwNot,
	kwExists,
	kwUpdate,
	kwSet,
	kwDelete,
}

// Operators where op is operator.
const (
	OpSub = "-"
	OpAdd = "+"
	OpDiv = "/"
	OpMul = "*"
	OpExp = "^"
	OpEq  = "="
	OpLt  = "<"
	OpGt  = ">"
)

// operators is a list of all operators.
var operators = []string{
	OpSub,
	OpAdd,
	OpDiv,
	OpMul,
	OpExp,
	OpEq,
	OpLt,
	OpGt,
}

// opPrecedence defines operator precedence. The higher the number the higher
// the precedence.
var opPrecedence = map[string]int{
	OpEq:  1,
	OpLt:  2,
	OpGt:  2,
	OpSub: 3,
	OpAdd: 3,
	OpDiv: 4,
	OpMul: 4,
	OpExp: 5,
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

// ToStatements splits the src string into a list of statements where each
// statement is terminated by a semi colon.
func (l *lexer) ToStatements() Statements {
	tokens := l.Lex()
	statements := [][]token{}
	start := 0
	for i := range tokens {
		if tokens[i].value == ";" {
			statements = append(statements, tokens[start:i+1])
			start = i + 1
		}
	}
	if start == len(tokens) {
		return statements
	}
	statements = append(statements, tokens[start:])
	lastStmt := statements[len(statements)-1]
	if isAllWhitespace(lastStmt) {
		return statements[:len(statements)-1]
	}
	return statements
}

func isAllWhitespace(s Statement) bool {
	for _, t := range s {
		if t.tokenType != tkWhitespace {
			return false
		}
	}
	return true
}

// IsTerminated returns true when the last Statement in the list of Statements
// is terminated by a semi colon.
func IsTerminated(statements Statements) bool {
	if len(statements) == 0 {
		return false
	}
	lastStatement := statements[len(statements)-1]
	for _, token := range slices.Backward(lastStatement) {
		if token.tokenType == tkWhitespace {
			continue
		}
		if token.value == ";" {
			return true
		}
		break
	}
	return false
}

// Lex tokenizes the src string.
func (l *lexer) Lex() []token {
	ret := []token{}
	for {
		t := l.getToken()
		if t.tokenType == tkEOF {
			return ret
		}
		if t.tokenType != tkComment {
			ret = append(ret, t)
		}
	}
}

func (l *lexer) getToken() token {
	l.start = l.end
	r := l.peek(l.start)
	switch {
	case l.isWhiteSpace(r):
		return l.scanWhiteSpace()
	case l.isLineCommentStart(r):
		return l.scanLineComment()
	case l.isBlockCommentStart(r):
		return l.scanBlockComment()
	case l.isLetter(r):
		return l.scanWord()
	case l.isDigit(r):
		return l.scanDigit()
	case l.isSeparator(r):
		return l.scanSeparator()
	case l.isSingleQuote(r):
		return l.scanLiteral('\'')
	case l.isDoubleQuote(r):
		return l.scanLiteral('"')
	case l.isOperator(r):
		return l.scanOperator()
	case l.isParam(r):
		return l.scanParam()
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

func (l *lexer) scanLiteral(quote rune) token {
	l.next()
	for {
		if l.peek(l.end) == quote && l.peek(l.end+1) == quote {
			l.next()
			l.next()
			continue
		}
		if l.peek(l.end) == quote {
			break
		}
		l.next()
	}
	l.next()
	v := strings.ReplaceAll(
		l.src[l.start+1:l.end-1],
		fmt.Sprintf("%c%c", quote, quote),
		fmt.Sprintf("%c", quote),
	)
	return token{tokenType: tkLiteral, value: v}
}

func (l *lexer) scanOperator() token {
	l.next()
	return token{tokenType: tkOperator, value: l.src[l.start:l.end]}
}

func (l *lexer) scanParam() token {
	l.next()
	return token{tokenType: tkParam, value: l.src[l.start:l.end]}
}

func (*lexer) isWhiteSpace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\n'
}

func (*lexer) isLetter(r rune) bool {
	return unicode.IsLetter(r)
}

func (l *lexer) isLineCommentStart(r rune) bool {
	return (r == '-') && l.peek(l.end+1) == '-'
}

func (l *lexer) scanLineComment() token {
	l.next()
	l.next()
	for {
		t := l.next()
		if t == '\n' || l.end == len(l.src) {
			return token{tokenType: tkComment, value: l.src[l.start:l.end]}
		}
	}
}

func (l *lexer) isBlockCommentStart(r rune) bool {
	return (r == '/') && l.peek(l.end+1) == '*'
}

func (l *lexer) scanBlockComment() token {
	l.next()
	l.next()
	for {
		t := l.next()
		if t == '*' && l.peek(l.end+1) == '/' {
			l.next()
			l.next()
			return token{tokenType: tkComment, value: l.src[l.start:l.end]}
		}
		if l.end == len(l.src) {
			return token{tokenType: tkComment, value: l.src[l.start:l.end]}
		}
	}
}

func (*lexer) isUnderscore(r rune) bool {
	return r == '_'
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

func (*lexer) isDoubleQuote(r rune) bool {
	return r == '"'
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

func (*lexer) isParam(r rune) bool {
	return r == '?'
}
