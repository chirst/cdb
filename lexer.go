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
	KEYWORD = iota + 1
	ASTERISK
	IDENTIFIER
	SPACE
	EOF
	SEMI
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
	case l.isSpace(r):
		return l.scanSpace()
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

func (l *lexer) scanSpace() token {
	l.next()
	for l.isSpace(l.peek(l.end)) {
		l.next()
	}
	return token{tokenType: SPACE, value: l.src[l.start:l.end]}
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
	return token{tokenType: ASTERISK, value: l.src[l.start:l.end]}
}

func (l *lexer) scanSemi() token {
	l.next()
	return token{tokenType: SEMI, value: l.src[l.start:l.end]}
}

func (*lexer) isEOF(r rune) bool {
	return r == 0
}

func (*lexer) isSpace(r rune) bool {
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
