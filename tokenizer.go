package xsqlparser

import (
	"io"
	"text/scanner"

	"github.com/pkg/errors"

	"github.com/akito0107/xsqlparser/dialect"
)

type Token int

const (
	// A keyword (like SELECT)
	SQLWord Token = iota
	// Numeric literal
	Number
	// A character that cloud not be tokenized
	Char
	// Single quoted string i.e: 'string'
	SingleQuotedString
	// National string i.e: N'string'
	NationalStringLiteral
	// Comma
	Comma
	// Whitespace
	Whitespace
	// = operator
	Eq
	// != or <> operator
	Neq
	// <  operator
	Lt
	// > operator
	Gt
	// <= operator
	LtEq
	// >= operator
	GtEq
	// + operator
	Plus
	// - operator
	Minus
	// * operator
	Mult
	// / operator
	Div
	// % operator
	Mod
	// Left parenthesis `(`
	LParen
	// Right parenthesis `)`
	RParen
	// Period
	Period
	// Colon
	Colon
	// DoubleColon
	DoubleColon
	// Semicolon
	Semicolon
	// Backslash
	Backslash
	// Left bracket `]`
	LBracket
	// Right bracket `[`
	RBracket
	// &
	Ampersand
	// Left brace `{`
	LBrace
	// Right brace `}`
	RBrace
	// ILLEGAL token
	ILLEGAL
)

type TokenSet struct {
	Tok   Token
	Value string
	Pos   *TokenPos
}

type TokenPos struct {
	Line int
	Col  int
}

type Tokenizer struct {
	Dialect dialect.Dialect
	Scanner *scanner.Scanner
	Line    int
	Col     int
}

func Init(src io.Reader, dialect dialect.Dialect) *Tokenizer {
	var scan scanner.Scanner
	return &Tokenizer{
		Dialect: dialect,
		Scanner: scan.Init(src),
		Line:    1,
		Col:     1,
	}
}

func (t *Tokenizer) Tokenize() ([]*TokenSet, error) {
	var tokenset []*TokenSet

	for {
		t, err := t.NextToken()
		if err == io.EOF {
			break
		}
		tokenset = append(tokenset, t)
	}

	return tokenset, nil
}

func (t *Tokenizer) NextToken() (*TokenSet, error) {
	tok, str, err := t.next()
	if err != nil {
		return &TokenSet{Tok: ILLEGAL, Value: "", Pos: t.Pos()}, errors.Wrap(err, "tokenize failed")
	}

	return &TokenSet{Tok: tok, Value: str, Pos: t.Pos()}, nil
}

func (t *Tokenizer) Pos() *TokenPos {
	return &TokenPos{
		Line: t.Line,
		Col:  t.Col,
	}
}

func (t *Tokenizer) next() (Token, string, error) {
	r := t.Scanner.Peek()

	switch r {
	case ' ':
		t.Scanner.Next()
		t.Col += 1
		return Whitespace, " ", nil
	case '\t':
		t.Scanner.Next()
		t.Col += 4
		return Whitespace, "\t", nil
	case '\n':
		t.Scanner.Next()
		t.Line += 1
		t.Col = 1
		return Whitespace, "\t", nil
	case '\r':
		t.Scanner.Next()
		n := t.Scanner.Peek()
		if n == '\n' {
			t.Scanner.Next()
		}
		return Whitespace, "\r", nil
	}

	return ILLEGAL, "", nil
}
