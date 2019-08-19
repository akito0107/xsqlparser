package xsqlparser

type TokenKind int

//go:generate stringer -type TokenKind tokenkind.go
const (
	// A keyword (like SELECT)
	SQLKeyword TokenKind = iota
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
