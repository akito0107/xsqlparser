package xsqlparser

import (
	"fmt"
	"io"
	"strings"
	"text/scanner"

	errors "golang.org/x/xerrors"

	"github.com/akito0107/xsqlparser/dialect"
	"github.com/akito0107/xsqlparser/sqlast"
)

type SQLWord struct {
	Value      string
	QuoteStyle rune
	Keyword    string
}

func (s *SQLWord) String() string {
	if s.QuoteStyle == '"' || s.QuoteStyle == '[' || s.QuoteStyle == '`' {
		return string(s.QuoteStyle) + s.Value + string(matchingEndQuote(s.QuoteStyle))
	} else if s.QuoteStyle == 0 {
		return s.Value
	}
	return ""
}

func (s *SQLWord) AsSQLIdent() *sqlast.Ident {
	return sqlast.NewIdent(s.String())
}

func matchingEndQuote(quoteStyle rune) rune {
	switch quoteStyle {
	case '"':
		return '"'
	case '[':
		return ']'
	case '`':
		return '`'
	}
	return 0
}

func MakeKeyword(word string, quoteStyle rune) *SQLWord {
	w := strings.ToUpper(word)

	_, ok := dialect.Keywords[w]

	if quoteStyle == 0 && ok {
		return &SQLWord{
			Value:   word,
			Keyword: w,
		}
	} else {
		return &SQLWord{
			Value:      word,
			Keyword:    w,
			QuoteStyle: quoteStyle,
		}
	}
}

type Token struct {
	Kind  TokenKind
	Value interface{}
	Pos   TokenPos
}

type TokenPos struct {
	Line int
	Col  int
}

func (t *TokenPos) String() string {
	return fmt.Sprintf("{Line: %d Col: %d}", t.Line, t.Col)
}

type Tokenizer struct {
	Dialect dialect.Dialect
	Scanner *scanner.Scanner
	Line    int
	Col     int
}

func NewTokenizer(src io.Reader, dialect dialect.Dialect) *Tokenizer {
	var scan scanner.Scanner
	return &Tokenizer{
		Dialect: dialect,
		Scanner: scan.Init(src),
		Line:    1,
		Col:     0,
	}
}

func (t *Tokenizer) Tokenize() ([]*Token, error) {
	var tokenset []*Token

	for {
		t, err := t.NextToken()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		tokenset = append(tokenset, t)
	}

	return tokenset, nil
}

func (t *Tokenizer) NextToken() (*Token, error) {
	tok, str, err := t.next()
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return &Token{Kind: ILLEGAL, Value: "", Pos: t.Pos()}, errors.Errorf("tokenize failed %w", err)
	}

	return &Token{Kind: tok, Value: str, Pos: t.Pos()}, nil
}

func (t *Tokenizer) Pos() TokenPos {
	return TokenPos{
		Line: t.Line,
		Col:  t.Col,
	}
}

func (t *Tokenizer) next() (TokenKind, interface{}, error) {
	r := t.Scanner.Peek()
	switch {
	case ' ' == r:
		t.Scanner.Next()
		t.Col += 1
		return Whitespace, " ", nil

	case '\t' == r:
		t.Scanner.Next()
		t.Col += 4
		return Whitespace, "\t", nil

	case '\n' == r:
		t.Scanner.Next()
		t.Line += 1
		t.Col = 0
		return Whitespace, "\n", nil

	case '\r' == r:
		t.Scanner.Next()
		n := t.Scanner.Peek()
		if n == '\n' {
			t.Scanner.Next()
		}
		t.Line += 1
		t.Col = 0
		return Whitespace, "\n", nil

	case 'N' == r:
		t.Scanner.Next()
		n := t.Scanner.Peek()
		if n == '\'' {
			t.Col += 1
			str := t.tokenizeSingleQuotedString()
			return NationalStringLiteral, str, nil
		}
		s := t.tokenizeWord('N')
		v := MakeKeyword(s, 0)
		return SQLKeyword, v, nil

	case t.Dialect.IsIdentifierStart(r):
		t.Scanner.Next()
		s := t.tokenizeWord(r)
		return SQLKeyword, MakeKeyword(s, 0), nil

	case '\'' == r:
		s := t.tokenizeSingleQuotedString()
		return SingleQuotedString, s, nil

	case t.Dialect.IsDelimitedIdentifierStart(r):
		t.Scanner.Next()
		end := matchingEndQuote(r)

		var s []rune
		for {
			n := t.Scanner.Next()
			if n == end {
				break
			}
			s = append(s, n)
		}
		t.Col += 2 + len(s)

		return SQLKeyword, MakeKeyword(string(s), r), nil

	case '0' <= r && r <= '9':
		var s []rune
		for {
			n := t.Scanner.Peek()
			if ('0' <= n && n <= '9') || n == '.' {
				s = append(s, n)
				t.Scanner.Next()
			} else {
				break
			}
		}
		t.Col += len(s)
		return Number, string(s), nil

	case '(' == r:
		t.Scanner.Next()
		t.Col += 1
		return LParen, "(", nil

	case ')' == r:
		t.Scanner.Next()
		t.Col += 1
		return RParen, ")", nil

	case ',' == r:
		t.Scanner.Next()
		t.Col += 1
		return Comma, ",", nil

	case '-' == r:
		t.Scanner.Next()

		if '-' == t.Scanner.Peek() {
			t.Scanner.Next()

			var s []rune
			for {
				ch := t.Scanner.Next()
				if ch != scanner.EOF && ch != '\n' {
					s = append(s, ch)
				} else {
					s = append(s, '\n')
					t.Col = 0
					t.Line += 1
					return Whitespace, string(s), nil // Comment Node
				}
			}
		}
		t.Col += 1
		return Minus, "-", nil

	case '/' == r:
		t.Scanner.Next()

		if '*' == t.Scanner.Peek() {
			t.Scanner.Next()
			return Whitespace, t.tokenizeMultilineComment(), nil
		}
		t.Col += 1
		return Div, "/", nil

	case '+' == r:
		t.Scanner.Next()
		t.Col += 1
		return Plus, "+", nil
	case '*' == r:
		t.Scanner.Next()
		t.Col += 1
		return Mult, "*", nil
	case '%' == r:
		t.Scanner.Next()
		t.Col += 1
		return Mod, "%", nil
	case '=' == r:
		t.Scanner.Next()
		t.Col += 1
		return Eq, "=", nil
	case '.' == r:
		t.Scanner.Next()
		t.Col += 1
		return Period, ".", nil

	case '!' == r:
		t.Scanner.Next()
		n := t.Scanner.Peek()
		if n == '=' {
			t.Scanner.Next()
			t.Col += 2
			return Neq, "!=", nil
		}
		return ILLEGAL, "", errors.Errorf("tokenizer error: illegal sequence %s%s", string(r), string(n))

	case '<' == r:
		t.Scanner.Next()
		switch t.Scanner.Peek() {
		case '=':
			t.Scanner.Next()
			t.Col += 2
			return LtEq, "<=", nil
		case '>':
			t.Scanner.Next()
			t.Col += 2
			return Neq, "<>", nil
		default:
			t.Col += 1
			return Lt, "<", nil
		}
	case '>' == r:
		t.Scanner.Next()
		switch t.Scanner.Peek() {
		case '=':
			t.Scanner.Next()
			t.Col += 2
			return GtEq, ">=", nil
		default:
			t.Col += 1
			return Gt, ">", nil
		}
	case ':' == r:
		t.Scanner.Next()
		n := t.Scanner.Peek()
		if n == ':' {
			t.Scanner.Next()
			t.Col += 2
			return DoubleColon, "::", nil
		}
		t.Col += 1
		return Colon, ":", nil
	case ';' == r:
		t.Scanner.Next()
		t.Col += 1
		return Semicolon, ";", nil
	case '\\' == r:
		t.Scanner.Next()
		t.Col += 1
		return Backslash, "\\", nil
	case '[' == r:
		t.Scanner.Next()
		t.Col += 1
		return LBracket, "[", nil
	case ']' == r:
		t.Scanner.Next()
		t.Col += 1
		return RBracket, "]", nil
	case '&' == r:
		t.Scanner.Next()
		t.Col += 1
		return Ampersand, "&", nil
	case '{' == r:
		t.Scanner.Next()
		t.Col += 1
		return LBrace, "{", nil
	case '}' == r:
		t.Scanner.Next()
		t.Col += 1
		return RBrace, "}", nil
	case scanner.EOF == r:
		return ILLEGAL, "", io.EOF
	default:
		t.Scanner.Next()
		t.Col += 1
		return Char, string(r), nil
	}
}

func (t *Tokenizer) tokenizeWord(f rune) string {
	var str []rune
	str = append(str, f)

	for {
		r := t.Scanner.Peek()
		if t.Dialect.IsIdentifierPart(r) {
			t.Scanner.Next()
			str = append(str, r)
		} else {
			break
		}
	}
	t.Col += len(str)
	return string(str)
}

func (t *Tokenizer) tokenizeSingleQuotedString() string {
	var str []rune
	t.Scanner.Next()

	for {
		n := t.Scanner.Peek()
		if n == '\'' {
			t.Scanner.Next()
			if t.Scanner.Peek() == '\'' {
				str = append(str, '\'')
				t.Scanner.Next()
			} else {
				break
			}
			continue
		}

		t.Scanner.Next()
		str = append(str, n)
	}
	t.Col += 2 + len(str)

	return string(str)
}

func (t *Tokenizer) tokenizeMultilineComment() string {
	var str []rune
	var mayBeClosingComment bool
	t.Col += 2
	for {
		n := t.Scanner.Next()

		if n == '\r' {
			if t.Scanner.Peek() == '\n' {
				t.Scanner.Next()
			}
			t.Col = 0
			t.Line += 1
		} else if n == '\n' {
			t.Col = 0
			t.Line += 1
		} else {
			t.Col += 1
		}

		if mayBeClosingComment {
			if n == '/' {
				break
			} else {
				str = append(str, n)
			}
		}
		mayBeClosingComment = n == '*'
		if !mayBeClosingComment {
			str = append(str, n)
		}
	}

	return string(str)
}
