package sqltoken

import (
	"fmt"
	"io"
	"strings"
	"text/scanner"

	errors "golang.org/x/xerrors"

	"github.com/akito0107/xsqlparser/dialect"
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

var keywordCache = map[string]*SQLWord{}

func init() {
	for keyword := range dialect.Keywords {
		keywordCache[keyword] = &SQLWord{
			Value:      keyword,
			Keyword:    keyword,
		}
		lower := strings.ToLower(keyword)
		keywordCache[lower] = &SQLWord{
			Value:      lower,
			Keyword:    keyword,
		}
	}
}

func MakeKeyword(word string, quoteStyle rune) *SQLWord {
	if quoteStyle == 0 {
		if w, ok := keywordCache[word]; ok {
			return w
		}
	}
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
	Kind  Kind
	Value interface{}
	From  Pos
	To    Pos
}

func NewPos(line, col int) Pos {
	return Pos{
		Line: line,
		Col:  col,
	}
}

type Pos struct {
	Line int
	Col  int
}

func (p *Pos) String() string {
	return fmt.Sprintf("{Line: %d Col: %d}", p.Line, p.Col)
}

func ComparePos(x, y Pos) int {
	if x.Line == y.Line && x.Col == y.Col {
		return 0
	}

	if x.Line > y.Line {
		return 1
	} else if x.Line < y.Line {
		return -1
	}

	if x.Col > y.Col {
		return 1
	}

	return -1
}

type Tokenizer struct {
	Dialect      dialect.Dialect
	Scanner      *scanner.Scanner
	Line         int
	Col          int
	parseComment bool
}

func NewTokenizer(src io.Reader, dialect dialect.Dialect) *Tokenizer {
	var scan scanner.Scanner
	return &Tokenizer{
		Dialect:      dialect,
		Scanner:      scan.Init(src),
		Line:         1,
		Col:          1,
		parseComment: true,
	}
}

type TokenizerOption func(*Tokenizer)

func Dialect(dialect dialect.Dialect) TokenizerOption {
	return func(tokenizer *Tokenizer) {
		tokenizer.Dialect = dialect
	}
}

func DisableParseComment() TokenizerOption {
	return func(tokenizer *Tokenizer) {
		tokenizer.parseComment = false
	}
}

func NewTokenizerWithOptions(src io.Reader, options ...TokenizerOption) *Tokenizer {
	tokenizer := NewTokenizer(src, &dialect.GenericSQLDialect{})
	for _, o := range options {
		o(tokenizer)
	}
	return tokenizer
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

		if t == nil {
			continue
		}
		tokenset = append(tokenset, t)
	}

	return tokenset, nil
}

func (t *Tokenizer) NextToken() (*Token, error) {
	pos := t.Pos()
	tok, str, err := t.next()
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return &Token{Kind: ILLEGAL, Value: "", From: pos, To: t.Pos()}, errors.Errorf("tokenize failed: %w", err)
	}

	if !t.parseComment && (tok == Whitespace || tok == Comment) {
		return nil, nil
	}

	return &Token{Kind: tok, Value: str, From: pos, To: t.Pos()}, nil
}

func (t *Tokenizer) Pos() Pos {
	return Pos{
		Line: t.Line,
		Col:  t.Col,
	}
}

func (t *Tokenizer) next() (Kind, interface{}, error) {
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
		t.Col = 1
		return Whitespace, "\n", nil

	case '\r' == r:
		t.Scanner.Next()
		n := t.Scanner.Peek()
		if n == '\n' {
			t.Scanner.Next()
		}
		t.Line += 1
		t.Col = 1
		return Whitespace, "\n", nil

	case 'N' == r:
		t.Scanner.Next()
		n := t.Scanner.Peek()
		if n == '\'' {
			t.Col += 1
			str, err := t.tokenizeSingleQuotedString()
			if err != nil {
				return ILLEGAL, "", err
			}
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
		s, err := t.tokenizeSingleQuotedString()
		if err != nil {
			return ILLEGAL, "", err
		}
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
				ch := t.Scanner.Peek()
				if ch != scanner.EOF && ch != '\n' {
					t.Scanner.Next()
					s = append(s, ch)
				} else {
					t.Col += len(s) + 2
					return Comment, string(s), nil // Comment Node
				}
			}
		}
		t.Col += 1
		return Minus, "-", nil

	case '/' == r:
		t.Scanner.Next()

		if '*' == t.Scanner.Peek() {
			t.Scanner.Next()
			str, err := t.tokenizeMultilineComment()
			if err != nil {
				return ILLEGAL, str, err
			}
			return Comment, str, nil
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
	var builder strings.Builder
	builder.WriteRune(f)

	for {
		r := t.Scanner.Peek()
		if t.Dialect.IsIdentifierPart(r) {
			t.Scanner.Next()
			builder.WriteRune(r)
		} else {
			break
		}
	}

	str := builder.String()
	t.Col += len(str)
	return str
}

func (t *Tokenizer) tokenizeSingleQuotedString() (string, error) {
	// var str []rune
	var builder strings.Builder
	t.Scanner.Next()

	for {
		n := t.Scanner.Peek()
		if n == '\'' {
			t.Scanner.Next()
			if t.Scanner.Peek() == '\'' {
				// str = append(str, '\'')
				builder.WriteRune('\'')
				t.Scanner.Next()
			} else {
				break
			}
			continue
		}
		if n == scanner.EOF {
			return "", errors.Errorf("unclosed single quoted string: %s at %+v", builder.String(), t.Pos())
		}

		t.Scanner.Next()
		builder.WriteRune(n)
		// str = append(str, n)
	}
	str := builder.String()
	t.Col += 2 + len(str)

	return str, nil
}

func (t *Tokenizer) tokenizeMultilineComment() (string, error) {
	var str []rune
	var mayBeClosingComment bool
	t.Col += 2
	for {
		n := t.Scanner.Next()

		if n == '\r' {
			if t.Scanner.Peek() == '\n' {
				t.Scanner.Next()
			}
			t.Col = 1
			t.Line += 1
		} else if n == '\n' {
			t.Col = 1
			t.Line += 1
		} else if n == scanner.EOF {
			return "", errors.Errorf("unclosed multiline comment: %s at %+v", string(str), t.Pos())
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

	return string(str), nil
}
