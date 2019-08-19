package xsqlparser

import (
	"reflect"
	"strings"
	"testing"

	"github.com/akito0107/xsqlparser/dialect"
)

func TestTokenizer_Tokenize(t *testing.T) {
	cases := []struct {
		name string
		in   string
		out  []*Token
	}{
		{
			name: "whitespace",
			in:   " ",
			out: []*Token{
				{
					Kind:  Whitespace,
					Value: " ",
				},
			},
		},
		{
			name: "whitespace and new line",
			in: `
 `,
			out: []*Token{
				{
					Kind:  Whitespace,
					Value: "\n",
				},
				{
					Kind:  Whitespace,
					Value: " ",
				},
			},
		},
		{
			name: "whitespace and tab",
			in: "\r\n	",
			out: []*Token{
				{
					Kind:  Whitespace,
					Value: "\n",
				},
				{
					Kind:  Whitespace,
					Value: "\t",
				},
			},
		},
		{
			name: "N string",
			in:   "N'string'",
			out: []*Token{
				{
					Kind:  NationalStringLiteral,
					Value: "string",
				},
			},
		},
		{
			name: "N string with keyword",
			in:   "N'string' NOT",
			out: []*Token{
				{
					Kind:  NationalStringLiteral,
					Value: "string",
				},
				{
					Kind:  Whitespace,
					Value: " ",
				},
				{
					Kind: SQLKeyword,
					Value: &SQLWord{
						Value:   "NOT",
						Keyword: "NOT",
					},
				},
			},
		},
		{
			name: "Ident",
			in:   "select",
			out: []*Token{
				{
					Kind: SQLKeyword,
					Value: &SQLWord{
						Value:   "select",
						Keyword: "SELECT",
					},
				},
			},
		},
		{
			name: "single quote string",
			in:   "'test'",
			out: []*Token{
				{
					Kind:  SingleQuotedString,
					Value: "test",
				},
			},
		},
		{
			name: "quoted string",
			in:   "\"SELECT\"",
			out: []*Token{
				{
					Kind: SQLKeyword,
					Value: &SQLWord{
						Value:      "SELECT",
						Keyword:    "SELECT",
						QuoteStyle: '"',
					},
				},
			},
		},
		{
			name: "parents with number",
			in:   "(123),",
			out: []*Token{
				{
					Kind:  LParen,
					Value: "(",
				},
				{
					Kind:  Number,
					Value: "123",
				},
				{
					Kind:  RParen,
					Value: ")",
				},
				{
					Kind:  Comma,
					Value: ",",
				},
			},
		},
		{
			name: "minus comment",
			in:   "-- test",
			out: []*Token{
				{
					Kind:  Whitespace,
					Value: " test\n",
				},
			},
		},
		{
			name: "minus operator",
			in:   "1-3",
			out: []*Token{
				{
					Kind:  Number,
					Value: "1",
				},
				{
					Kind:  Minus,
					Value: "-",
				},
				{
					Kind:  Number,
					Value: "3",
				},
			},
		},
		{
			name: "/* comment",
			in: `/* test
multiline
comment */`,
			out: []*Token{
				{
					Kind:  Whitespace,
					Value: " test\nmultiline\ncomment ",
				},
			},
		},
		{
			name: "operators",
			in:   "1/1*1+1%1=1.1-.",
			out: []*Token{
				{
					Kind:  Number,
					Value: "1",
				},
				{
					Kind:  Div,
					Value: "/",
				},
				{
					Kind:  Number,
					Value: "1",
				},
				{
					Kind:  Mult,
					Value: "*",
				},
				{
					Kind:  Number,
					Value: "1",
				},
				{
					Kind:  Plus,
					Value: "+",
				},
				{
					Kind:  Number,
					Value: "1",
				},
				{
					Kind:  Mod,
					Value: "%",
				},
				{
					Kind:  Number,
					Value: "1",
				},
				{
					Kind:  Eq,
					Value: "=",
				},
				{
					Kind:  Number,
					Value: "1.1",
				},
				{
					Kind:  Minus,
					Value: "-",
				},
				{
					Kind:  Period,
					Value: ".",
				},
			},
		},
		{
			name: "Neq",
			in:   "1!=2",
			out: []*Token{
				{
					Kind:  Number,
					Value: "1",
				},
				{
					Kind:  Neq,
					Value: "!=",
				},
				{
					Kind:  Number,
					Value: "2",
				},
			},
		},
		{
			name: "Lts",
			in:   "<<=<>",
			out: []*Token{
				{
					Kind:  Lt,
					Value: "<",
				},
				{
					Kind:  LtEq,
					Value: "<=",
				},
				{
					Kind:  Neq,
					Value: "<>",
				},
			},
		},
		{
			name: "Gts",
			in:   ">>=",
			out: []*Token{
				{
					Kind:  Gt,
					Value: ">",
				},
				{
					Kind:  GtEq,
					Value: ">=",
				},
			},
		},
		{
			name: "colons",
			in:   ":1::1;",
			out: []*Token{
				{
					Kind:  Colon,
					Value: ":",
				},
				{
					Kind:  Number,
					Value: "1",
				},
				{
					Kind:  DoubleColon,
					Value: "::",
				},
				{
					Kind:  Number,
					Value: "1",
				},
				{
					Kind:  Semicolon,
					Value: ";",
				},
			},
		},
		{
			name: "others",
			in:   "\\[{&}]",
			out: []*Token{
				{
					Kind:  Backslash,
					Value: "\\",
				},
				{
					Kind:  LBracket,
					Value: "[",
				},
				{
					Kind:  LBrace,
					Value: "{",
				},
				{
					Kind:  Ampersand,
					Value: "&",
				},
				{
					Kind:  RBrace,
					Value: "}",
				},
				{
					Kind:  RBracket,
					Value: "]",
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src := strings.NewReader(c.in)
			tokenizer := NewTokenizer(src, &dialect.GenericSQLDialect{})

			tok, err := tokenizer.Tokenize()
			if err != nil {
				t.Errorf("should be no error %v", err)
			}

			if len(tok) != len(c.out) {
				t.Fatalf("should be same length but %d, %d", len(tok), len(c.out))
			}

			for i := 0; i < len(tok); i++ {
				if tok[i].Kind != c.out[i].Kind {
					t.Errorf("%d, expected token: %d, but got %d", i, c.out[i].Kind, tok[i].Kind)
				}
				if !reflect.DeepEqual(tok[i].Value, c.out[i].Value) {
					t.Errorf("%d, expected value: %+v, but got %+v", i, c.out[i].Value, tok[i].Value)
				}
			}
		})
	}
}
