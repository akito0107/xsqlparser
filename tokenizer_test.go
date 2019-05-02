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
		out  []*TokenSet
	}{
		{
			name: "whitespace",
			in:   " ",
			out: []*TokenSet{
				{
					Tok:   Whitespace,
					Value: " ",
				},
			},
		},
		{
			name: "whitespace and new line",
			in: `
 `,
			out: []*TokenSet{
				{
					Tok:   Whitespace,
					Value: "\n",
				},
				{
					Tok:   Whitespace,
					Value: " ",
				},
			},
		},
		{
			name: "whitespace and tab",
			in: "\r\n	",
			out: []*TokenSet{
				{
					Tok:   Whitespace,
					Value: "\n",
				},
				{
					Tok:   Whitespace,
					Value: "\t",
				},
			},
		},
		{
			name: "N string",
			in:   "N'string'",
			out: []*TokenSet{
				{
					Tok:   NationalStringLiteral,
					Value: "string",
				},
			},
		},
		{
			name: "N string with keyword",
			in:   "N'string' NOT",
			out: []*TokenSet{
				{
					Tok:   NationalStringLiteral,
					Value: "string",
				},
				{
					Tok:   Whitespace,
					Value: " ",
				},
				{
					Tok: SQLKeyword,
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
			out: []*TokenSet{
				{
					Tok: SQLKeyword,
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
			out: []*TokenSet{
				{
					Tok:   SingleQuotedString,
					Value: "test",
				},
			},
		},
		{
			name: "quoted string",
			in:   "\"SELECT\"",
			out: []*TokenSet{
				{
					Tok: SQLKeyword,
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
			out: []*TokenSet{
				{
					Tok:   LParen,
					Value: "(",
				},
				{
					Tok:   Number,
					Value: "123",
				},
				{
					Tok:   RParen,
					Value: ")",
				},
				{
					Tok:   Comma,
					Value: ",",
				},
			},
		},
		{
			name: "minus comment",
			in:   "-- test",
			out: []*TokenSet{
				{
					Tok:   Whitespace,
					Value: " test\n",
				},
			},
		},
		{
			name: "minus operator",
			in:   "1-3",
			out: []*TokenSet{
				{
					Tok:   Number,
					Value: "1",
				},
				{
					Tok:   Minus,
					Value: "-",
				},
				{
					Tok:   Number,
					Value: "3",
				},
			},
		},
		{
			name: "/* comment",
			in: `/* test
multiline
comment */`,
			out: []*TokenSet{
				{
					Tok:   Whitespace,
					Value: " test\nmultiline\ncomment ",
				},
			},
		},
		{
			name: "operators",
			in:   "1/1*1+1%1=1.1-.",
			out: []*TokenSet{
				{
					Tok:   Number,
					Value: "1",
				},
				{
					Tok:   Div,
					Value: "/",
				},
				{
					Tok:   Number,
					Value: "1",
				},
				{
					Tok:   Mult,
					Value: "*",
				},
				{
					Tok:   Number,
					Value: "1",
				},
				{
					Tok:   Plus,
					Value: "+",
				},
				{
					Tok:   Number,
					Value: "1",
				},
				{
					Tok:   Mod,
					Value: "%",
				},
				{
					Tok:   Number,
					Value: "1",
				},
				{
					Tok:   Eq,
					Value: "=",
				},
				{
					Tok:   Number,
					Value: "1.1",
				},
				{
					Tok:   Minus,
					Value: "-",
				},
				{
					Tok:   Period,
					Value: ".",
				},
			},
		},
		{
			name: "Neq",
			in:   "1!=2",
			out: []*TokenSet{
				{
					Tok:   Number,
					Value: "1",
				},
				{
					Tok:   Neq,
					Value: "!=",
				},
				{
					Tok:   Number,
					Value: "2",
				},
			},
		},
		{
			name: "Lts",
			in:   "<<=<>",
			out: []*TokenSet{
				{
					Tok:   Lt,
					Value: "<",
				},
				{
					Tok:   LtEq,
					Value: "<=",
				},
				{
					Tok:   Neq,
					Value: "<>",
				},
			},
		},
		{
			name: "Gts",
			in:   ">>=",
			out: []*TokenSet{
				{
					Tok:   Gt,
					Value: ">",
				},
				{
					Tok:   GtEq,
					Value: ">=",
				},
			},
		},
		{
			name: "colons",
			in:   ":1::1;",
			out: []*TokenSet{
				{
					Tok:   Colon,
					Value: ":",
				},
				{
					Tok:   Number,
					Value: "1",
				},
				{
					Tok:   DoubleColon,
					Value: "::",
				},
				{
					Tok:   Number,
					Value: "1",
				},
				{
					Tok:   Semicolon,
					Value: ";",
				},
			},
		},
		{
			name: "others",
			in:   "\\[{&}]",
			out: []*TokenSet{
				{
					Tok:   Backslash,
					Value: "\\",
				},
				{
					Tok:   LBracket,
					Value: "[",
				},
				{
					Tok:   LBrace,
					Value: "{",
				},
				{
					Tok:   Ampersand,
					Value: "&",
				},
				{
					Tok:   RBrace,
					Value: "}",
				},
				{
					Tok:   RBracket,
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
				if tok[i].Tok != c.out[i].Tok {
					t.Errorf("%d, expected token: %d, but got %d", i, c.out[i].Tok, tok[i].Tok)
				}
				if !reflect.DeepEqual(tok[i].Value, c.out[i].Value) {
					t.Errorf("%d, expected value: %+v, but got %+v", i, c.out[i].Value, tok[i].Value)
				}
			}
		})
	}
}
