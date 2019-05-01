package xsqlparser

import (
	"reflect"
	"strings"
	"testing"

	"github.com/akito0107/xsqlparser/dialect"
)

func TestTokenizer_NextToken(t *testing.T) {
	t.Run("single character", func(t *testing.T) {

		cases := []struct {
			name string
			in   string
			out  *TokenSet
		}{
			{
				name: "tokenize whitespace",
				in:   " ",
				out: &TokenSet{
					Tok:   Whitespace,
					Value: " ",
				},
			},
		}

		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				src := strings.NewReader(c.in)
				tokenizer := Init(src, &dialect.GenericDialect{})

				tok, _, err := tokenizer.NextToken()
				if err != nil {
					t.Errorf("should be no error %v", err)
				}

				if !reflect.DeepEqual(tok, c.out) {
					t.Errorf("expected: %+v, but got %+v", c.out, tok)
				}

			})
		}
	})
}
