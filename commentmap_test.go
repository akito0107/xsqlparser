package xsqlparser_test

import (
	"strings"
	"testing"

	"github.com/akito0107/xsqlparser"
	"github.com/akito0107/xsqlparser/dialect"
	"github.com/akito0107/xsqlparser/sqlast"
	"github.com/k0kubun/pp"
)

func TestNewCommentMap(t *testing.T) {

	t.Run("simple case", func(t *testing.T) {

		src := `
--test
SELECT * from test;
`

		parser, err := xsqlparser.NewParser(strings.NewReader(src), &dialect.GenericSQLDialect{}, xsqlparser.ParseComment())
		if err != nil {
			t.Fatal(err)
		}

		f, err := parser.ParseFile()
		if err != nil {
			t.Fatal(err)
		}

		m := sqlast.NewCommentMap(f)

		pp.Println(m)

	})

}
