package xsqlparser_test

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/akito0107/xsqlparser"
	"github.com/akito0107/xsqlparser/dialect"
	"github.com/akito0107/xsqlparser/sqlast"
	"github.com/akito0107/xsqlparser/sqltoken"
)

func parseFile(t *testing.T, src string) *sqlast.File {
	t.Helper()
	parser, err := xsqlparser.NewParser(strings.NewReader(src), &dialect.GenericSQLDialect{}, xsqlparser.ParseComment)
	if err != nil {
		t.Fatal(err)
	}

	f, err := parser.ParseFile()
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestNewCommentMap(t *testing.T) {

	t.Run("associate with single statements", func(t *testing.T) {
		f := parseFile(t, `
--test
SELECT * from test;
`)

		m := sqlast.NewCommentMap(f)

		if diff := cmp.Diff(m[f.Stmts[0]], []*sqlast.CommentGroup{
			{
				List: []*sqlast.Comment{
					{
						Text: "test",
						From: sqltoken.NewPos(2, 0),
						To:   sqltoken.NewPos(2, 6),
					},
				},
			},
		}); diff != "" {
			t.Error(diff)
		}
	})
}
