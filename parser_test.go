package xsqlparser

import (
	"bytes"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/akito0107/xsqlparser/dialect"
	"github.com/akito0107/xsqlparser/sqlast"
)

func TestParser_ParseStatement(t *testing.T) {
	t.Run("Select", func(t *testing.T) {

		cases := []struct {
			name string
			in   string
			out  sqlast.SQLStmt
		}{
			{
				name: "simple select",
				in:   "SELECT test FROM test_table",
				out: &sqlast.SQLQuery{
					Body: &sqlast.SQLSelect{
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedExpression{
								Node: sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("test")),
							},
						},
						Relation: &sqlast.Table{
							Name: sqlast.NewSQLObjectName("test_table"),
						},
					},
				},
			},
		}

		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				parser, err := NewParser(bytes.NewBufferString(c.in), &dialect.GenericSQLDialect{})
				if err != nil {
					t.Fatal(err)
				}
				ast, err := parser.ParseStatement()
				if err != nil {
					t.Fatal(err)
				}

				if diff := cmp.Diff(c.out, ast); diff != "" {
					t.Errorf("diff %s", diff)
				}
			})
		}
	})
}
