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
			skip bool
		}{
			{
				skip: true,
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
			{
				name: "where",
				in:   "SELECT test FROM test_table WHERE test_table.column1 = 'test'",
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
						Selection: &sqlast.SQLBinaryExpr{
							Left:  sqlast.NewSQLObjectName("test_table", "column1"),
							Op:    sqlast.Eq,
							Right: sqlast.NewSingleQuotedString("test"),
						},
					},
				},
			},
		}

		for _, c := range cases {

			t.Run(c.name, func(t *testing.T) {
				if c.skip {
					t.Skip()
				}
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
