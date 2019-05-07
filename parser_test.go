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
							Left: &sqlast.SQLCompoundIdentifier{
								Idents: []*sqlast.SQLIdent{sqlast.NewSQLIdent("test_table"), sqlast.NewSQLIdent("column1")},
							},
							Op:    sqlast.Eq,
							Right: sqlast.NewSingleQuotedString("test"),
						},
					},
				},
			},
			{
				name: "count and join",
				in:   "SELECT COUNT(t1.id) AS c FROM test_table AS t1 LEFT JOIN test_table2 AS t2 ON t1.id = t2.test_table_id",
				out: &sqlast.SQLQuery{
					Body: &sqlast.SQLSelect{
						Projection: []sqlast.SQLSelectItem{
							&sqlast.ExpressionWithAlias{
								Expr: &sqlast.SQLFunction{
									Name: sqlast.NewSQLObjectName("COUNT"),
									Args: []sqlast.ASTNode{&sqlast.SQLCompoundIdentifier{
										Idents: []*sqlast.SQLIdent{sqlast.NewSQLIdent("t1"), sqlast.NewSQLIdent("id")},
									}},
								},
								Alias: sqlast.NewSQLIdent("c"),
							},
						},
						Relation: &sqlast.Table{
							Name:  sqlast.NewSQLObjectName("test_table"),
							Alias: sqlast.NewSQLIdent("t1"),
						},
						Joins: []*sqlast.Join{
							{
								Relation: &sqlast.Table{
									Name:  sqlast.NewSQLObjectName("test_table2"),
									Alias: sqlast.NewSQLIdent("t2"),
								},
								Op: sqlast.LeftOuter,
								Constant: &sqlast.OnJoinConstant{
									Node: &sqlast.SQLBinaryExpr{
										Left: &sqlast.SQLCompoundIdentifier{
											Idents: []*sqlast.SQLIdent{sqlast.NewSQLIdent("t1"), sqlast.NewSQLIdent("id")},
										},
										Op: sqlast.Eq,
										Right: &sqlast.SQLCompoundIdentifier{
											Idents: []*sqlast.SQLIdent{sqlast.NewSQLIdent("t2"), sqlast.NewSQLIdent("test_table_id")},
										},
									},
								},
							},
						},
					},
				},
			},
			{
				name: "group by",
				in:   "SELECT COUNT(customer_id), country.* FROM customers GROUP BY country",
				out: &sqlast.SQLQuery{
					Body: &sqlast.SQLSelect{
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedExpression{
								Node: &sqlast.SQLFunction{
									Name: sqlast.NewSQLObjectName("COUNT"),
									Args: []sqlast.ASTNode{sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("customer_id"))},
								},
							},
							&sqlast.QualifiedWildcard{
								Prefix: sqlast.NewSQLObjectName("country"),
							},
						},
						Relation: &sqlast.Table{
							Name: sqlast.NewSQLObjectName("customers"),
						},
						GroupBy: []sqlast.ASTNode{sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("country"))},
					},
				},
			},
			{
				name: "having",
				in:   "SELECT COUNT(customer_id), country FROM customers GROUP BY country HAVING COUNT(customer_id) > 3",
				out: &sqlast.SQLQuery{
					Body: &sqlast.SQLSelect{
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedExpression{
								Node: &sqlast.SQLFunction{
									Name: sqlast.NewSQLObjectName("COUNT"),
									Args: []sqlast.ASTNode{sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("customer_id"))},
								},
							},
							&sqlast.UnnamedExpression{
								Node: sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("country")),
							},
						},
						Relation: &sqlast.Table{
							Name: sqlast.NewSQLObjectName("customers"),
						},
						GroupBy: []sqlast.ASTNode{sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("country"))},
						Having: &sqlast.SQLBinaryExpr{
							Op: sqlast.Gt,
							Left: &sqlast.SQLFunction{
								Name: sqlast.NewSQLObjectName("COUNT"),
								Args: []sqlast.ASTNode{sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("customer_id"))},
							},
							Right: sqlast.NewLongValue(3),
						},
					},
				},
			},
			{
				name: "order by and limit",
				out: &sqlast.SQLQuery{
					Body: &sqlast.SQLSelect{
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedExpression{Node: sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("product"))},
							&sqlast.ExpressionWithAlias{
								Alias: sqlast.NewSQLIdent("product_units"),
								Expr: &sqlast.SQLFunction{
									Name: sqlast.NewSQLObjectName("SUM"),
									Args: []sqlast.ASTNode{sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("quantity"))},
								},
							},
						},
						Relation: &sqlast.Table{
							Name: sqlast.NewSQLObjectName("orders"),
						},
						Selection: &sqlast.SQLInSubQuery{
							Expr: sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("region")),
							SubQuery: &sqlast.SQLQuery{
								Body: &sqlast.SQLSelect{
									Projection: []sqlast.SQLSelectItem{
										&sqlast.UnnamedExpression{Node: sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("region"))},
									},
									Relation: &sqlast.Table{
										Name: sqlast.NewSQLObjectName("top_regions"),
									},
								},
							},
						},
					},
					OrderBy: []*sqlast.SQLOrderByExpr{
						{Expr: sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("product_units"))},
					},
					Limit: sqlast.NewLongValue(100),
				},
				in: "SELECT product, SUM(quantity) AS product_units " +
					"FROM orders " +
					"WHERE region IN (SELECT region FROM top_regions) " +
					"ORDER BY product_units LIMIT 100",
			},
			{
				// from https://www.postgresql.jp/document/9.3/html/queries-with.html
				name: "with cte",
				out: &sqlast.SQLQuery{
					CTEs: []*sqlast.CTE{
						{
							Alias: sqlast.NewSQLIdent("regional_sales"),
							Query: &sqlast.SQLQuery{
								Body: &sqlast.SQLSelect{
									Projection: []sqlast.SQLSelectItem{
										&sqlast.UnnamedExpression{Node: sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("region"))},
										&sqlast.ExpressionWithAlias{
											Alias: sqlast.NewSQLIdent("total_sales"),
											Expr: &sqlast.SQLFunction{
												Name: sqlast.NewSQLObjectName("SUM"),
												Args: []sqlast.ASTNode{sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("amount"))},
											},
										},
									},
									Relation: &sqlast.Table{
										Name: sqlast.NewSQLObjectName("orders"),
									},
									GroupBy: []sqlast.ASTNode{sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("region"))},
								},
							},
						},
					},
					Body: &sqlast.SQLSelect{
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedExpression{Node: sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("product"))},
							&sqlast.ExpressionWithAlias{
								Alias: sqlast.NewSQLIdent("product_units"),
								Expr: &sqlast.SQLFunction{
									Name: sqlast.NewSQLObjectName("SUM"),
									Args: []sqlast.ASTNode{sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("quantity"))},
								},
							},
						},
						Relation: &sqlast.Table{
							Name: sqlast.NewSQLObjectName("orders"),
						},
						Selection: &sqlast.SQLInSubQuery{
							Expr: sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("region")),
							SubQuery: &sqlast.SQLQuery{
								Body: &sqlast.SQLSelect{
									Projection: []sqlast.SQLSelectItem{
										&sqlast.UnnamedExpression{Node: sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("region"))},
									},
									Relation: &sqlast.Table{
										Name: sqlast.NewSQLObjectName("top_regions"),
									},
								},
							},
						},
						GroupBy: []sqlast.ASTNode{sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("region")), sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("product"))},
					},
				},
				in: "WITH regional_sales AS (" +
					"SELECT region, SUM(amount) AS total_sales " +
					"FROM orders GROUP BY region) " +
					"SELECT product, SUM(quantity) AS product_units " +
					"FROM orders " +
					"WHERE region IN (SELECT region FROM top_regions) " +
					"GROUP BY region, product",
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

	t.Run("create", func(t *testing.T) {
		cases := []struct {
			name string
			in   string
			out  sqlast.SQLStmt
			skip bool
		}{
			{
				name: "create table",
				in: "CREATE TABLE persons (" +
					"person_id UUID PRIMARY KEY NOT NULL, " +
					"first_name varchar(255) UNIQUE, " +
					"last_name character varying(255) NOT NULL, " +
					"created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL)",
				out: &sqlast.SQLCreateTable{
					Name: sqlast.NewSQLObjectName("persons"),
					Columns: []*sqlast.SQLColumnDef{
						{
							Name:      sqlast.NewSQLIdent("person_id"),
							DateType:  &sqlast.UUID{},
							IsPrimary: true,
						},
						{
							Name: sqlast.NewSQLIdent("first_name"),
							DateType: &sqlast.VarcharType{
								Size: sqlast.NewSize(255),
							},
							AllowNull: true,
							IsUnique:  true,
						},
						{
							Name: sqlast.NewSQLIdent("last_name"),
							DateType: &sqlast.VarcharType{
								Size: sqlast.NewSize(255),
							},
						},
						{
							Name:     sqlast.NewSQLIdent("created_at"),
							DateType: &sqlast.Timestamp{},
							Default:  sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("CURRENT_TIMESTAMP")),
						},
					},
				},
			},
			{
				name: "create view",
				in:   "CREATE VIEW comedies AS SELECT * FROM films WHERE kind = 'Comedy'",
				out: &sqlast.SQLCreateView{
					Name: sqlast.NewSQLObjectName("comedies"),
					Query: &sqlast.SQLQuery{
						Body: &sqlast.SQLSelect{
							Projection: []sqlast.SQLSelectItem{&sqlast.UnnamedExpression{Node: &sqlast.SQLWildcard{}}},
							Relation: &sqlast.Table{
								Name: sqlast.NewSQLObjectName("films"),
							},
							Selection: &sqlast.SQLBinaryExpr{
								Op:    sqlast.Eq,
								Left:  sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("kind")),
								Right: sqlast.NewSingleQuotedString("Comedy"),
							},
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
