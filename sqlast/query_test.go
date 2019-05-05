package sqlast

import (
	"testing"

	"github.com/andreyvit/diff"
)

func TestSQLSelect_Eval(t *testing.T) {
	cases := []struct {
		name string
		in   *SQLSelect
		out  string
	}{
		{
			name: "simple select",
			in: &SQLSelect{
				Projection: []SQLSelectItem{
					&UnnamedExpression{
						Node: &SQLObjectName{
							Idents: []SQLIdent{"test"},
						},
					},
				},
				Relation: &Table{
					Name: &SQLObjectName{
						Idents: []SQLIdent{"test_table"},
					},
				},
			},
			out: "SELECT test FROM test_table",
		},
		{
			name: "join",
			in: &SQLSelect{
				Projection: []SQLSelectItem{
					&UnnamedExpression{
						Node: &SQLObjectName{
							Idents: []SQLIdent{"test"},
						},
					},
				},
				Relation: &Table{
					Name: &SQLObjectName{
						Idents: []SQLIdent{"test_table"},
					},
				},
				Joins: []Join{
					{
						Relation: &Table{
							Name: &SQLObjectName{
								Idents: []SQLIdent{"test_table2"},
							},
						},
						Op:       Inner,
						Constant: &NaturalConstant{},
					},
				},
			},
			out: "SELECT test FROM test_table NATURAL JOIN test_table2",
		},
		{
			name: "where",
			in: &SQLSelect{
				Projection: []SQLSelectItem{
					&UnnamedExpression{
						Node: &SQLObjectName{
							Idents: []SQLIdent{"test"},
						},
					},
				},
				Relation: &Table{
					Name: &SQLObjectName{
						Idents: []SQLIdent{"test_table"},
					},
				},
				Selection: &SQLBinaryExpr{
					Left: &SQLObjectName{
						Idents: []SQLIdent{"test_table", "column1"},
					},
					Op:    Eq,
					Right: NewSingleQuotedString("test"),
				},
			},
			out: "SELECT test FROM test_table WHERE test_table.column1 = 'test'",
		},
		{
			name: "count and join",
			in: &SQLSelect{
				Projection: []SQLSelectItem{
					&ExpressionWithAlias{
						Expr: &SQLFunction{
							Name: &SQLObjectName{
								Idents: []SQLIdent{"COUNT"},
							},
							Args: []ASTNode{&SQLObjectName{Idents: []SQLIdent{"t1", "id"}}},
						},
						Alias: NewSQLIdent("c"),
					},
				},
				Relation: &Table{
					Name: &SQLObjectName{
						Idents: []SQLIdent{"test_table"},
					},
					Alias: NewSQLIdent("t1"),
				},
				Joins: []Join{
					{
						Relation: &Table{
							Name: &SQLObjectName{
								Idents: []SQLIdent{"test_table2"},
							},
							Alias: NewSQLIdent("t2"),
						},
						Op: LeftOuter,
						Constant: &OnJoinConstant{
							Node: &SQLBinaryExpr{
								Left: &SQLObjectName{
									Idents: []SQLIdent{"t1", "id"},
								},
								Op: Eq,
								Right: &SQLObjectName{
									Idents: []SQLIdent{"t2", "test_table_id"},
								},
							},
						},
					},
				},
			},
			out: "SELECT COUNT(t1.id) AS c FROM test_table AS t1 LEFT JOIN test_table2 AS t2 ON t1.id = t2.test_table_id",
		},
		{
			name: "group by",
			in: &SQLSelect{
				Projection: []SQLSelectItem{
					&UnnamedExpression{
						Node: &SQLFunction{
							Name: &SQLObjectName{
								Idents: []SQLIdent{"COUNT"},
							},
							Args: []ASTNode{&SQLObjectName{Idents: []SQLIdent{"customer_id"}}},
						},
					},
					&QualifiedWildcard{
						Prefix: &SQLObjectName{
							Idents: []SQLIdent{"country"},
						},
					},
				},
				Relation: &Table{
					Name: &SQLObjectName{
						Idents: []SQLIdent{"customers"},
					},
				},
				GroupBy: []ASTNode{NewSQLIdent("country")},
			},
			out: "SELECT COUNT(customer_id), country.* FROM customers GROUP BY country",
		},
		{
			name: "having",
			in: &SQLSelect{
				Projection: []SQLSelectItem{
					&SQLFunction{
						Name: &SQLObjectName{
							Idents: []SQLIdent{"COUNT"},
						},
						Args: []ASTNode{&SQLObjectName{Idents: []SQLIdent{"customer_id"}}},
					},
					NewSQLIdent("country"),
				},
				Relation: &Table{
					Name: &SQLObjectName{
						Idents: []SQLIdent{"customers"},
					},
				},
				GroupBy: []ASTNode{NewSQLIdent("country")},
				Having: &SQLBinaryExpr{
					Op: Gt,
					Left: &SQLFunction{
						Name: &SQLObjectName{Idents: []SQLIdent{"COUNT"}},
						Args: []ASTNode{NewSQLIdent("customer_id")},
					},
					Right: NewLongValue(3),
				},
			},
			out: "SELECT COUNT(customer_id), country FROM customers GROUP BY country HAVING COUNT(customer_id) > 3",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			act := c.in.Eval()

			if act != c.out {
				t.Errorf("must be \n%s but \n%s \n diff: %s", c.out, act, diff.CharacterDiff(c.out, act))
			}
		})
	}

}

func TestSQLQuery_Eval(t *testing.T) {
	cases := []struct {
		name string
		in   *SQLQuery
		out  string
	}{
		{
			// from https://www.postgresql.jp/document/9.3/html/queries-with.html
			name: "with cte",
			in: &SQLQuery{
				CTEs: []CTE{
					{
						Alias: NewSQLIdent("regional_sales"),
						Query: &SQLQuery{
							Body: &SQLSelect{
								Projection: []SQLSelectItem{
									&UnnamedExpression{Node: NewSQLIdent("region")},
									&ExpressionWithAlias{
										Alias: NewSQLIdent("total_sales"),
										Expr: &SQLFunction{
											Name: NewSQLObjectName("SUM"),
											Args: []ASTNode{NewSQLIdent("amount")},
										},
									},
								},
								Relation: &Table{
									Name: NewSQLObjectName("orders"),
								},
								GroupBy: []ASTNode{NewSQLIdent("region")},
							},
						},
					},
				},
				Body: &SelectExpr{
					Select: &SQLSelect{
						Projection: []SQLSelectItem{
							&UnnamedExpression{Node: NewSQLIdent("product")},
							&ExpressionWithAlias{
								Alias: NewSQLIdent("product_units"),
								Expr: &SQLFunction{
									Name: NewSQLObjectName("SUM"),
									Args: []ASTNode{NewSQLIdent("quantity")},
								},
							},
						},
						Relation: &Table{
							Name: NewSQLObjectName("orders"),
						},
						Selection: &SQLInSubQuery{
							Expr: NewSQLIdent("region"),
							SubQuery: &SQLQuery{
								Body: &SelectExpr{
									Select: &SQLSelect{
										Projection: []SQLSelectItem{
											&UnnamedExpression{Node: NewSQLIdent("region")},
										},
										Relation: &Table{
											Name: NewSQLObjectName("top_regions"),
										},
									},
								},
							},
						},
						GroupBy: []ASTNode{NewSQLIdent("region"), NewSQLIdent("product")},
					},
				},
			},
			out: "WITH regional_sales AS (" +
				"SELECT region, SUM(amount) AS total_sales " +
				"FROM orders GROUP BY region) " +
				"SELECT product, SUM(quantity) AS product_units " +
				"FROM orders " +
				"WHERE region IN (SELECT region FROM top_regions) " +
				"GROUP BY region, product",
		},
		{
			name: "order by and limit",
			in: &SQLQuery{
				Body: &SelectExpr{
					Select: &SQLSelect{
						Projection: []SQLSelectItem{
							&UnnamedExpression{Node: NewSQLIdent("product")},
							&ExpressionWithAlias{
								Alias: NewSQLIdent("product_units"),
								Expr: &SQLFunction{
									Name: NewSQLObjectName("SUM"),
									Args: []ASTNode{NewSQLIdent("quantity")},
								},
							},
						},
						Relation: &Table{
							Name: NewSQLObjectName("orders"),
						},
						Selection: &SQLInSubQuery{
							Expr: NewSQLIdent("region"),
							SubQuery: &SQLQuery{
								Body: &SelectExpr{
									Select: &SQLSelect{
										Projection: []SQLSelectItem{
											&UnnamedExpression{Node: NewSQLIdent("region")},
										},
										Relation: &Table{
											Name: NewSQLObjectName("top_regions"),
										},
									},
								},
							},
						},
					},
				},
				OrderBy: []SQLOrderByExpr{
					{Expr: NewSQLIdent("product_units")},
				},
				Limit: NewLongValue(100),
			},
			out: "SELECT product, SUM(quantity) AS product_units " +
				"FROM orders " +
				"WHERE region IN (SELECT region FROM top_regions) " +
				"ORDER BY product_units LIMIT 100",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			act := c.in.Eval()

			if act != c.out {
				t.Errorf("must be \n%s but \n%s \n diff: %s", c.out, act, diff.CharacterDiff(c.out, act))
			}
		})
	}

}
