package xsqlparser

import (
	"bytes"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/akito0107/xsqlparser/dialect"
	"github.com/akito0107/xsqlparser/sqlast"
	"github.com/akito0107/xsqlparser/sqltoken"
)

func TestParser_ParseStatement(t *testing.T) {
	t.Run("select", func(t *testing.T) {

		cases := []struct {
			name string
			in   string
			out  sqlast.Stmt
			skip bool
		}{
			{
				name: "simple select",
				in:   "SELECT test FROM test_table",
				out: &sqlast.QueryStmt{
					Body: &sqlast.SQLSelect{
						Select: sqltoken.NewPos(1, 1),
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedSelectItem{
								Node: sqlast.NewIdentWithPos(
									"test",
									sqltoken.NewPos(1, 8),
									sqltoken.NewPos(1, 12),
								),
							},
						},
						FromClause: []sqlast.TableReference{
							&sqlast.Table{
								Name: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										sqlast.NewIdentWithPos(
											"test_table",
											sqltoken.NewPos(1, 18),
											sqltoken.NewPos(1, 28),
										),
									},
								},
							},
						},
					},
				},
			},
			{
				name: "where",
				in:   "SELECT test FROM test_table WHERE test_table.column1 = 'test'",
				out: &sqlast.QueryStmt{
					Body: &sqlast.SQLSelect{
						Select: sqltoken.NewPos(1, 1),
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedSelectItem{
								Node: sqlast.NewIdentWithPos(
									"test",
									sqltoken.NewPos(1, 8),
									sqltoken.NewPos(1, 12),
								),
							},
						},
						FromClause: []sqlast.TableReference{
							&sqlast.Table{
								Name: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										sqlast.NewIdentWithPos(
											"test_table",
											sqltoken.NewPos(1, 18),
											sqltoken.NewPos(1, 28),
										),
									},
								},
							},
						},
						WhereClause: &sqlast.BinaryExpr{
							Left: &sqlast.CompoundIdent{
								Idents: []*sqlast.Ident{
									sqlast.NewIdentWithPos(
										"test_table",
										sqltoken.NewPos(1, 35),
										sqltoken.NewPos(1, 45),
									),
									sqlast.NewIdentWithPos(
										"column1",
										sqltoken.NewPos(1, 46),
										sqltoken.NewPos(1, 53),
									),
								},
							},
							Op: &sqlast.Operator{
								Type: sqlast.Eq,
								From: sqltoken.NewPos(1, 54),
								To:   sqltoken.NewPos(1, 55),
							},
							Right: &sqlast.SingleQuotedString{
								From:   sqltoken.NewPos(1, 56),
								To:     sqltoken.NewPos(1, 62),
								String: "test",
							},
						},
					},
				},
			},
			{
				name: "count and join",
				in:   "SELECT COUNT(t1.id) AS c FROM test_table AS t1 LEFT JOIN test_table2 AS t2 ON t1.id = t2.test_table_id",
				out: &sqlast.QueryStmt{
					Body: &sqlast.SQLSelect{
						Select: sqltoken.NewPos(1, 1),
						Projection: []sqlast.SQLSelectItem{
							&sqlast.AliasSelectItem{
								Expr: &sqlast.Function{
									Name: &sqlast.ObjectName{
										Idents: []*sqlast.Ident{
											sqlast.NewIdentWithPos(
												"COUNT",
												sqltoken.NewPos(1, 8),
												sqltoken.NewPos(1, 13),
											),
										},
									},
									Args: []sqlast.Node{&sqlast.CompoundIdent{
										Idents: []*sqlast.Ident{
											sqlast.NewIdentWithPos(
												"t1",
												sqltoken.NewPos(1, 14),
												sqltoken.NewPos(1, 16),
											),
											sqlast.NewIdentWithPos(
												"id",
												sqltoken.NewPos(1, 17),
												sqltoken.NewPos(1, 19),
											),
										},
									}},
									ArgsRParen: sqltoken.NewPos(1, 20),
								},
								Alias: &sqlast.Ident{
									Value: "c",
									From:  sqltoken.NewPos(1, 24),
									To:    sqltoken.NewPos(1, 25),
								},
							},
						},
						FromClause: []sqlast.TableReference{
							&sqlast.QualifiedJoin{
								LeftElement: &sqlast.TableJoinElement{
									Ref: &sqlast.Table{
										Name: &sqlast.ObjectName{
											Idents: []*sqlast.Ident{
												{
													Value: "test_table",
													From:  sqltoken.NewPos(1, 31),
													To:    sqltoken.NewPos(1, 41),
												},
											},
										},
										Alias: &sqlast.Ident{
											Value: "t1",
											From:  sqltoken.NewPos(1, 45),
											To:    sqltoken.NewPos(1, 47),
										},
									},
								},
								Type: &sqlast.JoinType{
									Condition: sqlast.LEFT,
									From:      sqltoken.NewPos(1, 48),
									To:        sqltoken.NewPos(1, 52),
								},
								RightElement: &sqlast.TableJoinElement{
									Ref: &sqlast.Table{
										Name: &sqlast.ObjectName{
											Idents: []*sqlast.Ident{
												{
													Value: "test_table2",
													From:  sqltoken.NewPos(1, 58),
													To:    sqltoken.NewPos(1, 69),
												},
											},
										},
										Alias: &sqlast.Ident{
											Value: "t2",
											From:  sqltoken.NewPos(1, 73),
											To:    sqltoken.NewPos(1, 75),
										},
									},
								},
								Spec: &sqlast.JoinCondition{
									On: sqltoken.NewPos(1, 76),
									SearchCondition: &sqlast.BinaryExpr{
										Left: &sqlast.CompoundIdent{
											Idents: []*sqlast.Ident{
												{
													Value: "t1",
													From:  sqltoken.NewPos(1, 79),
													To:    sqltoken.NewPos(1, 81),
												},
												{
													Value: "id",
													From:  sqltoken.NewPos(1, 82),
													To:    sqltoken.NewPos(1, 84),
												},
											},
										},
										Op: &sqlast.Operator{
											Type: sqlast.Eq,
											From: sqltoken.NewPos(1, 85),
											To:   sqltoken.NewPos(1, 86),
										},
										Right: &sqlast.CompoundIdent{
											Idents: []*sqlast.Ident{
												{
													Value: "t2",
													From:  sqltoken.NewPos(1, 87),
													To:    sqltoken.NewPos(1, 89),
												},
												{
													Value: "test_table_id",
													From:  sqltoken.NewPos(1, 90),
													To:    sqltoken.NewPos(1, 103),
												},
											},
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
				out: &sqlast.QueryStmt{
					Body: &sqlast.SQLSelect{
						Select: sqltoken.NewPos(1, 1),
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedSelectItem{
								Node: &sqlast.Function{
									Name: &sqlast.ObjectName{
										Idents: []*sqlast.Ident{
											{
												Value: "COUNT",
												From:  sqltoken.NewPos(1, 8),
												To:    sqltoken.NewPos(1, 13),
											},
										},
									},
									Args: []sqlast.Node{
										&sqlast.Ident{
											Value: "customer_id",
											From:  sqltoken.NewPos(1, 14),
											To:    sqltoken.NewPos(1, 25),
										},
									},
									ArgsRParen: sqltoken.NewPos(1, 26),
								},
							},
							&sqlast.QualifiedWildcardSelectItem{
								Prefix: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										{
											Value: "country",
											From:  sqltoken.NewPos(1, 28),
											To:    sqltoken.NewPos(1, 35),
										},
									},
								},
							},
						},
						FromClause: []sqlast.TableReference{
							&sqlast.Table{
								Name: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										{
											Value: "customers",
											From:  sqltoken.NewPos(1, 43),
											To:    sqltoken.NewPos(1, 52),
										},
									},
								},
							},
						},
						GroupByClause: []sqlast.Node{
							&sqlast.Ident{
								Value: "country",
								From:  sqltoken.NewPos(1, 62),
								To:    sqltoken.NewPos(1, 69),
							},
						},
					},
				},
			},
			{
				name: "having",
				in: `SELECT COUNT(customer_id), country 
FROM customers 
GROUP BY country 
HAVING COUNT(customer_id) > 3`,
				out: &sqlast.QueryStmt{
					Body: &sqlast.SQLSelect{
						Select: sqltoken.NewPos(1, 1),
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedSelectItem{
								Node: &sqlast.Function{
									Name: &sqlast.ObjectName{
										Idents: []*sqlast.Ident{
											{
												Value: "COUNT",
												From:  sqltoken.NewPos(1, 8),
												To:    sqltoken.NewPos(1, 13),
											},
										},
									},
									Args: []sqlast.Node{
										&sqlast.Ident{
											Value: "customer_id",
											From:  sqltoken.NewPos(1, 14),
											To:    sqltoken.NewPos(1, 25),
										},
									},
									ArgsRParen: sqltoken.NewPos(1, 26),
								},
							},
							&sqlast.UnnamedSelectItem{
								Node: &sqlast.Ident{
									Value: "country",
									From:  sqltoken.NewPos(1, 28),
									To:    sqltoken.NewPos(1, 35),
								},
							},
						},
						FromClause: []sqlast.TableReference{
							&sqlast.Table{
								Name: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										{
											Value: "customers",
											From:  sqltoken.NewPos(2, 6),
											To:    sqltoken.NewPos(2, 15),
										},
									},
								},
							},
						},
						GroupByClause: []sqlast.Node{
							&sqlast.Ident{
								Value: "country",
								From:  sqltoken.NewPos(3, 10),
								To:    sqltoken.NewPos(3, 17),
							},
						},
						HavingClause: &sqlast.BinaryExpr{
							Op: &sqlast.Operator{
								Type: sqlast.Gt,
								From: sqltoken.NewPos(4, 27),
								To:   sqltoken.NewPos(4, 28),
							},
							Left: &sqlast.Function{
								Name: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										{
											Value: "COUNT",
											From:  sqltoken.NewPos(4, 8),
											To:    sqltoken.NewPos(4, 13),
										},
									},
								},
								Args: []sqlast.Node{
									&sqlast.Ident{
										Value: "customer_id",
										From:  sqltoken.NewPos(4, 14),
										To:    sqltoken.NewPos(4, 25),
									},
								},
								ArgsRParen: sqltoken.NewPos(4, 26),
							},
							Right: &sqlast.LongValue{
								From: sqltoken.NewPos(4, 29),
								To:   sqltoken.NewPos(4, 30),
								Long: 3,
							},
						},
					},
				},
			},
			{
				name: "order by and limit",
				in: `SELECT product, SUM(quantity) AS product_units
FROM orders 
WHERE region IN (SELECT region FROM top_regions) 
ORDER BY product_units LIMIT 100`,
				out: &sqlast.QueryStmt{
					Body: &sqlast.SQLSelect{
						Select: sqltoken.NewPos(1, 1),
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedSelectItem{
								Node: &sqlast.Ident{
									Value: "product",
									From:  sqltoken.NewPos(1, 8),
									To:    sqltoken.NewPos(1, 15),
								},
							},
							&sqlast.AliasSelectItem{
								Alias: &sqlast.Ident{
									Value: "product_units",
									From:  sqltoken.NewPos(1, 34),
									To:    sqltoken.NewPos(1, 47),
								},
								Expr: &sqlast.Function{
									Name: &sqlast.ObjectName{
										Idents: []*sqlast.Ident{
											{
												Value: "SUM",
												From:  sqltoken.NewPos(1, 17),
												To:    sqltoken.NewPos(1, 20),
											},
										},
									},
									Args: []sqlast.Node{
										&sqlast.Ident{
											Value: "quantity",
											From:  sqltoken.NewPos(1, 21),
											To:    sqltoken.NewPos(1, 29),
										},
									},
									ArgsRParen: sqltoken.NewPos(1, 30),
								},
							},
						},
						FromClause: []sqlast.TableReference{
							&sqlast.Table{
								Name: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										{
											Value: "orders",
											From:  sqltoken.NewPos(2, 6),
											To:    sqltoken.NewPos(2, 12),
										},
									},
								},
							},
						},
						WhereClause: &sqlast.InSubQuery{
							Expr: &sqlast.Ident{
								Value: "region",
								From:  sqltoken.NewPos(3, 7),
								To:    sqltoken.NewPos(3, 13),
							},
							RParen: sqltoken.NewPos(3, 49),
							SubQuery: &sqlast.QueryStmt{
								Body: &sqlast.SQLSelect{
									Select: sqltoken.NewPos(3, 18),
									Projection: []sqlast.SQLSelectItem{
										&sqlast.UnnamedSelectItem{
											Node: &sqlast.Ident{
												Value: "region",
												From:  sqltoken.NewPos(3, 25),
												To:    sqltoken.NewPos(3, 31),
											},
										},
									},
									FromClause: []sqlast.TableReference{
										&sqlast.Table{
											Name: &sqlast.ObjectName{
												Idents: []*sqlast.Ident{
													{
														Value: "top_regions",
														From:  sqltoken.NewPos(3, 37),
														To:    sqltoken.NewPos(3, 48),
													},
												},
											},
										},
									},
								},
							},
						},
					},
					OrderBy: []*sqlast.OrderByExpr{
						{
							Expr: &sqlast.Ident{
								Value: "product_units",
								From:  sqltoken.NewPos(4, 10),
								To:    sqltoken.NewPos(4, 23),
							},
						},
					},
					Limit: &sqlast.LimitExpr{
						LimitValue: &sqlast.LongValue{
							From: sqltoken.NewPos(4, 30),
							To:   sqltoken.NewPos(4, 33),
							Long: 100,
						},
					},
				},
			},
			{
				// from https://www.postgresql.jp/document/9.3/html/queries-with.html
				name: "with cte",
				in: `WITH regional_sales AS (SELECT region, SUM(amount) AS total_sales FROM orders GROUP BY region)
SELECT product, SUM(quantity) AS product_units
FROM orders
WHERE region IN (SELECT region FROM top_regions)
GROUP BY region, product`,
				out: &sqlast.QueryStmt{
					CTEs: []*sqlast.CTE{
						{
							Alias: &sqlast.Ident{
								Value: "regional_sales",
								From:  sqltoken.NewPos(1, 6),
								To:    sqltoken.NewPos(1, 20),
							},
							Query: &sqlast.QueryStmt{
								Body: &sqlast.SQLSelect{
									Select: sqltoken.NewPos(1, 25),
									Projection: []sqlast.SQLSelectItem{
										&sqlast.UnnamedSelectItem{
											Node: &sqlast.Ident{
												Value: "region",
												From:  sqltoken.NewPos(1, 32),
												To:    sqltoken.NewPos(1, 38),
											},
										},
										&sqlast.AliasSelectItem{
											Alias: &sqlast.Ident{
												Value: "total_sales",
												From:  sqltoken.NewPos(1, 55),
												To:    sqltoken.NewPos(1, 66),
											},
											Expr: &sqlast.Function{
												Name: &sqlast.ObjectName{
													Idents: []*sqlast.Ident{
														{
															Value: "SUM",
															From:  sqltoken.NewPos(1, 40),
															To:    sqltoken.NewPos(1, 43),
														},
													},
												},
												Args: []sqlast.Node{
													&sqlast.Ident{
														Value: "amount",
														From:  sqltoken.NewPos(1, 44),
														To:    sqltoken.NewPos(1, 50),
													},
												},
												ArgsRParen: sqltoken.NewPos(1, 51),
											},
										},
									},
									FromClause: []sqlast.TableReference{
										&sqlast.Table{
											Name: &sqlast.ObjectName{
												Idents: []*sqlast.Ident{
													{
														Value: "orders",
														From:  sqltoken.NewPos(1, 72),
														To:    sqltoken.NewPos(1, 78),
													},
												},
											},
										},
									},
									GroupByClause: []sqlast.Node{
										&sqlast.Ident{
											Value: "region",
											From:  sqltoken.NewPos(1, 88),
											To:    sqltoken.NewPos(1, 94),
										},
									},
								},
							},
						},
					},
					Body: &sqlast.SQLSelect{
						Select: sqltoken.NewPos(2, 1),
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedSelectItem{Node: &sqlast.Ident{
								Value: "product",
								From:  sqltoken.NewPos(2, 8),
								To:    sqltoken.NewPos(2, 15),
							}},
							&sqlast.AliasSelectItem{
								Alias: &sqlast.Ident{
									Value: "product_units",
									From:  sqltoken.NewPos(2, 34),
									To:    sqltoken.NewPos(2, 47),
								},
								Expr: &sqlast.Function{
									Name: &sqlast.ObjectName{
										Idents: []*sqlast.Ident{
											{
												Value: "SUM",
												From:  sqltoken.NewPos(2, 17),
												To:    sqltoken.NewPos(2, 20),
											},
										},
									},
									Args: []sqlast.Node{
										&sqlast.Ident{
											Value: "quantity",
											From:  sqltoken.NewPos(2, 21),
											To:    sqltoken.NewPos(2, 29),
										},
									},
									ArgsRParen: sqltoken.NewPos(2, 30),
								},
							},
						},
						FromClause: []sqlast.TableReference{
							&sqlast.Table{
								Name: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										{
											Value: "orders",
											From:  sqltoken.NewPos(3, 6),
											To:    sqltoken.NewPos(3, 12),
										},
									},
								},
							},
						},
						WhereClause: &sqlast.InSubQuery{
							RParen: sqltoken.NewPos(4, 49),
							Expr: &sqlast.Ident{
								Value: "region",
								From:  sqltoken.NewPos(4, 7),
								To:    sqltoken.NewPos(4, 13),
							},
							SubQuery: &sqlast.QueryStmt{
								Body: &sqlast.SQLSelect{
									Select: sqltoken.NewPos(4, 18),
									Projection: []sqlast.SQLSelectItem{
										&sqlast.UnnamedSelectItem{
											Node: &sqlast.Ident{
												Value: "region",
												From:  sqltoken.NewPos(4, 25),
												To:    sqltoken.NewPos(4, 31),
											},
										},
									},
									FromClause: []sqlast.TableReference{
										&sqlast.Table{
											Name: &sqlast.ObjectName{
												Idents: []*sqlast.Ident{
													{
														Value: "top_regions",
														From:  sqltoken.NewPos(4, 37),
														To:    sqltoken.NewPos(4, 48),
													},
												},
											},
										},
									},
								},
							},
						},
						GroupByClause: []sqlast.Node{
							&sqlast.Ident{
								Value: "region",
								From:  sqltoken.NewPos(5, 10),
								To:    sqltoken.NewPos(5, 16),
							},
							&sqlast.Ident{
								Value: "product",
								From:  sqltoken.NewPos(5, 18),
								To:    sqltoken.NewPos(5, 25),
							},
						},
					},
				},
			},
			{
				name: "exists",
				in: `SELECT * FROM user WHERE NOT EXISTS 
(SELECT * 
FROM user_sub 
WHERE user.id = user_sub.id AND user_sub.job = 'job');`,
				out: &sqlast.QueryStmt{
					Body: &sqlast.SQLSelect{
						Select: sqltoken.NewPos(1, 1),
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedSelectItem{
								Node: &sqlast.Wildcard{
									Wildcard: sqltoken.NewPos(1, 8),
								},
							},
						},
						FromClause: []sqlast.TableReference{
							&sqlast.Table{
								Name: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										{
											Value: "user",
											From:  sqltoken.NewPos(1, 15),
											To:    sqltoken.NewPos(1, 19),
										},
									},
								},
							},
						},
						WhereClause: &sqlast.Exists{
							Negated: true,
							Exists:  sqltoken.NewPos(1, 30),
							Not:     sqltoken.NewPos(1, 26),
							RParen:  sqltoken.NewPos(4, 54),
							Query: &sqlast.QueryStmt{
								Body: &sqlast.SQLSelect{
									Select: sqltoken.NewPos(2, 2),
									Projection: []sqlast.SQLSelectItem{
										&sqlast.UnnamedSelectItem{
											Node: &sqlast.Wildcard{
												Wildcard: sqltoken.NewPos(2, 9),
											},
										},
									},
									FromClause: []sqlast.TableReference{
										&sqlast.Table{
											Name: &sqlast.ObjectName{
												Idents: []*sqlast.Ident{
													{
														Value: "user_sub",
														From:  sqltoken.NewPos(3, 6),
														To:    sqltoken.NewPos(3, 14),
													},
												},
											},
										},
									},
									WhereClause: &sqlast.BinaryExpr{
										Op: &sqlast.Operator{Type: sqlast.And, From: sqltoken.NewPos(4, 29), To: sqltoken.NewPos(4, 32)},
										Left: &sqlast.BinaryExpr{
											Op: &sqlast.Operator{Type: sqlast.Eq, From: sqltoken.NewPos(4, 15), To: sqltoken.NewPos(4, 16)},
											Left: &sqlast.CompoundIdent{
												Idents: []*sqlast.Ident{
													{
														Value: "user",
														From:  sqltoken.NewPos(4, 7),
														To:    sqltoken.NewPos(4, 11),
													},
													{
														Value: "id",
														From:  sqltoken.NewPos(4, 12),
														To:    sqltoken.NewPos(4, 14),
													},
												},
											},
											Right: &sqlast.CompoundIdent{
												Idents: []*sqlast.Ident{
													{
														Value: "user_sub",
														From:  sqltoken.NewPos(4, 17),
														To:    sqltoken.NewPos(4, 25),
													},
													{
														Value: "id",
														From:  sqltoken.NewPos(4, 26),
														To:    sqltoken.NewPos(4, 28),
													},
												},
											},
										},
										Right: &sqlast.BinaryExpr{
											Op: &sqlast.Operator{Type: sqlast.Eq, From: sqltoken.NewPos(4, 46), To: sqltoken.NewPos(4, 47)},
											Left: &sqlast.CompoundIdent{
												Idents: []*sqlast.Ident{
													{
														Value: "user_sub",
														From:  sqltoken.NewPos(4, 33),
														To:    sqltoken.NewPos(4, 41),
													},
													{
														Value: "job",
														From:  sqltoken.NewPos(4, 42),
														To:    sqltoken.NewPos(4, 45),
													},
												},
											},
											Right: &sqlast.SingleQuotedString{
												From:   sqltoken.NewPos(4, 48),
												To:     sqltoken.NewPos(4, 53),
												String: "job",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			{
				name: "between / case",
				in: `SELECT 
CASE
 WHEN expr1 = '1' THEN 'test1' 
 WHEN expr2 = '2' THEN 'test2' 
 ELSE 'other' 
END AS alias
FROM user WHERE id BETWEEN 1 AND 2`,
				out: &sqlast.QueryStmt{
					Body: &sqlast.SQLSelect{
						Select: sqltoken.NewPos(1, 1),
						Projection: []sqlast.SQLSelectItem{
							&sqlast.AliasSelectItem{
								Expr: &sqlast.CaseExpr{
									Case:    sqltoken.NewPos(2, 1),
									CaseEnd: sqltoken.NewPos(6, 4),
									Conditions: []sqlast.Node{
										&sqlast.BinaryExpr{
											Op: &sqlast.Operator{
												Type: sqlast.Eq,
												From: sqltoken.NewPos(3, 13),
												To:   sqltoken.NewPos(3, 14),
											},
											Left: &sqlast.Ident{
												Value: "expr1",
												From:  sqltoken.NewPos(3, 7),
												To:    sqltoken.NewPos(3, 12),
											},
											Right: &sqlast.SingleQuotedString{
												From:   sqltoken.NewPos(3, 15),
												To:     sqltoken.NewPos(3, 18),
												String: "1",
											},
										},
										&sqlast.BinaryExpr{
											Op: &sqlast.Operator{
												Type: sqlast.Eq,
												From: sqltoken.NewPos(4, 13),
												To:   sqltoken.NewPos(4, 14),
											},
											Left: &sqlast.Ident{
												Value: "expr2",
												From:  sqltoken.NewPos(4, 7),
												To:    sqltoken.NewPos(4, 12),
											},
											Right: &sqlast.SingleQuotedString{
												From:   sqltoken.NewPos(4, 15),
												To:     sqltoken.NewPos(4, 18),
												String: "2",
											},
										},
									},
									Results: []sqlast.Node{
										&sqlast.SingleQuotedString{
											From:   sqltoken.NewPos(3, 24),
											To:     sqltoken.NewPos(3, 31),
											String: "test1",
										},
										&sqlast.SingleQuotedString{
											From:   sqltoken.NewPos(4, 24),
											To:     sqltoken.NewPos(4, 31),
											String: "test2",
										},
									},
									ElseResult: &sqlast.SingleQuotedString{
										From:   sqltoken.NewPos(5, 7),
										To:     sqltoken.NewPos(5, 14),
										String: "other",
									},
								},
								Alias: &sqlast.Ident{
									Value: "alias",
									From:  sqltoken.NewPos(6, 8),
									To:    sqltoken.NewPos(6, 13),
								},
							},
						},
						FromClause: []sqlast.TableReference{
							&sqlast.Table{
								Name: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										{
											Value: "user",
											From:  sqltoken.NewPos(7, 6),
											To:    sqltoken.NewPos(7, 10),
										},
									},
								},
							},
						},
						WhereClause: &sqlast.Between{
							Expr: &sqlast.Ident{
								Value: "id",
								From:  sqltoken.NewPos(7, 17),
								To:    sqltoken.NewPos(7, 19),
							},
							High: &sqlast.LongValue{
								Long: int64(2),
								From: sqltoken.NewPos(7, 34),
								To:   sqltoken.NewPos(7, 35),
							},
							Low: &sqlast.LongValue{
								Long: int64(1),
								From: sqltoken.NewPos(7, 28),
								To:   sqltoken.NewPos(7, 29),
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

				if diff := CompareWithoutMarker(c.out, ast); diff != "" {
					t.Errorf("diff %s", diff)
				}
			})
		}
	})

	t.Run("create", func(t *testing.T) {
		cases := []struct {
			name string
			in   string
			out  sqlast.Stmt
			skip bool
		}{
			{
				name: "create table",
				in: `
CREATE TABLE persons (
 person_id UUID PRIMARY KEY NOT NULL,
 first_name varchar(255) UNIQUE,
 last_name character varying(255) NOT NULL,
 created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL
)`,
				out: &sqlast.CreateTableStmt{
					Create: sqltoken.NewPos(2, 1),
					Name: &sqlast.ObjectName{
						Idents: []*sqlast.Ident{
							{
								Value: "persons",
								From:  sqltoken.NewPos(2, 14),
								To:    sqltoken.NewPos(2, 21),
							},
						},
					},
					Elements: []sqlast.TableElement{
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "person_id",
								From:  sqltoken.NewPos(3, 2),
								To:    sqltoken.NewPos(3, 11),
							},
							DataType: &sqlast.UUID{
								From: sqltoken.NewPos(3, 12),
								To:   sqltoken.NewPos(3, 16),
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.UniqueColumnSpec{
										IsPrimaryKey: true,
										Primary:      sqltoken.NewPos(3, 17),
										Key:          sqltoken.NewPos(3, 28),
									},
								},
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not:  sqltoken.NewPos(3, 29),
										Null: sqltoken.NewPos(3, 37),
									},
								},
							},
						},
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "first_name",
								From:  sqltoken.NewPos(4, 2),
								To:    sqltoken.NewPos(4, 12),
							},
							DataType: &sqlast.VarcharType{
								Size:      sqlast.NewSize(255),
								Character: sqltoken.NewPos(4, 13),
								RParen:    sqltoken.NewPos(4, 25),
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.UniqueColumnSpec{
										Unique: sqltoken.NewPos(4, 26),
									},
								},
							},
						},
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "last_name",
								From:  sqltoken.NewPos(5, 2),
								To:    sqltoken.NewPos(5, 11),
							},
							DataType: &sqlast.VarcharType{
								Size:      sqlast.NewSize(255),
								Character: sqltoken.NewPos(5, 12),
								Varying:   sqltoken.NewPos(5, 29),
								RParen:    sqltoken.NewPos(5, 34),
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not:  sqltoken.NewPos(5, 35),
										Null: sqltoken.NewPos(5, 43),
									},
								},
							},
						},
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "created_at",
								From:  sqltoken.NewPos(6, 2),
								To:    sqltoken.NewPos(6, 12),
							},
							DataType: &sqlast.Timestamp{
								Timestamp: sqltoken.NewPos(6, 13),
							},
							Default: &sqlast.Ident{
								Value: "CURRENT_TIMESTAMP",
								From:  sqltoken.NewPos(6, 31),
								To:    sqltoken.NewPos(6, 48),
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not:  sqltoken.NewPos(6, 49),
										Null: sqltoken.NewPos(6, 57),
									},
								},
							},
						},
					},
				},
			},
			{
				name: "with case",
				in: `CREATE TABLE persons (
person_id int PRIMARY KEY NOT NULL,
last_name character varying(255) NOT NULL,
test_id int NOT NULL REFERENCES test(id1),
email character varying(255) UNIQUE NOT NULL,
age int NOT NULL CHECK(age > 0 AND age < 100),
created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL
)`,
				out: &sqlast.CreateTableStmt{
					Create: sqltoken.NewPos(1, 1),
					Name: &sqlast.ObjectName{
						Idents: []*sqlast.Ident{
							{
								Value: "persons",
								From:  sqltoken.NewPos(1, 14),
								To:    sqltoken.NewPos(1, 21),
							},
						},
					},
					Elements: []sqlast.TableElement{
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "person_id",
								From:  sqltoken.NewPos(2, 1),
								To:    sqltoken.NewPos(2, 10),
							},
							DataType: &sqlast.Int{
								From: sqltoken.NewPos(2, 11),
								To:   sqltoken.NewPos(2, 14),
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.UniqueColumnSpec{
										IsPrimaryKey: true,
										Primary:      sqltoken.NewPos(2, 15),
										Key:          sqltoken.NewPos(2, 26),
									},
								},
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not:  sqltoken.NewPos(2, 27),
										Null: sqltoken.NewPos(2, 35),
									},
								},
							},
						},
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "last_name",
								From:  sqltoken.NewPos(3, 1),
								To:    sqltoken.NewPos(3, 10),
							},
							DataType: &sqlast.VarcharType{
								Size:      sqlast.NewSize(255),
								Character: sqltoken.NewPos(3, 11),
								Varying:   sqltoken.NewPos(3, 28),
								RParen:    sqltoken.NewPos(3, 33),
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not:  sqltoken.NewPos(3, 34),
										Null: sqltoken.NewPos(3, 42),
									},
								},
							},
						},
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "test_id",
								From:  sqltoken.NewPos(4, 1),
								To:    sqltoken.NewPos(4, 8),
							},
							DataType: &sqlast.Int{
								From: sqltoken.NewPos(4, 9),
								To:   sqltoken.NewPos(4, 12),
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not:  sqltoken.NewPos(4, 13),
										Null: sqltoken.NewPos(4, 21),
									},
								},
								{
									Spec: &sqlast.ReferencesColumnSpec{
										References: sqltoken.NewPos(4, 22),
										RParen:     sqltoken.NewPos(4, 42),
										TableName: &sqlast.ObjectName{
											Idents: []*sqlast.Ident{
												{
													Value: "test",
													From:  sqltoken.NewPos(4, 33),
													To:    sqltoken.NewPos(4, 37),
												},
											},
										},
										Columns: []*sqlast.Ident{
											&sqlast.Ident{
												Value: "id1",
												From:  sqltoken.NewPos(4, 38),
												To:    sqltoken.NewPos(4, 41),
											},
										},
									},
								},
							},
						},
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "email",
								From:  sqltoken.NewPos(5, 1),
								To:    sqltoken.NewPos(5, 6),
							},
							DataType: &sqlast.VarcharType{
								Size:      sqlast.NewSize(255),
								Character: sqltoken.NewPos(5, 7),
								Varying:   sqltoken.NewPos(5, 24),
								RParen:    sqltoken.NewPos(5, 29),
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.UniqueColumnSpec{
										Unique: sqltoken.NewPos(5, 30),
									},
								},
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not:  sqltoken.NewPos(5, 37),
										Null: sqltoken.NewPos(5, 45),
									},
								},
							},
						},
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "age",
								From:  sqltoken.NewPos(6, 1),
								To:    sqltoken.NewPos(6, 4),
							},
							DataType: &sqlast.Int{
								From: sqltoken.NewPos(6, 5),
								To:   sqltoken.NewPos(6, 8),
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not:  sqltoken.NewPos(6, 9),
										Null: sqltoken.NewPos(6, 17),
									},
								},
								{
									Spec: &sqlast.CheckColumnSpec{
										Check:  sqltoken.NewPos(6, 18),
										RParen: sqltoken.NewPos(6, 46),
										Expr: &sqlast.BinaryExpr{
											Op: &sqlast.Operator{
												Type: sqlast.And,
												From: sqltoken.NewPos(6, 32),
												To:   sqltoken.NewPos(6, 35),
											},
											Left: &sqlast.BinaryExpr{
												Op: &sqlast.Operator{
													Type: sqlast.Gt,
													From: sqltoken.NewPos(6, 28),
													To:   sqltoken.NewPos(6, 29),
												},
												Left: &sqlast.Ident{
													Value: "age",
													From:  sqltoken.NewPos(6, 24),
													To:    sqltoken.NewPos(6, 27),
												},
												Right: &sqlast.LongValue{
													From: sqltoken.NewPos(6, 30),
													To:   sqltoken.NewPos(6, 31),
													Long: 0,
												},
											},
											Right: &sqlast.BinaryExpr{
												Op: &sqlast.Operator{
													Type: sqlast.Lt,
													From: sqltoken.NewPos(6, 40),
													To:   sqltoken.NewPos(6, 41),
												},
												Left: &sqlast.Ident{
													Value: "age",
													From:  sqltoken.NewPos(6, 36),
													To:    sqltoken.NewPos(6, 39),
												},
												Right: &sqlast.LongValue{
													From: sqltoken.NewPos(6, 42),
													To:   sqltoken.NewPos(6, 45),
													Long: 100,
												},
											},
										},
									},
								},
							},
						},
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "created_at",
								From:  sqltoken.NewPos(7, 1),
								To:    sqltoken.NewPos(7, 11),
							},
							DataType: &sqlast.Timestamp{
								Timestamp: sqltoken.NewPos(7, 12),
							},
							Default: &sqlast.Ident{
								Value: "CURRENT_TIMESTAMP",
								From:  sqltoken.NewPos(7, 30),
								To:    sqltoken.NewPos(7, 47),
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not:  sqltoken.NewPos(7, 48),
										Null: sqltoken.NewPos(7, 56),
									},
								},
							},
						},
					},
				},
			},
			{
				name: "with table constraint",
				in: `CREATE TABLE persons (
person_id int,
CONSTRAINT production UNIQUE(test_column),
PRIMARY KEY(person_id),
CHECK(id > 100),
FOREIGN KEY(test_id) REFERENCES other_table(col1, col2)
)`,
				out: &sqlast.CreateTableStmt{
					Create: sqltoken.NewPos(1, 1),
					Name: &sqlast.ObjectName{
						Idents: []*sqlast.Ident{
							{
								Value: "persons",
								From:  sqltoken.NewPos(1, 14),
								To:    sqltoken.NewPos(1, 21),
							},
						},
					},
					Elements: []sqlast.TableElement{
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "person_id",
								From:  sqltoken.NewPos(2, 1),
								To:    sqltoken.NewPos(2, 10),
							},
							DataType: &sqlast.Int{
								From: sqltoken.NewPos(2, 11),
								To:   sqltoken.NewPos(2, 14),
							},
						},
						&sqlast.TableConstraint{
							Constraint: sqltoken.NewPos(3, 1),
							Name: &sqlast.Ident{
								Value: "production",
								From:  sqltoken.NewPos(3, 12),
								To:    sqltoken.NewPos(3, 22),
							},
							Spec: &sqlast.UniqueTableConstraint{
								Unique: sqltoken.NewPos(3, 23),
								RParen: sqltoken.NewPos(3, 42),
								Columns: []*sqlast.Ident{&sqlast.Ident{
									Value: "test_column",
									From:  sqltoken.NewPos(3, 30),
									To:    sqltoken.NewPos(3, 41),
								}},
							},
						},
						&sqlast.TableConstraint{
							Spec: &sqlast.UniqueTableConstraint{
								Primary: sqltoken.NewPos(4, 1),
								RParen:  sqltoken.NewPos(4, 23),
								Columns: []*sqlast.Ident{&sqlast.Ident{
									Value: "person_id",
									From:  sqltoken.NewPos(4, 13),
									To:    sqltoken.NewPos(4, 22),
								}},
								IsPrimary: true,
							},
						},
						&sqlast.TableConstraint{
							Spec: &sqlast.CheckTableConstraint{
								Check:  sqltoken.NewPos(5, 1),
								RParen: sqltoken.NewPos(5, 16),
								Expr: &sqlast.BinaryExpr{
									Left: &sqlast.Ident{
										Value: "id",
										From:  sqltoken.NewPos(5, 7),
										To:    sqltoken.NewPos(5, 9),
									},
									Op: &sqlast.Operator{
										Type: sqlast.Gt,
										From: sqltoken.NewPos(5, 10),
										To:   sqltoken.NewPos(5, 11),
									},
									Right: &sqlast.LongValue{
										From: sqltoken.NewPos(5, 12),
										To:   sqltoken.NewPos(5, 15),
										Long: 100,
									},
								},
							},
						},
						&sqlast.TableConstraint{
							Spec: &sqlast.ReferentialTableConstraint{
								Foreign: sqltoken.NewPos(6, 1),
								Columns: []*sqlast.Ident{&sqlast.Ident{
									Value: "test_id",
									From:  sqltoken.NewPos(6, 13),
									To:    sqltoken.NewPos(6, 20),
								}},
								KeyExpr: &sqlast.ReferenceKeyExpr{
									TableName: &sqlast.Ident{
										Value: "other_table",
										From:  sqltoken.NewPos(6, 33),
										To:    sqltoken.NewPos(6, 44),
									},
									Columns: []*sqlast.Ident{
										&sqlast.Ident{
											Value: "col1",
											From:  sqltoken.NewPos(6, 45),
											To:    sqltoken.NewPos(6, 49),
										},
										&sqlast.Ident{
											Value: "col2",
											From:  sqltoken.NewPos(6, 51),
											To:    sqltoken.NewPos(6, 55),
										},
									},
									RParen: sqltoken.NewPos(6, 56),
								},
							},
						},
					},
				},
			},
			{
				name: "create view",
				in:   "CREATE VIEW comedies AS SELECT * FROM films WHERE kind = 'Comedy'",
				out: &sqlast.CreateViewStmt{
					Create: sqltoken.NewPos(1, 1),
					Name: &sqlast.ObjectName{
						Idents: []*sqlast.Ident{
							{
								Value: "comedies",
								From:  sqltoken.NewPos(1, 13),
								To:    sqltoken.NewPos(1, 21),
							},
						},
					},
					Query: &sqlast.QueryStmt{
						Body: &sqlast.SQLSelect{
							Select: sqltoken.NewPos(1, 25),
							Projection: []sqlast.SQLSelectItem{
								&sqlast.UnnamedSelectItem{Node: &sqlast.Wildcard{
									Wildcard: sqltoken.NewPos(1, 32),
								}}},
							FromClause: []sqlast.TableReference{
								&sqlast.Table{
									Name: &sqlast.ObjectName{
										Idents: []*sqlast.Ident{
											{
												Value: "films",
												From:  sqltoken.NewPos(1, 39),
												To:    sqltoken.NewPos(1, 44),
											},
										},
									},
								},
							},
							WhereClause: &sqlast.BinaryExpr{
								Op: &sqlast.Operator{
									Type: sqlast.Eq,
									From: sqltoken.NewPos(1, 56),
									To:   sqltoken.NewPos(1, 57),
								},
								Left: sqlast.NewIdentWithPos("kind", sqltoken.NewPos(1, 51), sqltoken.NewPos(1, 55)),
								Right: &sqlast.SingleQuotedString{
									From:   sqltoken.NewPos(1, 58),
									To:     sqltoken.NewPos(1, 66),
									String: "Comedy",
								},
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
					t.Fatalf("%+v", err)
				}

				if diff := CompareWithoutMarker(c.out, ast); diff != "" {
					t.Errorf("diff %s", diff)
				}
			})
		}
	})

	t.Run("delete", func(t *testing.T) {
		cases := []struct {
			name string
			in   string
			out  sqlast.Stmt
			skip bool
		}{
			{
				in:   "DELETE FROM customers WHERE customer_id = 1",
				name: "simple case",
				out: &sqlast.DeleteStmt{
					Delete: sqltoken.NewPos(1, 1),
					TableName: &sqlast.ObjectName{
						Idents: []*sqlast.Ident{
							{
								Value: "customers",
								From:  sqltoken.NewPos(1, 13),
								To:    sqltoken.NewPos(1, 22),
							},
						},
					},
					Selection: &sqlast.BinaryExpr{
						Op: &sqlast.Operator{
							Type: sqlast.Eq,
							From: sqltoken.NewPos(1, 41),
							To:   sqltoken.NewPos(1, 42),
						},
						Left: sqlast.NewIdentWithPos("customer_id", sqltoken.NewPos(1, 29), sqltoken.NewPos(1, 40)),
						Right: &sqlast.LongValue{
							From: sqltoken.NewPos(1, 43),
							To:   sqltoken.NewPos(1, 44),
							Long: 1,
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

				if diff := CompareWithoutMarker(c.out, ast); diff != "" {
					t.Errorf("diff %s", diff)
				}
			})
		}
	})

	t.Run("insert", func(t *testing.T) {
		cases := []struct {
			name string
			in   string
			out  sqlast.Stmt
			skip bool
		}{
			{
				in:   "INSERT INTO customers (customer_name, contract_name) VALUES('Cardinal', 'Tom B. Erichsen')",
				name: "simple case",
				out: &sqlast.InsertStmt{
					Insert: sqltoken.NewPos(1, 1),
					TableName: &sqlast.ObjectName{
						Idents: []*sqlast.Ident{
							sqlast.NewIdentWithPos("customers", sqltoken.NewPos(1, 13), sqltoken.NewPos(1, 22)),
						},
					},
					Columns: []*sqlast.Ident{
						sqlast.NewIdentWithPos("customer_name", sqltoken.NewPos(1, 24), sqltoken.NewPos(1, 37)),
						sqlast.NewIdentWithPos("contract_name", sqltoken.NewPos(1, 39), sqltoken.NewPos(1, 52)),
					},
					Source: &sqlast.ConstructorSource{
						Rows: []*sqlast.RowValueExpr{
							{
								LParen: sqltoken.NewPos(1, 60),
								RParen: sqltoken.NewPos(1, 91),
								Values: []sqlast.Node{
									&sqlast.SingleQuotedString{
										From:   sqltoken.NewPos(1, 61),
										To:     sqltoken.NewPos(1, 71),
										String: "Cardinal",
									},
									&sqlast.SingleQuotedString{
										From:   sqltoken.NewPos(1, 73),
										To:     sqltoken.NewPos(1, 90),
										String: "Tom B. Erichsen",
									},
								},
							},
						},
					},
				},
			},
			{
				name: "multi record case",
				in: `INSERT INTO customers (customer_name, contract_name) VALUES
('Cardinal', 'Tom B. Erichsen'),
('Cardinal', 'Tom B. Erichsen')`,
				out: &sqlast.InsertStmt{
					Insert: sqltoken.NewPos(1, 1),
					TableName: &sqlast.ObjectName{
						Idents: []*sqlast.Ident{
							{
								Value: "customers",
								From:  sqltoken.NewPos(1, 13),
								To:    sqltoken.NewPos(1, 22),
							},
						},
					},
					Columns: []*sqlast.Ident{
						sqlast.NewIdentWithPos("customer_name", sqltoken.NewPos(1, 24), sqltoken.NewPos(1, 37)),
						sqlast.NewIdentWithPos("contract_name", sqltoken.NewPos(1, 39), sqltoken.NewPos(1, 52)),
					},
					Source: &sqlast.ConstructorSource{
						Rows: []*sqlast.RowValueExpr{
							{
								LParen: sqltoken.NewPos(2, 1),
								RParen: sqltoken.NewPos(2, 32),
								Values: []sqlast.Node{
									&sqlast.SingleQuotedString{
										From:   sqltoken.NewPos(2, 2),
										To:     sqltoken.NewPos(2, 12),
										String: "Cardinal",
									},
									&sqlast.SingleQuotedString{
										From:   sqltoken.NewPos(2, 14),
										To:     sqltoken.NewPos(2, 31),
										String: "Tom B. Erichsen",
									},
								},
							},
							{
								LParen: sqltoken.NewPos(3, 1),
								RParen: sqltoken.NewPos(3, 32),
								Values: []sqlast.Node{
									&sqlast.SingleQuotedString{
										From:   sqltoken.NewPos(3, 2),
										To:     sqltoken.NewPos(3, 12),
										String: "Cardinal",
									},
									&sqlast.SingleQuotedString{
										From:   sqltoken.NewPos(3, 14),
										To:     sqltoken.NewPos(3, 31),
										String: "Tom B. Erichsen",
									},
								},
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

				if diff := CompareWithoutMarker(c.out, ast); diff != "" {
					t.Errorf("diff %s", diff)
				}
			})
		}
	})

	t.Run("alter", func(t *testing.T) {
		cases := []struct {
			name string
			in   string
			out  sqlast.Stmt
			skip bool
		}{
			{
				name: "add column",
				in: `
ALTER TABLE customers
ADD COLUMN email character varying(255)`,
				out: &sqlast.AlterTableStmt{
					Alter: sqltoken.NewPos(2, 1),
					TableName: &sqlast.ObjectName{
						Idents: []*sqlast.Ident{
							sqlast.NewIdentWithPos("customers", sqltoken.NewPos(2, 13), sqltoken.NewPos(2, 22)),
						},
					},
					Action: &sqlast.AddColumnTableAction{
						Add: sqltoken.NewPos(3, 1),
						Column: &sqlast.ColumnDef{
							Name: sqlast.NewIdentWithPos("email", sqltoken.NewPos(3, 12), sqltoken.NewPos(3, 17)),
							DataType: &sqlast.VarcharType{
								Size:      sqlast.NewSize(255),
								Character: sqltoken.NewPos(3, 18),
								Varying:   sqltoken.NewPos(3, 35),
								RParen:    sqltoken.NewPos(3, 40),
							},
						},
					},
				},
			},
			{
				name: "add constraint",
				in: `
ALTER TABLE products
ADD FOREIGN KEY(test_id) REFERENCES other_table(col1, col2)`,
				out: &sqlast.AlterTableStmt{
					Alter: sqltoken.NewPos(2, 1),
					TableName: &sqlast.ObjectName{
						Idents: []*sqlast.Ident{
							sqlast.NewIdentWithPos("products", sqltoken.NewPos(2, 13), sqltoken.NewPos(2, 21)),
						},
					},
					Action: &sqlast.AddConstraintTableAction{
						Add: sqltoken.NewPos(3, 1),
						Constraint: &sqlast.TableConstraint{
							Spec: &sqlast.ReferentialTableConstraint{
								Foreign: sqltoken.NewPos(3, 5),
								Columns: []*sqlast.Ident{
									sqlast.NewIdentWithPos("test_id", sqltoken.NewPos(3, 17), sqltoken.NewPos(3, 24)),
								},
								KeyExpr: &sqlast.ReferenceKeyExpr{
									TableName: sqlast.NewIdentWithPos("other_table", sqltoken.NewPos(3, 37), sqltoken.NewPos(3, 48)),
									Columns: []*sqlast.Ident{
										sqlast.NewIdentWithPos("col1", sqltoken.NewPos(3, 49), sqltoken.NewPos(3, 53)),
										sqlast.NewIdentWithPos("col2", sqltoken.NewPos(3, 55), sqltoken.NewPos(3, 59)),
									},
									RParen: sqltoken.NewPos(3, 60),
								},
							},
						},
					},
				},
			},
			{
				name: "drop constraint",
				in: `ALTER TABLE products
DROP CONSTRAINT fk CASCADE`,
				out: &sqlast.AlterTableStmt{
					Alter: sqltoken.NewPos(1, 1),
					TableName: &sqlast.ObjectName{
						Idents: []*sqlast.Ident{
							sqlast.NewIdentWithPos("products", sqltoken.NewPos(1, 13), sqltoken.NewPos(1, 21)),
						},
					},
					Action: &sqlast.DropConstraintTableAction{
						Drop:       sqltoken.NewPos(2, 1),
						Name:       sqlast.NewIdentWithPos("fk", sqltoken.NewPos(2, 17), sqltoken.NewPos(2, 19)),
						Cascade:    true,
						CascadePos: sqltoken.NewPos(2, 27),
					},
				},
			},
			{
				name: "remove column",
				in: `ALTER TABLE products
DROP COLUMN description CASCADE`,
				out: &sqlast.AlterTableStmt{
					Alter: sqltoken.NewPos(1, 1),
					TableName: &sqlast.ObjectName{
						Idents: []*sqlast.Ident{
							sqlast.NewIdentWithPos("products", sqltoken.NewPos(1, 13), sqltoken.NewPos(1, 21)),
						},
					},
					Action: &sqlast.RemoveColumnTableAction{
						Drop:       sqltoken.NewPos(2, 1),
						Name:       sqlast.NewIdentWithPos("description", sqltoken.NewPos(2, 13), sqltoken.NewPos(2, 24)),
						Cascade:    true,
						CascadePos: sqltoken.NewPos(2, 32),
					},
				},
			},
			{
				name: "alter column",
				in: `ALTER TABLE products
ALTER COLUMN created_at SET DEFAULT current_timestamp`,
				out: &sqlast.AlterTableStmt{
					Alter: sqltoken.NewPos(1, 1),
					TableName: &sqlast.ObjectName{
						Idents: []*sqlast.Ident{
							sqlast.NewIdentWithPos("products", sqltoken.NewPos(1, 13), sqltoken.NewPos(1, 21)),
						},
					},
					Action: &sqlast.AlterColumnTableAction{
						Alter:      sqltoken.NewPos(2, 1),
						ColumnName: sqlast.NewIdentWithPos("created_at", sqltoken.NewPos(2, 14), sqltoken.NewPos(2, 24)),
						Action: &sqlast.SetDefaultColumnAction{
							Set:     sqltoken.NewPos(2, 25),
							Default: sqlast.NewIdentWithPos("current_timestamp", sqltoken.NewPos(2, 37), sqltoken.NewPos(2, 54)),
						},
					},
				},
			},
			{
				name: "pg change type",
				in: `ALTER TABLE products
ALTER COLUMN number TYPE numeric(255,10)`,
				out: &sqlast.AlterTableStmt{
					Alter: sqltoken.NewPos(1, 1),
					TableName: &sqlast.ObjectName{
						Idents: []*sqlast.Ident{
							sqlast.NewIdentWithPos("products", sqltoken.NewPos(1, 13), sqltoken.NewPos(1, 21)),
						},
					},
					Action: &sqlast.AlterColumnTableAction{
						Alter:      sqltoken.NewPos(2, 1),
						ColumnName: sqlast.NewIdentWithPos("number", sqltoken.NewPos(2, 14), sqltoken.NewPos(2, 20)),
						Action: &sqlast.PGAlterDataTypeColumnAction{
							Type: sqltoken.NewPos(2, 21),
							DataType: &sqlast.Decimal{
								Scale:     sqlast.NewSize(10),
								Precision: sqlast.NewSize(255),
								Numeric:   sqltoken.NewPos(2, 26),
								RParen:    sqltoken.NewPos(2, 41),
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
					t.Fatalf("%+v", err)
				}

				if diff := CompareWithoutMarker(c.out, ast); diff != "" {
					t.Errorf("diff %s", diff)
				}
			})
		}
	})

	t.Run("update", func(t *testing.T) {
		cases := []struct {
			name string
			in   string
			out  sqlast.Stmt
			skip bool
		}{
			{
				name: "simple case",
				in:   "UPDATE customers SET contract_name = 'Alfred Schmidt', city = 'Frankfurt' WHERE customer_id = 1",
				out: &sqlast.UpdateStmt{
					Update: sqltoken.NewPos(1, 1),
					TableName: &sqlast.ObjectName{
						Idents: []*sqlast.Ident{
							{
								Value: "customers",
								From:  sqltoken.NewPos(1, 8),
								To:    sqltoken.NewPos(1, 17),
							},
						},
					},
					Assignments: []*sqlast.Assignment{
						{
							ID: sqlast.NewIdentWithPos("contract_name", sqltoken.NewPos(1, 22), sqltoken.NewPos(1, 35)),
							Value: &sqlast.SingleQuotedString{
								From:   sqltoken.NewPos(1, 38),
								To:     sqltoken.NewPos(1, 54),
								String: "Alfred Schmidt",
							},
						},
						{
							ID:    sqlast.NewIdentWithPos("city", sqltoken.NewPos(1, 56), sqltoken.NewPos(1, 60)),
							Value: &sqlast.SingleQuotedString{String: "Frankfurt", From: sqltoken.NewPos(1, 63), To: sqltoken.NewPos(1, 74)},
						},
					},
					Selection: &sqlast.BinaryExpr{
						Op:   &sqlast.Operator{Type: sqlast.Eq, From: sqltoken.NewPos(1, 93), To: sqltoken.NewPos(1, 94)},
						Left: sqlast.NewIdentWithPos("customer_id", sqltoken.NewPos(1, 81), sqltoken.NewPos(1, 92)),
						Right: &sqlast.LongValue{
							From: sqltoken.NewPos(1, 95),
							To:   sqltoken.NewPos(1, 96),
							Long: 1,
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

				if diff := CompareWithoutMarker(c.out, ast); diff != "" {
					t.Errorf("diff %s", diff)
				}
			})
		}
	})
}

func TestParser_ParseSQL(t *testing.T) {
	in := `
create table account (
    account_id serial primary key,
    name varchar(255) not null,
    email varchar(255) unique not null,
    age smallint not null,
    registered_at timestamp with time zone default current_timestamp
);

create table category (
    category_id serial primary key,
    name varchar(255) not null
);

create table item (
    item_id serial primary key,
    price int not null,
    name varchar(255) not null,
    category_id int references category(category_id),
    created_at timestamp with time zone default current_timestamp
);
`
	parser, err := NewParser(bytes.NewBufferString(in), &dialect.GenericSQLDialect{})
	if err != nil {
		t.Fatalf("%+v", err)
	}

	stmts, err := parser.ParseSQL()
	if err != nil {
		t.Fatalf("%+v", err)
	}

	if len(stmts) != 3 {
		t.Fatal("must be 3 stmts")
	}
}

func TestParser_ParseFile(t *testing.T) {

	cases := []struct {
		name string
		skip bool
		in   string
		out  []*sqlast.CommentGroup
	}{
		{
			name: "single line",
			in: `--comment
select 1 from test;`,
			out: []*sqlast.CommentGroup{
				{
					List: []*sqlast.Comment{
						{
							Text: "comment",
							From: sqltoken.NewPos(1, 1),
							To:   sqltoken.NewPos(1, 10),
						},
					},
				},
			},
		},
		{
			name: "multi line",
			in: `
create table account (
    account_id serial primary key,  --aaa
	/*bbb*/
    name varchar(255) not null,
    email /*ccc*/ varchar(255) unique not null --ddd
);

--eee

/*fff
ggg
*/
select 1 from test; --hhh
/*jjj*/ --kkk
select 1 from test; /*lll*/ --mmm
--nnn
`,
			out: []*sqlast.CommentGroup{
				{
					List: []*sqlast.Comment{
						{
							Text: "aaa",
							From: sqltoken.NewPos(3, 37),
							To:   sqltoken.NewPos(3, 42),
						},
					},
				},
				{
					List: []*sqlast.Comment{
						{
							Text: "bbb",
							From: sqltoken.NewPos(4, 5),
							To:   sqltoken.NewPos(4, 12),
						},
					},
				},
				{
					List: []*sqlast.Comment{
						{
							Text: "ccc",
							From: sqltoken.NewPos(6, 11),
							To:   sqltoken.NewPos(6, 18),
						},
					},
				},
				{
					List: []*sqlast.Comment{
						{
							Text: "ddd",
							From: sqltoken.NewPos(6, 48),
							To:   sqltoken.NewPos(6, 53),
						},
					},
				},
				{
					List: []*sqlast.Comment{
						{
							Text: "eee",
							From: sqltoken.NewPos(9, 1),
							To:   sqltoken.NewPos(9, 6),
						},
						{
							Text: "fff\nggg\n",
							From: sqltoken.NewPos(11, 1),
							To:   sqltoken.NewPos(13, 3),
						},
					},
				},
				{
					List: []*sqlast.Comment{
						{
							Text: "hhh",
							From: sqltoken.NewPos(14, 21),
							To:   sqltoken.NewPos(14, 26),
						},
					},
				},
				{
					List: []*sqlast.Comment{
						{
							Text: "jjj",
							From: sqltoken.NewPos(15, 1),
							To:   sqltoken.NewPos(15, 8),
						},
						{
							Text: "kkk",
							From: sqltoken.NewPos(15, 9),
							To:   sqltoken.NewPos(15, 14),
						},
					},
				},
				{
					List: []*sqlast.Comment{
						{
							Text: "lll",
							From: sqltoken.NewPos(16, 21),
							To:   sqltoken.NewPos(16, 28),
						},
						{
							Text: "mmm",
							From: sqltoken.NewPos(16, 29),
							To:   sqltoken.NewPos(16, 34),
						},
					},
				},
				{
					List: []*sqlast.Comment{
						{
							Text: "nnn",
							From: sqltoken.NewPos(17, 1),
							To:   sqltoken.NewPos(17, 6),
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
			parser, err := NewParser(bytes.NewBufferString(c.in), &dialect.GenericSQLDialect{}, ParseComment)
			if err != nil {
				t.Fatal(err)
			}

			f, err := parser.ParseFile()
			if err != nil {
				t.Fatalf("%+v", err)
			}

			if diff := cmp.Diff(c.out, f.Comments); diff != "" {
				t.Errorf("diff %s", diff)
			}

		})
	}

}
