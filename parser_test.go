package xsqlparser

import (
	"bytes"
	"reflect"
	"testing"
	"unicode"

	"github.com/google/go-cmp/cmp"

	"github.com/akito0107/xsqlparser/dialect"
	"github.com/akito0107/xsqlparser/sqlast"
	"github.com/akito0107/xsqlparser/sqltoken"
)

var IgnoreMarker = cmp.FilterPath(func(paths cmp.Path) bool {
	s := paths.Last().Type()
	name := s.Name()
	r := []rune(name)
	return s.Kind() == reflect.Struct && len(r) > 0 && unicode.IsLower(r[0])
}, cmp.Ignore())

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
				out: &sqlast.Query{
					Body: &sqlast.SQLSelect{
						Select: sqltoken.NewPos(1, 0),
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedSelectItem{
								Node: sqlast.NewIdentWithPos(
									"test",
									sqltoken.NewPos(1, 7),
									sqltoken.NewPos(1, 11),
								),
							},
						},
						FromClause: []sqlast.TableReference{
							&sqlast.Table{
								Name: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										sqlast.NewIdentWithPos(
											"test_table",
											sqltoken.NewPos(1, 17),
											sqltoken.NewPos(1, 27),
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
				out: &sqlast.Query{
					Body: &sqlast.SQLSelect{
						Select: sqltoken.NewPos(1, 0),
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedSelectItem{
								Node: sqlast.NewIdentWithPos(
									"test",
									sqltoken.NewPos(1, 7),
									sqltoken.NewPos(1, 11),
								),
							},
						},
						FromClause: []sqlast.TableReference{
							&sqlast.Table{
								Name: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										sqlast.NewIdentWithPos(
											"test_table",
											sqltoken.NewPos(1, 17),
											sqltoken.NewPos(1, 27),
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
										sqltoken.NewPos(1, 34),
										sqltoken.NewPos(1, 44),
									),
									sqlast.NewIdentWithPos(
										"column1",
										sqltoken.NewPos(1, 45),
										sqltoken.NewPos(1, 52),
									),
								},
							},
							Op: &sqlast.Operator{
								Type: sqlast.Eq,
								From: sqltoken.NewPos(1, 53),
								To:   sqltoken.NewPos(1, 54),
							},
							Right: &sqlast.SingleQuotedString{
								From:   sqltoken.NewPos(1, 55),
								To:     sqltoken.NewPos(1, 61),
								String: "test",
							},
						},
					},
				},
			},
			{
				name: "count and join",
				in:   "SELECT COUNT(t1.id) AS c FROM test_table AS t1 LEFT JOIN test_table2 AS t2 ON t1.id = t2.test_table_id",
				out: &sqlast.Query{
					Body: &sqlast.SQLSelect{
						Select: sqltoken.NewPos(1, 0),
						Projection: []sqlast.SQLSelectItem{
							&sqlast.AliasSelectItem{
								Expr: &sqlast.Function{
									Name: &sqlast.ObjectName{
										Idents: []*sqlast.Ident{
											sqlast.NewIdentWithPos(
												"COUNT",
												sqltoken.NewPos(1, 7),
												sqltoken.NewPos(1, 12),
											),
										},
									},
									Args: []sqlast.Node{&sqlast.CompoundIdent{
										Idents: []*sqlast.Ident{
											sqlast.NewIdentWithPos(
												"t1",
												sqltoken.NewPos(1, 13),
												sqltoken.NewPos(1, 15),
											),
											sqlast.NewIdentWithPos(
												"id",
												sqltoken.NewPos(1, 16),
												sqltoken.NewPos(1, 18),
											),
										},
									}},
									ArgsRParen: sqltoken.NewPos(1, 19),
								},
								Alias: &sqlast.Ident{
									Value: "c",
									From:  sqltoken.NewPos(1, 23),
									To:    sqltoken.NewPos(1, 24),
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
													From:  sqltoken.NewPos(1, 30),
													To:    sqltoken.NewPos(1, 40),
												},
											},
										},
										Alias: &sqlast.Ident{
											Value: "t1",
											From:  sqltoken.NewPos(1, 44),
											To:    sqltoken.NewPos(1, 46),
										},
									},
								},
								Type: &sqlast.JoinType{
									Condition: sqlast.LEFT,
									From:      sqltoken.NewPos(1, 47),
									To:        sqltoken.NewPos(1, 51),
								},
								RightElement: &sqlast.TableJoinElement{
									Ref: &sqlast.Table{
										Name: &sqlast.ObjectName{
											Idents: []*sqlast.Ident{
												{
													Value: "test_table2",
													From:  sqltoken.NewPos(1, 57),
													To:    sqltoken.NewPos(1, 68),
												},
											},
										},
										Alias: &sqlast.Ident{
											Value: "t2",
											From:  sqltoken.NewPos(1, 72),
											To:    sqltoken.NewPos(1, 74),
										},
									},
								},
								Spec: &sqlast.JoinCondition{
									On: sqltoken.NewPos(1, 75),
									SearchCondition: &sqlast.BinaryExpr{
										Left: &sqlast.CompoundIdent{
											Idents: []*sqlast.Ident{
												{
													Value: "t1",
													From:  sqltoken.NewPos(1, 78),
													To:    sqltoken.NewPos(1, 80),
												},
												{
													Value: "id",
													From:  sqltoken.NewPos(1, 81),
													To:    sqltoken.NewPos(1, 83),
												},
											},
										},
										Op: &sqlast.Operator{
											Type: sqlast.Eq,
											From: sqltoken.NewPos(1, 84),
											To:   sqltoken.NewPos(1, 85),
										},
										Right: &sqlast.CompoundIdent{
											Idents: []*sqlast.Ident{
												{
													Value: "t2",
													From:  sqltoken.NewPos(1, 86),
													To:    sqltoken.NewPos(1, 88),
												},
												{
													Value: "test_table_id",
													From:  sqltoken.NewPos(1, 89),
													To:    sqltoken.NewPos(1, 102),
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
				out: &sqlast.Query{
					Body: &sqlast.SQLSelect{
						Select: sqltoken.NewPos(1, 0),
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedSelectItem{
								Node: &sqlast.Function{
									Name: &sqlast.ObjectName{
										Idents: []*sqlast.Ident{
											{
												Value: "COUNT",
												From:  sqltoken.NewPos(1, 7),
												To:    sqltoken.NewPos(1, 12),
											},
										},
									},
									Args: []sqlast.Node{
										&sqlast.Ident{
											Value: "customer_id",
											From:  sqltoken.NewPos(1, 13),
											To:    sqltoken.NewPos(1, 24),
										},
									},
									ArgsRParen: sqltoken.NewPos(1, 25),
								},
							},
							&sqlast.QualifiedWildcardSelectItem{
								Prefix: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										{
											Value: "country",
											From:  sqltoken.NewPos(1, 27),
											To:    sqltoken.NewPos(1, 34),
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
											From:  sqltoken.NewPos(1, 42),
											To:    sqltoken.NewPos(1, 51),
										},
									},
								},
							},
						},
						GroupByClause: []sqlast.Node{
							&sqlast.Ident{
								Value: "country",
								From:  sqltoken.NewPos(1, 61),
								To:    sqltoken.NewPos(1, 68),
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
				out: &sqlast.Query{
					Body: &sqlast.SQLSelect{
						Select: sqltoken.NewPos(1, 0),
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedSelectItem{
								Node: &sqlast.Function{
									Name: &sqlast.ObjectName{
										Idents: []*sqlast.Ident{
											{
												Value: "COUNT",
												From:  sqltoken.NewPos(1, 7),
												To:    sqltoken.NewPos(1, 12),
											},
										},
									},
									Args: []sqlast.Node{
										&sqlast.Ident{
											Value: "customer_id",
											From:  sqltoken.NewPos(1, 13),
											To:    sqltoken.NewPos(1, 24),
										},
									},
									ArgsRParen: sqltoken.NewPos(1, 25),
								},
							},
							&sqlast.UnnamedSelectItem{
								Node: &sqlast.Ident{
									Value: "country",
									From:  sqltoken.NewPos(1, 27),
									To:    sqltoken.NewPos(1, 34),
								},
							},
						},
						FromClause: []sqlast.TableReference{
							&sqlast.Table{
								Name: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										{
											Value: "customers",
											From:  sqltoken.NewPos(2, 5),
											To:    sqltoken.NewPos(2, 14),
										},
									},
								},
							},
						},
						GroupByClause: []sqlast.Node{
							&sqlast.Ident{
								Value: "country",
								From:  sqltoken.NewPos(3, 9),
								To:    sqltoken.NewPos(3, 16),
							},
						},
						HavingClause: &sqlast.BinaryExpr{
							Op: &sqlast.Operator{
								Type: sqlast.Gt,
								From: sqltoken.NewPos(4, 26),
								To:   sqltoken.NewPos(4, 27),
							},
							Left: &sqlast.Function{
								Name: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										{
											Value: "COUNT",
											From:  sqltoken.NewPos(4, 7),
											To:    sqltoken.NewPos(4, 12),
										},
									},
								},
								Args: []sqlast.Node{
									&sqlast.Ident{
										Value: "customer_id",
										From:  sqltoken.NewPos(4, 13),
										To:    sqltoken.NewPos(4, 24),
									},
								},
								ArgsRParen: sqltoken.NewPos(4, 25),
							},
							Right: &sqlast.LongValue{
								From: sqltoken.NewPos(4, 28),
								To:   sqltoken.NewPos(4, 29),
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
				out: &sqlast.Query{
					Body: &sqlast.SQLSelect{
						Select: sqltoken.NewPos(1, 0),
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedSelectItem{
								Node: &sqlast.Ident{
									Value: "product",
									From:  sqltoken.NewPos(1, 7),
									To:    sqltoken.NewPos(1, 14),
								},
							},
							&sqlast.AliasSelectItem{
								Alias: &sqlast.Ident{
									Value: "product_units",
									From:  sqltoken.NewPos(1, 33),
									To:    sqltoken.NewPos(1, 46),
								},
								Expr: &sqlast.Function{
									Name: &sqlast.ObjectName{
										Idents: []*sqlast.Ident{
											{
												Value: "SUM",
												From:  sqltoken.NewPos(1, 16),
												To:    sqltoken.NewPos(1, 19),
											},
										},
									},
									Args: []sqlast.Node{
										&sqlast.Ident{
											Value: "quantity",
											From:  sqltoken.NewPos(1, 20),
											To:    sqltoken.NewPos(1, 28),
										},
									},
									ArgsRParen: sqltoken.NewPos(1, 29),
								},
							},
						},
						FromClause: []sqlast.TableReference{
							&sqlast.Table{
								Name: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										{
											Value: "orders",
											From:  sqltoken.NewPos(2, 5),
											To:    sqltoken.NewPos(2, 11),
										},
									},
								},
							},
						},
						WhereClause: &sqlast.InSubQuery{
							Expr: &sqlast.Ident{
								Value: "region",
								From:  sqltoken.NewPos(3, 6),
								To:    sqltoken.NewPos(3, 12),
							},
							RParen: sqltoken.NewPos(3, 48),
							SubQuery: &sqlast.Query{
								Body: &sqlast.SQLSelect{
									Select: sqltoken.NewPos(3, 17),
									Projection: []sqlast.SQLSelectItem{
										&sqlast.UnnamedSelectItem{
											Node: &sqlast.Ident{
												Value: "region",
												From:  sqltoken.NewPos(3, 24),
												To:    sqltoken.NewPos(3, 30),
											},
										},
									},
									FromClause: []sqlast.TableReference{
										&sqlast.Table{
											Name: &sqlast.ObjectName{
												Idents: []*sqlast.Ident{
													{
														Value: "top_regions",
														From:  sqltoken.NewPos(3, 36),
														To:    sqltoken.NewPos(3, 47),
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
								From:  sqltoken.NewPos(4, 9),
								To:    sqltoken.NewPos(4, 22),
							},
						},
					},
					Limit: &sqlast.LimitExpr{
						LimitValue: &sqlast.LongValue{
							From: sqltoken.NewPos(4, 29),
							To:   sqltoken.NewPos(4, 32),
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
				out: &sqlast.Query{
					CTEs: []*sqlast.CTE{
						{
							Alias: &sqlast.Ident{
								Value: "regional_sales",
								From:  sqltoken.NewPos(1, 5),
								To:    sqltoken.NewPos(1, 19),
							},
							Query: &sqlast.Query{
								Body: &sqlast.SQLSelect{
									Select: sqltoken.NewPos(1, 24),
									Projection: []sqlast.SQLSelectItem{
										&sqlast.UnnamedSelectItem{
											Node: &sqlast.Ident{
												Value: "region",
												From:  sqltoken.NewPos(1, 31),
												To:    sqltoken.NewPos(1, 37),
											},
										},
										&sqlast.AliasSelectItem{
											Alias: &sqlast.Ident{
												Value: "total_sales",
												From:  sqltoken.NewPos(1, 54),
												To:    sqltoken.NewPos(1, 65),
											},
											Expr: &sqlast.Function{
												Name: &sqlast.ObjectName{
													Idents: []*sqlast.Ident{
														{
															Value: "SUM",
															From:  sqltoken.NewPos(1, 39),
															To:    sqltoken.NewPos(1, 42),
														},
													},
												},
												Args: []sqlast.Node{
													&sqlast.Ident{
														Value: "amount",
														From:  sqltoken.NewPos(1, 43),
														To:    sqltoken.NewPos(1, 49),
													},
												},
												ArgsRParen: sqltoken.NewPos(1, 50),
											},
										},
									},
									FromClause: []sqlast.TableReference{
										&sqlast.Table{
											Name: &sqlast.ObjectName{
												Idents: []*sqlast.Ident{
													{
														Value: "orders",
														From:  sqltoken.NewPos(1, 71),
														To:    sqltoken.NewPos(1, 77),
													},
												},
											},
										},
									},
									GroupByClause: []sqlast.Node{
										&sqlast.Ident{
											Value: "region",
											From:  sqltoken.NewPos(1, 87),
											To:    sqltoken.NewPos(1, 93),
										},
									},
								},
							},
						},
					},
					Body: &sqlast.SQLSelect{
						Select: sqltoken.NewPos(2, 0),
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedSelectItem{Node: &sqlast.Ident{
								Value: "product",
								From:  sqltoken.NewPos(2, 7),
								To:    sqltoken.NewPos(2, 14),
							}},
							&sqlast.AliasSelectItem{
								Alias: &sqlast.Ident{
									Value: "product_units",
									From:  sqltoken.NewPos(2, 33),
									To:    sqltoken.NewPos(2, 46),
								},
								Expr: &sqlast.Function{
									Name: &sqlast.ObjectName{
										Idents: []*sqlast.Ident{
											{
												Value: "SUM",
												From:  sqltoken.NewPos(2, 16),
												To:    sqltoken.NewPos(2, 19),
											},
										},
									},
									Args: []sqlast.Node{
										&sqlast.Ident{
											Value: "quantity",
											From:  sqltoken.NewPos(2, 20),
											To:    sqltoken.NewPos(2, 28),
										},
									},
									ArgsRParen: sqltoken.NewPos(2, 29),
								},
							},
						},
						FromClause: []sqlast.TableReference{
							&sqlast.Table{
								Name: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										{
											Value: "orders",
											From:  sqltoken.NewPos(3, 5),
											To:    sqltoken.NewPos(3, 11),
										},
									},
								},
							},
						},
						WhereClause: &sqlast.InSubQuery{
							RParen: sqltoken.NewPos(4, 48),
							Expr: &sqlast.Ident{
								Value: "region",
								From:  sqltoken.NewPos(4, 6),
								To:    sqltoken.NewPos(4, 12),
							},
							SubQuery: &sqlast.Query{
								Body: &sqlast.SQLSelect{
									Select: sqltoken.NewPos(4, 17),
									Projection: []sqlast.SQLSelectItem{
										&sqlast.UnnamedSelectItem{
											Node: &sqlast.Ident{
												Value: "region",
												From:  sqltoken.NewPos(4, 24),
												To:    sqltoken.NewPos(4, 30),
											},
										},
									},
									FromClause: []sqlast.TableReference{
										&sqlast.Table{
											Name: &sqlast.ObjectName{
												Idents: []*sqlast.Ident{
													{
														Value: "top_regions",
														From:  sqltoken.NewPos(4, 36),
														To:    sqltoken.NewPos(4, 47),
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
								From:  sqltoken.NewPos(5, 9),
								To:    sqltoken.NewPos(5, 15),
							},
							&sqlast.Ident{
								Value: "product",
								From:  sqltoken.NewPos(5, 17),
								To:    sqltoken.NewPos(5, 24),
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
				out: &sqlast.Query{
					Body: &sqlast.SQLSelect{
						Select: sqltoken.NewPos(1, 0),
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedSelectItem{
								Node: &sqlast.Wildcard{
									Wildcard: sqltoken.NewPos(1, 7),
								},
							},
						},
						FromClause: []sqlast.TableReference{
							&sqlast.Table{
								Name: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										{
											Value: "user",
											From:  sqltoken.NewPos(1, 14),
											To:    sqltoken.NewPos(1, 18),
										},
									},
								},
							},
						},
						WhereClause: &sqlast.Exists{
							Negated: true,
							Exists:  sqltoken.NewPos(1, 29),
							Not:     sqltoken.NewPos(1, 25),
							RParen:  sqltoken.NewPos(4, 53),
							Query: &sqlast.Query{
								Body: &sqlast.SQLSelect{
									Select: sqltoken.NewPos(2, 1),
									Projection: []sqlast.SQLSelectItem{
										&sqlast.UnnamedSelectItem{
											Node: &sqlast.Wildcard{
												Wildcard: sqltoken.NewPos(2, 8),
											},
										},
									},
									FromClause: []sqlast.TableReference{
										&sqlast.Table{
											Name: &sqlast.ObjectName{
												Idents: []*sqlast.Ident{
													{
														Value: "user_sub",
														From:  sqltoken.NewPos(3, 5),
														To:    sqltoken.NewPos(3, 13),
													},
												},
											},
										},
									},
									WhereClause: &sqlast.BinaryExpr{
										Op: &sqlast.Operator{Type: sqlast.And, From: sqltoken.NewPos(4, 28), To: sqltoken.NewPos(4, 31)},
										Left: &sqlast.BinaryExpr{
											Op: &sqlast.Operator{Type: sqlast.Eq, From: sqltoken.NewPos(4, 14), To: sqltoken.NewPos(4, 15)},
											Left: &sqlast.CompoundIdent{
												Idents: []*sqlast.Ident{
													{
														Value: "user",
														From:  sqltoken.NewPos(4, 6),
														To:    sqltoken.NewPos(4, 10),
													},
													{
														Value: "id",
														From:  sqltoken.NewPos(4, 11),
														To:    sqltoken.NewPos(4, 13),
													},
												},
											},
											Right: &sqlast.CompoundIdent{
												Idents: []*sqlast.Ident{
													{
														Value: "user_sub",
														From:  sqltoken.NewPos(4, 16),
														To:    sqltoken.NewPos(4, 24),
													},
													{
														Value: "id",
														From:  sqltoken.NewPos(4, 25),
														To:    sqltoken.NewPos(4, 27),
													},
												},
											},
										},
										Right: &sqlast.BinaryExpr{
											Op: &sqlast.Operator{Type: sqlast.Eq, From: sqltoken.NewPos(4, 45), To: sqltoken.NewPos(4, 46)},
											Left: &sqlast.CompoundIdent{
												Idents: []*sqlast.Ident{
													{
														Value: "user_sub",
														From:  sqltoken.NewPos(4, 32),
														To:    sqltoken.NewPos(4, 40),
													},
													{
														Value: "job",
														From:  sqltoken.NewPos(4, 41),
														To:    sqltoken.NewPos(4, 44),
													},
												},
											},
											Right: &sqlast.SingleQuotedString{
												From:   sqltoken.NewPos(4, 47),
												To:     sqltoken.NewPos(4, 52),
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
				out: &sqlast.Query{
					Body: &sqlast.SQLSelect{
						Select: sqltoken.NewPos(1, 0),
						Projection: []sqlast.SQLSelectItem{
							&sqlast.AliasSelectItem{
								Expr: &sqlast.CaseExpr{
									Case:    sqltoken.NewPos(2, 0),
									CaseEnd: sqltoken.NewPos(6, 3),
									Conditions: []sqlast.Node{
										&sqlast.BinaryExpr{
											Op: &sqlast.Operator{
												Type: sqlast.Eq,
												From: sqltoken.NewPos(3, 12),
												To:   sqltoken.NewPos(3, 13),
											},
											Left: &sqlast.Ident{
												Value: "expr1",
												From:  sqltoken.NewPos(3, 6),
												To:    sqltoken.NewPos(3, 11),
											},
											Right: &sqlast.SingleQuotedString{
												From:   sqltoken.NewPos(3, 14),
												To:     sqltoken.NewPos(3, 17),
												String: "1",
											},
										},
										&sqlast.BinaryExpr{
											Op: &sqlast.Operator{
												Type: sqlast.Eq,
												From: sqltoken.NewPos(4, 12),
												To:   sqltoken.NewPos(4, 13),
											},
											Left: &sqlast.Ident{
												Value: "expr2",
												From:  sqltoken.NewPos(4, 6),
												To:    sqltoken.NewPos(4, 11),
											},
											Right: &sqlast.SingleQuotedString{
												From:   sqltoken.NewPos(4, 14),
												To:     sqltoken.NewPos(4, 17),
												String: "2",
											},
										},
									},
									Results: []sqlast.Node{
										&sqlast.SingleQuotedString{
											From:   sqltoken.NewPos(3, 23),
											To:     sqltoken.NewPos(3, 30),
											String: "test1",
										},
										&sqlast.SingleQuotedString{
											From:   sqltoken.NewPos(4, 23),
											To:     sqltoken.NewPos(4, 30),
											String: "test2",
										},
									},
									ElseResult: &sqlast.SingleQuotedString{
										From:   sqltoken.NewPos(5, 6),
										To:     sqltoken.NewPos(5, 13),
										String: "other",
									},
								},
								Alias: &sqlast.Ident{
									Value: "alias",
									From:  sqltoken.NewPos(6, 7),
									To:    sqltoken.NewPos(6, 12),
								},
							},
						},
						FromClause: []sqlast.TableReference{
							&sqlast.Table{
								Name: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										{
											Value: "user",
											From:  sqltoken.NewPos(7, 5),
											To:    sqltoken.NewPos(7, 9),
										},
									},
								},
							},
						},
						WhereClause: &sqlast.Between{
							Expr: &sqlast.Ident{
								Value: "id",
								From:  sqltoken.NewPos(7, 16),
								To:    sqltoken.NewPos(7, 18),
							},
							High: &sqlast.LongValue{
								Long: int64(2),
								From: sqltoken.NewPos(7, 33),
								To:   sqltoken.NewPos(7, 34),
							},
							Low: &sqlast.LongValue{
								Long: int64(1),
								From: sqltoken.NewPos(7, 27),
								To:   sqltoken.NewPos(7, 28),
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

				if diff := cmp.Diff(c.out, ast, IgnoreMarker); diff != "" {
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
					Create: sqltoken.NewPos(2, 0),
					Name: &sqlast.ObjectName{
						Idents: []*sqlast.Ident{
							{
								Value: "persons",
								From:  sqltoken.NewPos(2, 13),
								To:    sqltoken.NewPos(2, 20),
							},
						},
					},
					Elements: []sqlast.TableElement{
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "person_id",
								From:  sqltoken.NewPos(3, 1),
								To:    sqltoken.NewPos(3, 10),
							},
							DataType: &sqlast.UUID{
								From: sqltoken.NewPos(3, 11),
								To:   sqltoken.NewPos(3, 15),
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.UniqueColumnSpec{
										IsPrimaryKey: true,
										Primary:      sqltoken.NewPos(3, 16),
										Key:          sqltoken.NewPos(3, 27),
									},
								},
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not:  sqltoken.NewPos(3, 28),
										Null: sqltoken.NewPos(3, 36),
									},
								},
							},
						},
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "first_name",
								From:  sqltoken.NewPos(4, 1),
								To:    sqltoken.NewPos(4, 11),
							},
							DataType: &sqlast.VarcharType{
								Size:      sqlast.NewSize(255),
								Character: sqltoken.NewPos(4, 12),
								RParen:    sqltoken.NewPos(4, 24),
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.UniqueColumnSpec{
										Unique: sqltoken.NewPos(4, 25),
									},
								},
							},
						},
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "last_name",
								From:  sqltoken.NewPos(5, 1),
								To:    sqltoken.NewPos(5, 10),
							},
							DataType: &sqlast.VarcharType{
								Size:      sqlast.NewSize(255),
								Character: sqltoken.NewPos(5, 11),
								Varying:   sqltoken.NewPos(5, 28),
								RParen:    sqltoken.NewPos(5, 33),
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not:  sqltoken.NewPos(5, 34),
										Null: sqltoken.NewPos(5, 42),
									},
								},
							},
						},
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "created_at",
								From:  sqltoken.NewPos(6, 1),
								To:    sqltoken.NewPos(6, 11),
							},
							DataType: &sqlast.Timestamp{
								Timestamp: sqltoken.NewPos(6, 12),
							},
							Default: &sqlast.Ident{
								Value: "CURRENT_TIMESTAMP",
								From:  sqltoken.NewPos(6, 30),
								To:    sqltoken.NewPos(6, 47),
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not:  sqltoken.NewPos(6, 48),
										Null: sqltoken.NewPos(6, 56),
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
					Create: sqltoken.NewPos(1, 0),
					Name: &sqlast.ObjectName{
						Idents: []*sqlast.Ident{
							{
								Value: "persons",
								From:  sqltoken.NewPos(1, 13),
								To:    sqltoken.NewPos(1, 20),
							},
						},
					},
					Elements: []sqlast.TableElement{
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "person_id",
								From:  sqltoken.NewPos(2, 0),
								To:    sqltoken.NewPos(2, 9),
							},
							DataType: &sqlast.Int{
								From: sqltoken.NewPos(2, 10),
								To:   sqltoken.NewPos(2, 13),
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.UniqueColumnSpec{
										IsPrimaryKey: true,
										Primary:      sqltoken.NewPos(2, 14),
										Key:          sqltoken.NewPos(2, 25),
									},
								},
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not:  sqltoken.NewPos(2, 26),
										Null: sqltoken.NewPos(2, 34),
									},
								},
							},
						},
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "last_name",
								From:  sqltoken.NewPos(3, 0),
								To:    sqltoken.NewPos(3, 9),
							},
							DataType: &sqlast.VarcharType{
								Size:      sqlast.NewSize(255),
								Character: sqltoken.NewPos(3, 10),
								Varying:   sqltoken.NewPos(3, 27),
								RParen:    sqltoken.NewPos(3, 32),
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not:  sqltoken.NewPos(3, 33),
										Null: sqltoken.NewPos(3, 41),
									},
								},
							},
						},
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "test_id",
								From:  sqltoken.NewPos(4, 0),
								To:    sqltoken.NewPos(4, 7),
							},
							DataType: &sqlast.Int{
								From: sqltoken.NewPos(4, 8),
								To:   sqltoken.NewPos(4, 11),
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not:  sqltoken.NewPos(4, 12),
										Null: sqltoken.NewPos(4, 20),
									},
								},
								{
									Spec: &sqlast.ReferencesColumnSpec{
										References: sqltoken.NewPos(4, 21),
										RParen:     sqltoken.NewPos(4, 41),
										TableName: &sqlast.ObjectName{
											Idents: []*sqlast.Ident{
												{
													Value: "test",
													From:  sqltoken.NewPos(4, 32),
													To:    sqltoken.NewPos(4, 36),
												},
											},
										},
										Columns: []*sqlast.Ident{
											&sqlast.Ident{
												Value: "id1",
												From:  sqltoken.NewPos(4, 37),
												To:    sqltoken.NewPos(4, 40),
											},
										},
									},
								},
							},
						},
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "email",
								From:  sqltoken.NewPos(5, 0),
								To:    sqltoken.NewPos(5, 5),
							},
							DataType: &sqlast.VarcharType{
								Size:      sqlast.NewSize(255),
								Character: sqltoken.NewPos(5, 6),
								Varying:   sqltoken.NewPos(5, 23),
								RParen:    sqltoken.NewPos(5, 28),
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.UniqueColumnSpec{
										Unique: sqltoken.NewPos(5, 29),
									},
								},
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not:  sqltoken.NewPos(5, 36),
										Null: sqltoken.NewPos(5, 44),
									},
								},
							},
						},
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "age",
								From:  sqltoken.NewPos(6, 0),
								To:    sqltoken.NewPos(6, 3),
							},
							DataType: &sqlast.Int{
								From: sqltoken.NewPos(6, 4),
								To:   sqltoken.NewPos(6, 7),
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not:  sqltoken.NewPos(6, 8),
										Null: sqltoken.NewPos(6, 16),
									},
								},
								{
									Spec: &sqlast.CheckColumnSpec{
										Check:  sqltoken.NewPos(6, 17),
										RParen: sqltoken.NewPos(6, 45),
										Expr: &sqlast.BinaryExpr{
											Op: &sqlast.Operator{
												Type: sqlast.And,
												From: sqltoken.NewPos(6, 31),
												To:   sqltoken.NewPos(6, 34),
											},
											Left: &sqlast.BinaryExpr{
												Op: &sqlast.Operator{
													Type: sqlast.Gt,
													From: sqltoken.NewPos(6, 27),
													To:   sqltoken.NewPos(6, 28),
												},
												Left: &sqlast.Ident{
													Value: "age",
													From:  sqltoken.NewPos(6, 23),
													To:    sqltoken.NewPos(6, 26),
												},
												Right: &sqlast.LongValue{
													From: sqltoken.NewPos(6, 29),
													To:   sqltoken.NewPos(6, 30),
													Long: 0,
												},
											},
											Right: &sqlast.BinaryExpr{
												Op: &sqlast.Operator{
													Type: sqlast.Lt,
													From: sqltoken.NewPos(6, 39),
													To:   sqltoken.NewPos(6, 40),
												},
												Left: &sqlast.Ident{
													Value: "age",
													From:  sqltoken.NewPos(6, 35),
													To:    sqltoken.NewPos(6, 38),
												},
												Right: &sqlast.LongValue{
													From: sqltoken.NewPos(6, 41),
													To:   sqltoken.NewPos(6, 44),
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
								From:  sqltoken.NewPos(7, 0),
								To:    sqltoken.NewPos(7, 10),
							},
							DataType: &sqlast.Timestamp{
								Timestamp: sqltoken.NewPos(7, 11),
							},
							Default: &sqlast.Ident{
								Value: "CURRENT_TIMESTAMP",
								From:  sqltoken.NewPos(7, 29),
								To:    sqltoken.NewPos(7, 46),
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not:  sqltoken.NewPos(7, 47),
										Null: sqltoken.NewPos(7, 55),
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
					Create: sqltoken.NewPos(1, 0),
					Name: &sqlast.ObjectName{
						Idents: []*sqlast.Ident{
							{
								Value: "persons",
								From:  sqltoken.NewPos(1, 13),
								To:    sqltoken.NewPos(1, 20),
							},
						},
					},
					Elements: []sqlast.TableElement{
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "person_id",
								From:  sqltoken.NewPos(2, 0),
								To:    sqltoken.NewPos(2, 9),
							},
							DataType: &sqlast.Int{
								From: sqltoken.NewPos(2, 10),
								To:   sqltoken.NewPos(2, 13),
							},
						},
						&sqlast.TableConstraint{
							Constraint: sqltoken.NewPos(3, 0),
							Name: &sqlast.Ident{
								Value: "production",
								From:  sqltoken.NewPos(3, 11),
								To:    sqltoken.NewPos(3, 21),
							},
							Spec: &sqlast.UniqueTableConstraint{
								Unique: sqltoken.NewPos(3, 22),
								RParen: sqltoken.NewPos(3, 41),
								Columns: []*sqlast.Ident{&sqlast.Ident{
									Value: "test_column",
									From:  sqltoken.NewPos(3, 29),
									To:    sqltoken.NewPos(3, 40),
								}},
							},
						},
						&sqlast.TableConstraint{
							Spec: &sqlast.UniqueTableConstraint{
								Primary: sqltoken.NewPos(4, 0),
								RParen:  sqltoken.NewPos(4, 22),
								Columns: []*sqlast.Ident{&sqlast.Ident{
									Value: "person_id",
									From:  sqltoken.NewPos(4, 12),
									To:    sqltoken.NewPos(4, 21),
								}},
								IsPrimary: true,
							},
						},
						&sqlast.TableConstraint{
							Spec: &sqlast.CheckTableConstraint{
								Check:  sqltoken.NewPos(5, 0),
								RParen: sqltoken.NewPos(5, 15),
								Expr: &sqlast.BinaryExpr{
									Left: &sqlast.Ident{
										Value: "id",
										From:  sqltoken.NewPos(5, 6),
										To:    sqltoken.NewPos(5, 8),
									},
									Op: &sqlast.Operator{
										Type: sqlast.Gt,
										From: sqltoken.NewPos(5, 9),
										To:   sqltoken.NewPos(5, 10),
									},
									Right: &sqlast.LongValue{
										From: sqltoken.NewPos(5, 11),
										To:   sqltoken.NewPos(5, 14),
										Long: 100,
									},
								},
							},
						},
						&sqlast.TableConstraint{
							Spec: &sqlast.ReferentialTableConstraint{
								Foreign: sqltoken.NewPos(6, 0),
								Columns: []*sqlast.Ident{&sqlast.Ident{
									Value: "test_id",
									From:  sqltoken.NewPos(6, 12),
									To:    sqltoken.NewPos(6, 19),
								}},
								KeyExpr: &sqlast.ReferenceKeyExpr{
									TableName: &sqlast.Ident{
										Value: "other_table",
										From:  sqltoken.NewPos(6, 32),
										To:    sqltoken.NewPos(6, 43),
									},
									Columns: []*sqlast.Ident{
										&sqlast.Ident{
											Value: "col1",
											From:  sqltoken.NewPos(6, 44),
											To:    sqltoken.NewPos(6, 48),
										},
										&sqlast.Ident{
											Value: "col2",
											From:  sqltoken.NewPos(6, 50),
											To:    sqltoken.NewPos(6, 54),
										},
									},
									RParen: sqltoken.NewPos(6, 55),
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
					Name: sqlast.NewObjectName("comedies"),
					Query: &sqlast.Query{
						Body: &sqlast.SQLSelect{
							Projection: []sqlast.SQLSelectItem{&sqlast.UnnamedSelectItem{Node: &sqlast.Wildcard{}}},
							FromClause: []sqlast.TableReference{
								&sqlast.Table{
									Name: sqlast.NewObjectName("films"),
								},
							},
							WhereClause: &sqlast.BinaryExpr{
								Op:    &sqlast.Operator{Type: sqlast.Eq},
								Left:  sqlast.NewIdent("kind"),
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
					t.Fatalf("%+v", err)
				}

				if diff := cmp.Diff(c.out, ast, IgnoreMarker); diff != "" {
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
					TableName: sqlast.NewObjectName("customers"),
					Selection: &sqlast.BinaryExpr{
						Op:    &sqlast.Operator{Type: sqlast.Eq},
						Left:  sqlast.NewIdent("customer_id"),
						Right: sqlast.NewLongValue(1),
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

				if diff := cmp.Diff(c.out, ast, IgnoreMarker); diff != "" {
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
					TableName: sqlast.NewObjectName("customers"),
					Columns: []*sqlast.Ident{
						sqlast.NewIdent("customer_name"),
						sqlast.NewIdent("contract_name"),
					},
					Source: &sqlast.ConstructorSource{
						Rows: []*sqlast.RowValueExpr{
							{
								Values: []sqlast.Node{
									sqlast.NewSingleQuotedString("Cardinal"),
									sqlast.NewSingleQuotedString("Tom B. Erichsen"),
								},
							},
						},
					},
				},
			},
			{
				name: "multi record case",
				in: "INSERT INTO customers (customer_name, contract_name) VALUES" +
					"('Cardinal', 'Tom B. Erichsen')," +
					"('Cardinal', 'Tom B. Erichsen')",
				out: &sqlast.InsertStmt{
					TableName: sqlast.NewObjectName("customers"),
					Columns: []*sqlast.Ident{
						sqlast.NewIdent("customer_name"),
						sqlast.NewIdent("contract_name"),
					},
					Source: &sqlast.ConstructorSource{
						Rows: []*sqlast.RowValueExpr{
							{
								Values: []sqlast.Node{
									sqlast.NewSingleQuotedString("Cardinal"),
									sqlast.NewSingleQuotedString("Tom B. Erichsen"),
								},
							},
							{
								Values: []sqlast.Node{
									sqlast.NewSingleQuotedString("Cardinal"),
									sqlast.NewSingleQuotedString("Tom B. Erichsen"),
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

				if diff := cmp.Diff(c.out, ast, IgnoreMarker); diff != "" {
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
				out: &sqlast.AlterTableStmt{
					TableName: sqlast.NewObjectName("customers"),
					Action: &sqlast.AddColumnTableAction{
						Column: &sqlast.ColumnDef{
							Name: sqlast.NewIdent("email"),
							DataType: &sqlast.VarcharType{
								Size: sqlast.NewSize(255),
							},
						},
					},
				},
				in: "ALTER TABLE customers " +
					"ADD COLUMN email character varying(255)",
			},
			{
				name: "add constraint",
				out: &sqlast.AlterTableStmt{
					TableName: sqlast.NewObjectName("products"),
					Action: &sqlast.AddConstraintTableAction{
						Constraint: &sqlast.TableConstraint{
							Spec: &sqlast.ReferentialTableConstraint{
								Columns: []*sqlast.Ident{sqlast.NewIdent("test_id")},
								KeyExpr: &sqlast.ReferenceKeyExpr{
									TableName: sqlast.NewIdent("other_table"),
									Columns:   []*sqlast.Ident{sqlast.NewIdent("col1"), sqlast.NewIdent("col2")},
								},
							},
						},
					},
				},
				in: "ALTER TABLE products " +
					"ADD FOREIGN KEY(test_id) REFERENCES other_table(col1, col2)",
			},
			{
				name: "drop constraint",
				out: &sqlast.AlterTableStmt{
					TableName: sqlast.NewObjectName("products"),
					Action: &sqlast.DropConstraintTableAction{
						Name:    sqlast.NewIdent("fk"),
						Cascade: true,
					},
				},
				in: "ALTER TABLE products " +
					"DROP CONSTRAINT fk CASCADE",
			},
			{
				name: "remove column",
				out: &sqlast.AlterTableStmt{
					TableName: sqlast.NewObjectName("products"),
					Action: &sqlast.RemoveColumnTableAction{
						Name:    sqlast.NewIdent("description"),
						Cascade: true,
					},
				},
				in: "ALTER TABLE products " +
					"DROP COLUMN description CASCADE",
			},
			{
				name: "alter column",
				out: &sqlast.AlterTableStmt{
					TableName: sqlast.NewObjectName("products"),
					Action: &sqlast.AlterColumnTableAction{
						ColumnName: sqlast.NewIdent("created_at"),
						Action: &sqlast.SetDefaultColumnAction{
							Default: sqlast.NewIdent("current_timestamp"),
						},
					},
				},
				in: "ALTER TABLE products " +
					"ALTER COLUMN created_at SET DEFAULT current_timestamp",
			},
			{
				name: "pg change type",
				out: &sqlast.AlterTableStmt{
					TableName: sqlast.NewObjectName("products"),
					Action: &sqlast.AlterColumnTableAction{
						ColumnName: sqlast.NewIdent("number"),
						Action: &sqlast.PGAlterDataTypeColumnAction{
							DataType: &sqlast.Decimal{
								Scale:     sqlast.NewSize(10),
								Precision: sqlast.NewSize(255),
							},
						},
					},
				},
				in: "ALTER TABLE products " +
					"ALTER COLUMN number TYPE numeric(255,10)",
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

				if diff := cmp.Diff(c.out, ast, IgnoreMarker); diff != "" {
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
					TableName: sqlast.NewObjectName("customers"),
					Assignments: []*sqlast.Assignment{
						{
							ID:    sqlast.NewIdent("contract_name"),
							Value: sqlast.NewSingleQuotedString("Alfred Schmidt"),
						},
						{
							ID:    sqlast.NewIdent("city"),
							Value: sqlast.NewSingleQuotedString("Frankfurt"),
						},
					},
					Selection: &sqlast.BinaryExpr{
						Op:    &sqlast.Operator{Type: sqlast.Eq},
						Left:  sqlast.NewIdent("customer_id"),
						Right: sqlast.NewLongValue(1),
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

				if diff := cmp.Diff(c.out, ast, IgnoreMarker); diff != "" {
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
