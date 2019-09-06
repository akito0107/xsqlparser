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
						Select: sqltoken.Pos{
							Line: 1,
							Col:  0,
						},
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedSelectItem{
								Node: &sqlast.Ident{
									Value: "test",
									From:  sqltoken.Pos{Line: 1, Col: 7},
									To:    sqltoken.Pos{Line: 1, Col: 11},
								},
							},
						},
						FromClause: []sqlast.TableReference{
							&sqlast.Table{
								Name: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										{
											Value: "test_table",
											From:  sqltoken.Pos{Line: 1, Col: 17},
											To:    sqltoken.Pos{Line: 1, Col: 27},
										},
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
						Select: sqltoken.Pos{
							Line: 1,
							Col:  0,
						},
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedSelectItem{
								Node: &sqlast.Ident{
									Value: "test",
									From:  sqltoken.Pos{Line: 1, Col: 7},
									To:    sqltoken.Pos{Line: 1, Col: 11},
								},
							},
						},
						FromClause: []sqlast.TableReference{
							&sqlast.Table{
								Name: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										{
											Value: "test_table",
											From:  sqltoken.Pos{Line: 1, Col: 17},
											To:    sqltoken.Pos{Line: 1, Col: 27},
										},
									},
								},
							},
						},
						WhereClause: &sqlast.BinaryExpr{
							Left: &sqlast.CompoundIdent{
								Idents: []*sqlast.Ident{
									{
										Value: "test_table",
										From:  sqltoken.Pos{Line: 1, Col: 34},
										To:    sqltoken.Pos{Line: 1, Col: 44},
									},
									{
										Value: "column1",
										From:  sqltoken.Pos{Line: 1, Col: 45},
										To:    sqltoken.Pos{Line: 1, Col: 52},
									},
								},
							},
							Op: &sqlast.Operator{
								Type: sqlast.Eq,
								From: sqltoken.Pos{Line: 1, Col: 53},
								To:   sqltoken.Pos{Line: 1, Col: 54},
							},
							Right: &sqlast.SingleQuotedString{
								From:   sqltoken.Pos{Line: 1, Col: 55},
								To:     sqltoken.Pos{Line: 1, Col: 61},
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
						Select: sqltoken.Pos{Line: 1, Col: 0},
						Projection: []sqlast.SQLSelectItem{
							&sqlast.AliasSelectItem{
								Expr: &sqlast.Function{
									Name: &sqlast.ObjectName{
										Idents: []*sqlast.Ident{
											{
												Value: "COUNT",
												From:  sqltoken.Pos{Line: 1, Col: 7},
												To:    sqltoken.Pos{Line: 1, Col: 12},
											},
										},
									},
									Args: []sqlast.Node{&sqlast.CompoundIdent{
										Idents: []*sqlast.Ident{
											{
												Value: "t1",
												From:  sqltoken.Pos{Line: 1, Col: 13},
												To:    sqltoken.Pos{Line: 1, Col: 15},
											},
											{
												Value: "id",
												From:  sqltoken.Pos{Line: 1, Col: 16},
												To:    sqltoken.Pos{Line: 1, Col: 18},
											},
										},
									}},
									ArgsRParen: sqltoken.Pos{Line: 1, Col: 19},
								},
								Alias: &sqlast.Ident{
									Value: "c",
									From:  sqltoken.Pos{Line: 1, Col: 23},
									To:    sqltoken.Pos{Line: 1, Col: 24},
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
													From:  sqltoken.Pos{Line: 1, Col: 30},
													To:    sqltoken.Pos{Line: 1, Col: 40},
												},
											},
										},
										Alias: &sqlast.Ident{
											Value: "t1",
											From:  sqltoken.Pos{Line: 1, Col: 44},
											To:    sqltoken.Pos{Line: 1, Col: 46},
										},
									},
								},
								Type: &sqlast.JoinType{
									Condition: sqlast.LEFT,
									From:      sqltoken.Pos{Line: 1, Col: 47},
									To:        sqltoken.Pos{Line: 1, Col: 51},
								},
								RightElement: &sqlast.TableJoinElement{
									Ref: &sqlast.Table{
										Name: &sqlast.ObjectName{
											Idents: []*sqlast.Ident{
												{
													Value: "test_table2",
													From:  sqltoken.Pos{Line: 1, Col: 57},
													To:    sqltoken.Pos{Line: 1, Col: 68},
												},
											},
										},
										Alias: &sqlast.Ident{
											Value: "t2",
											From:  sqltoken.Pos{Line: 1, Col: 72},
											To:    sqltoken.Pos{Line: 1, Col: 74},
										},
									},
								},
								Spec: &sqlast.JoinCondition{
									On: sqltoken.Pos{Line: 1, Col: 75},
									SearchCondition: &sqlast.BinaryExpr{
										Left: &sqlast.CompoundIdent{
											Idents: []*sqlast.Ident{
												{
													Value: "t1",
													From:  sqltoken.Pos{Line: 1, Col: 78},
													To:    sqltoken.Pos{Line: 1, Col: 80},
												},
												{
													Value: "id",
													From:  sqltoken.Pos{Line: 1, Col: 81},
													To:    sqltoken.Pos{Line: 1, Col: 83},
												},
											},
										},
										Op: &sqlast.Operator{
											Type: sqlast.Eq,
											From: sqltoken.Pos{Line: 1, Col: 84},
											To:   sqltoken.Pos{Line: 1, Col: 85},
										},
										Right: &sqlast.CompoundIdent{
											Idents: []*sqlast.Ident{
												{
													Value: "t2",
													From:  sqltoken.Pos{Line: 1, Col: 86},
													To:    sqltoken.Pos{Line: 1, Col: 88},
												},
												{
													Value: "test_table_id",
													From:  sqltoken.Pos{Line: 1, Col: 89},
													To:    sqltoken.Pos{Line: 1, Col: 102},
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
						Select: sqltoken.Pos{Col: 0, Line: 1},
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedSelectItem{
								Node: &sqlast.Function{
									Name: &sqlast.ObjectName{
										Idents: []*sqlast.Ident{
											{
												Value: "COUNT",
												From:  sqltoken.Pos{Line: 1, Col: 7},
												To:    sqltoken.Pos{Line: 1, Col: 12},
											},
										},
									},
									Args: []sqlast.Node{
										&sqlast.Ident{
											Value: "customer_id",
											From:  sqltoken.Pos{Line: 1, Col: 13},
											To:    sqltoken.Pos{Line: 1, Col: 24},
										},
									},
									ArgsRParen: sqltoken.Pos{Line: 1, Col: 25},
								},
							},
							&sqlast.QualifiedWildcardSelectItem{
								Prefix: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										{
											Value: "country",
											From:  sqltoken.Pos{Line: 1, Col: 27},
											To:    sqltoken.Pos{Line: 1, Col: 34},
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
											From:  sqltoken.Pos{Line: 1, Col: 42},
											To:    sqltoken.Pos{Line: 1, Col: 51},
										},
									},
								},
							},
						},
						GroupByClause: []sqlast.Node{
							&sqlast.Ident{
								Value: "country",
								From:  sqltoken.Pos{Line: 1, Col: 61},
								To:    sqltoken.Pos{Line: 1, Col: 68},
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
						Select: sqltoken.Pos{Col: 0, Line: 1},
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedSelectItem{
								Node: &sqlast.Function{
									Name: &sqlast.ObjectName{
										Idents: []*sqlast.Ident{
											{
												Value: "COUNT",
												From:  sqltoken.Pos{Line: 1, Col: 7},
												To:    sqltoken.Pos{Line: 1, Col: 12},
											},
										},
									},
									Args: []sqlast.Node{
										&sqlast.Ident{
											Value: "customer_id",
											From:  sqltoken.Pos{Line: 1, Col: 13},
											To:    sqltoken.Pos{Line: 1, Col: 24},
										},
									},
									ArgsRParen: sqltoken.Pos{Line: 1, Col: 25},
								},
							},
							&sqlast.UnnamedSelectItem{
								Node: &sqlast.Ident{
									Value: "country",
									From:  sqltoken.Pos{Line: 1, Col: 27},
									To:    sqltoken.Pos{Line: 1, Col: 34},
								},
							},
						},
						FromClause: []sqlast.TableReference{
							&sqlast.Table{
								Name: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										{
											Value: "customers",
											From:  sqltoken.Pos{Line: 2, Col: 5},
											To:    sqltoken.Pos{Line: 2, Col: 14},
										},
									},
								},
							},
						},
						GroupByClause: []sqlast.Node{
							&sqlast.Ident{
								Value: "country",
								From:  sqltoken.Pos{Line: 3, Col: 9},
								To:    sqltoken.Pos{Line: 3, Col: 16},
							},
						},
						HavingClause: &sqlast.BinaryExpr{
							Op: &sqlast.Operator{
								Type: sqlast.Gt,
								From: sqltoken.Pos{Line: 4, Col: 26},
								To:   sqltoken.Pos{Line: 4, Col: 27},
							},
							Left: &sqlast.Function{
								Name: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										{
											Value: "COUNT",
											From:  sqltoken.Pos{Line: 4, Col: 7},
											To:    sqltoken.Pos{Line: 4, Col: 12},
										},
									},
								},
								Args: []sqlast.Node{
									&sqlast.Ident{
										Value: "customer_id",
										From:  sqltoken.Pos{Line: 4, Col: 13},
										To:    sqltoken.Pos{Line: 4, Col: 24},
									},
								},
								ArgsRParen: sqltoken.Pos{Line: 4, Col: 25},
							},
							Right: &sqlast.LongValue{
								From: sqltoken.Pos{Line: 4, Col: 28},
								To:   sqltoken.Pos{Line: 4, Col: 29},
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
						Select: sqltoken.Pos{Line: 1, Col: 0},
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedSelectItem{
								Node: &sqlast.Ident{
									Value: "product",
									From:  sqltoken.Pos{Line: 1, Col: 7},
									To:    sqltoken.Pos{Line: 1, Col: 14},
								},
							},
							&sqlast.AliasSelectItem{
								Alias: &sqlast.Ident{
									Value: "product_units",
									From:  sqltoken.Pos{Line: 1, Col: 33},
									To:    sqltoken.Pos{Line: 1, Col: 46},
								},
								Expr: &sqlast.Function{
									Name: &sqlast.ObjectName{
										Idents: []*sqlast.Ident{
											{
												Value: "SUM",
												From:  sqltoken.Pos{Line: 1, Col: 16},
												To:    sqltoken.Pos{Line: 1, Col: 19},
											},
										},
									},
									Args: []sqlast.Node{
										&sqlast.Ident{
											Value: "quantity",
											From:  sqltoken.Pos{Line: 1, Col: 20},
											To:    sqltoken.Pos{Line: 1, Col: 28},
										},
									},
									ArgsRParen: sqltoken.Pos{Line: 1, Col: 29},
								},
							},
						},
						FromClause: []sqlast.TableReference{
							&sqlast.Table{
								Name: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										{
											Value: "orders",
											From:  sqltoken.Pos{Line: 2, Col: 5},
											To:    sqltoken.Pos{Line: 2, Col: 11},
										},
									},
								},
							},
						},
						WhereClause: &sqlast.InSubQuery{
							Expr: &sqlast.Ident{
								Value: "region",
								From:  sqltoken.Pos{Line: 3, Col: 6},
								To:    sqltoken.Pos{Line: 3, Col: 12},
							},
							RParen: sqltoken.Pos{Line: 3, Col: 48},
							SubQuery: &sqlast.Query{
								Body: &sqlast.SQLSelect{
									Select: sqltoken.Pos{Line: 3, Col: 17},
									Projection: []sqlast.SQLSelectItem{
										&sqlast.UnnamedSelectItem{
											Node: &sqlast.Ident{
												Value: "region",
												From:  sqltoken.Pos{Line: 3, Col: 24},
												To:    sqltoken.Pos{Line: 3, Col: 30},
											},
										},
									},
									FromClause: []sqlast.TableReference{
										&sqlast.Table{
											Name: &sqlast.ObjectName{
												Idents: []*sqlast.Ident{
													{
														Value: "top_regions",
														From:  sqltoken.Pos{Line: 3, Col: 36},
														To:    sqltoken.Pos{Line: 3, Col: 47},
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
								From:  sqltoken.Pos{Line: 4, Col: 9},
								To:    sqltoken.Pos{Line: 4, Col: 22},
							},
						},
					},
					Limit: &sqlast.LimitExpr{
						LimitValue: &sqlast.LongValue{
							From: sqltoken.Pos{Line: 4, Col: 29},
							To:   sqltoken.Pos{Line: 4, Col: 32},
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
								From:  sqltoken.Pos{Line: 1, Col: 5},
								To:    sqltoken.Pos{Line: 1, Col: 19},
							},
							Query: &sqlast.Query{
								Body: &sqlast.SQLSelect{
									Select: sqltoken.Pos{Line: 1, Col: 24},
									Projection: []sqlast.SQLSelectItem{
										&sqlast.UnnamedSelectItem{
											Node: &sqlast.Ident{
												Value: "region",
												From:  sqltoken.Pos{Line: 1, Col: 31},
												To:    sqltoken.Pos{Line: 1, Col: 37},
											},
										},
										&sqlast.AliasSelectItem{
											Alias: &sqlast.Ident{
												Value: "total_sales",
												From:  sqltoken.Pos{Line: 1, Col: 54},
												To:    sqltoken.Pos{Line: 1, Col: 65},
											},
											Expr: &sqlast.Function{
												Name: &sqlast.ObjectName{
													Idents: []*sqlast.Ident{
														{
															Value: "SUM",
															From:  sqltoken.Pos{Line: 1, Col: 39},
															To:    sqltoken.Pos{Line: 1, Col: 42},
														},
													},
												},
												Args: []sqlast.Node{
													&sqlast.Ident{
														Value: "amount",
														From:  sqltoken.Pos{Line: 1, Col: 43},
														To:    sqltoken.Pos{Line: 1, Col: 49},
													},
												},
												ArgsRParen: sqltoken.Pos{Line: 1, Col: 50},
											},
										},
									},
									FromClause: []sqlast.TableReference{
										&sqlast.Table{
											Name: &sqlast.ObjectName{
												Idents: []*sqlast.Ident{
													{
														Value: "orders",
														From:  sqltoken.Pos{Line: 1, Col: 71},
														To:    sqltoken.Pos{Line: 1, Col: 77},
													},
												},
											},
										},
									},
									GroupByClause: []sqlast.Node{
										&sqlast.Ident{
											Value: "region",
											From:  sqltoken.Pos{Line: 1, Col: 87},
											To:    sqltoken.Pos{Line: 1, Col: 93},
										},
									},
								},
							},
						},
					},
					Body: &sqlast.SQLSelect{
						Select: sqltoken.Pos{Line: 2, Col: 0},
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedSelectItem{Node: &sqlast.Ident{
								Value: "product",
								From:  sqltoken.Pos{Line: 2, Col: 7},
								To:    sqltoken.Pos{Line: 2, Col: 14},
							}},
							&sqlast.AliasSelectItem{
								Alias: &sqlast.Ident{
									Value: "product_units",
									From:  sqltoken.Pos{Line: 2, Col: 33},
									To:    sqltoken.Pos{Line: 2, Col: 46},
								},
								Expr: &sqlast.Function{
									Name: &sqlast.ObjectName{
										Idents: []*sqlast.Ident{
											{
												Value: "SUM",
												From:  sqltoken.Pos{Line: 2, Col: 16},
												To:    sqltoken.Pos{Line: 2, Col: 19},
											},
										},
									},
									Args: []sqlast.Node{
										&sqlast.Ident{
											Value: "quantity",
											From:  sqltoken.Pos{Line: 2, Col: 20},
											To:    sqltoken.Pos{Line: 2, Col: 28},
										},
									},
									ArgsRParen: sqltoken.Pos{Line: 2, Col: 29},
								},
							},
						},
						FromClause: []sqlast.TableReference{
							&sqlast.Table{
								Name: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										{
											Value: "orders",
											From:  sqltoken.Pos{Line: 3, Col: 5},
											To:    sqltoken.Pos{Line: 3, Col: 11},
										},
									},
								},
							},
						},
						WhereClause: &sqlast.InSubQuery{
							RParen: sqltoken.Pos{
								Line: 4,
								Col:  48,
							},
							Expr: &sqlast.Ident{
								Value: "region",
								From:  sqltoken.Pos{Line: 4, Col: 6},
								To:    sqltoken.Pos{Line: 4, Col: 12},
							},
							SubQuery: &sqlast.Query{
								Body: &sqlast.SQLSelect{
									Select: sqltoken.Pos{
										Line: 4,
										Col:  17,
									},
									Projection: []sqlast.SQLSelectItem{
										&sqlast.UnnamedSelectItem{
											Node: &sqlast.Ident{
												Value: "region",
												From:  sqltoken.Pos{Line: 4, Col: 24},
												To:    sqltoken.Pos{Line: 4, Col: 30},
											},
										},
									},
									FromClause: []sqlast.TableReference{
										&sqlast.Table{
											Name: &sqlast.ObjectName{
												Idents: []*sqlast.Ident{
													{
														Value: "top_regions",
														From:  sqltoken.Pos{Line: 4, Col: 36},
														To:    sqltoken.Pos{Line: 4, Col: 47},
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
								From:  sqltoken.Pos{Line: 5, Col: 9},
								To:    sqltoken.Pos{Line: 5, Col: 15},
							},
							&sqlast.Ident{
								Value: "product",
								From:  sqltoken.Pos{Line: 5, Col: 17},
								To:    sqltoken.Pos{Line: 5, Col: 24},
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
						Select: sqltoken.Pos{Line: 1, Col: 0},
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedSelectItem{
								Node: &sqlast.Wildcard{
									Wildcard: sqltoken.Pos{Line: 1, Col: 7},
								},
							},
						},
						FromClause: []sqlast.TableReference{
							&sqlast.Table{
								Name: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										{
											Value: "user",
											From:  sqltoken.Pos{Line: 1, Col: 14},
											To:    sqltoken.Pos{Line: 1, Col: 18},
										},
									},
								},
							},
						},
						WhereClause: &sqlast.Exists{
							Negated: true,
							Exists: sqltoken.Pos{
								Line: 1,
								Col:  29,
							},
							Not: sqltoken.Pos{
								Line: 1,
								Col:  25,
							},
							RParen: sqltoken.Pos{
								Line: 4,
								Col:  53,
							},
							Query: &sqlast.Query{
								Body: &sqlast.SQLSelect{
									Select: sqltoken.Pos{Line: 2, Col: 1},
									Projection: []sqlast.SQLSelectItem{
										&sqlast.UnnamedSelectItem{
											Node: &sqlast.Wildcard{
												Wildcard: sqltoken.Pos{Line: 2, Col: 8},
											},
										},
									},
									FromClause: []sqlast.TableReference{
										&sqlast.Table{
											Name: &sqlast.ObjectName{
												Idents: []*sqlast.Ident{
													{
														Value: "user_sub",
														From:  sqltoken.Pos{Line: 3, Col: 5},
														To:    sqltoken.Pos{Line: 3, Col: 13},
													},
												},
											},
										},
									},
									WhereClause: &sqlast.BinaryExpr{
										Op: &sqlast.Operator{Type: sqlast.And, From: sqltoken.Pos{Line: 4, Col: 28}, To: sqltoken.Pos{Line: 4, Col: 31}},
										Left: &sqlast.BinaryExpr{
											Op: &sqlast.Operator{Type: sqlast.Eq, From: sqltoken.Pos{Line: 4, Col: 14}, To: sqltoken.Pos{Line: 4, Col: 15}},
											Left: &sqlast.CompoundIdent{
												Idents: []*sqlast.Ident{
													{
														Value: "user",
														From:  sqltoken.Pos{Line: 4, Col: 6},
														To:    sqltoken.Pos{Line: 4, Col: 10},
													},
													{
														Value: "id",
														From:  sqltoken.Pos{Line: 4, Col: 11},
														To:    sqltoken.Pos{Line: 4, Col: 13},
													},
												},
											},
											Right: &sqlast.CompoundIdent{
												Idents: []*sqlast.Ident{
													{
														Value: "user_sub",
														From:  sqltoken.Pos{Line: 4, Col: 16},
														To:    sqltoken.Pos{Line: 4, Col: 24},
													},
													{
														Value: "id",
														From:  sqltoken.Pos{Line: 4, Col: 25},
														To:    sqltoken.Pos{Line: 4, Col: 27},
													},
												},
											},
										},
										Right: &sqlast.BinaryExpr{
											Op: &sqlast.Operator{Type: sqlast.Eq, From: sqltoken.Pos{Line: 4, Col: 45}, To: sqltoken.Pos{Line: 4, Col: 46}},
											Left: &sqlast.CompoundIdent{
												Idents: []*sqlast.Ident{
													{
														Value: "user_sub",
														From:  sqltoken.Pos{Line: 4, Col: 32},
														To:    sqltoken.Pos{Line: 4, Col: 40},
													},
													{
														Value: "job",
														From:  sqltoken.Pos{Line: 4, Col: 41},
														To:    sqltoken.Pos{Line: 4, Col: 44},
													},
												},
											},
											Right: &sqlast.SingleQuotedString{
												From:   sqltoken.Pos{Line: 4, Col: 47},
												To:     sqltoken.Pos{Line: 4, Col: 52},
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
						Select: sqltoken.Pos{Line: 1, Col: 0},
						Projection: []sqlast.SQLSelectItem{
							&sqlast.AliasSelectItem{
								Expr: &sqlast.CaseExpr{
									Case:    sqltoken.Pos{Line: 2, Col: 0},
									CaseEnd: sqltoken.Pos{Line: 6, Col: 3},
									Conditions: []sqlast.Node{
										&sqlast.BinaryExpr{
											Op: &sqlast.Operator{
												Type: sqlast.Eq,
												From: sqltoken.Pos{Line: 3, Col: 12},
												To:   sqltoken.Pos{Line: 3, Col: 13},
											},
											Left: &sqlast.Ident{
												Value: "expr1",
												From:  sqltoken.Pos{Line: 3, Col: 6},
												To:    sqltoken.Pos{Line: 3, Col: 11},
											},
											Right: &sqlast.SingleQuotedString{
												From:   sqltoken.Pos{Line: 3, Col: 14},
												To:     sqltoken.Pos{Line: 3, Col: 17},
												String: "1",
											},
										},
										&sqlast.BinaryExpr{
											Op: &sqlast.Operator{
												Type: sqlast.Eq,
												From: sqltoken.Pos{Line: 4, Col: 12},
												To:   sqltoken.Pos{Line: 4, Col: 13},
											},
											Left: &sqlast.Ident{
												Value: "expr2",
												From:  sqltoken.Pos{Line: 4, Col: 6},
												To:    sqltoken.Pos{Line: 4, Col: 11},
											},
											Right: &sqlast.SingleQuotedString{
												From:   sqltoken.Pos{Line: 4, Col: 14},
												To:     sqltoken.Pos{Line: 4, Col: 17},
												String: "2",
											},
										},
									},
									Results: []sqlast.Node{
										&sqlast.SingleQuotedString{
											From:   sqltoken.Pos{Line: 3, Col: 23},
											To:     sqltoken.Pos{Line: 3, Col: 30},
											String: "test1",
										},
										&sqlast.SingleQuotedString{
											From:   sqltoken.Pos{Line: 4, Col: 23},
											To:     sqltoken.Pos{Line: 4, Col: 30},
											String: "test2",
										},
									},
									ElseResult: &sqlast.SingleQuotedString{
										From:   sqltoken.Pos{Line: 5, Col: 6},
										To:     sqltoken.Pos{Line: 5, Col: 13},
										String: "other",
									},
								},
								Alias: &sqlast.Ident{
									Value: "alias",
									From:  sqltoken.Pos{Line: 6, Col: 7},
									To:    sqltoken.Pos{Line: 6, Col: 12},
								},
							},
						},
						FromClause: []sqlast.TableReference{
							&sqlast.Table{
								Name: &sqlast.ObjectName{
									Idents: []*sqlast.Ident{
										{
											Value: "user",
											From:  sqltoken.Pos{Line: 7, Col: 5},
											To:    sqltoken.Pos{Line: 7, Col: 9},
										},
									},
								},
							},
						},
						WhereClause: &sqlast.Between{
							Expr: &sqlast.Ident{
								Value: "id",
								From:  sqltoken.Pos{Line: 7, Col: 16},
								To:    sqltoken.Pos{Line: 7, Col: 18},
							},
							High: &sqlast.LongValue{
								Long: int64(2),
								From: sqltoken.Pos{Line: 7, Col: 33},
								To:   sqltoken.Pos{Line: 7, Col: 34},
							},
							Low: &sqlast.LongValue{
								Long: int64(1),
								From: sqltoken.Pos{Line: 7, Col: 27},
								To:   sqltoken.Pos{Line: 7, Col: 28},
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
					Create: sqltoken.Pos{Line: 2, Col: 0},
					Name: &sqlast.ObjectName{
						Idents: []*sqlast.Ident{
							{
								Value: "persons",
								From:  sqltoken.Pos{Line: 2, Col: 13},
								To:    sqltoken.Pos{Line: 2, Col: 20},
							},
						},
					},
					Elements: []sqlast.TableElement{
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "person_id",
								From:  sqltoken.Pos{Line: 3, Col: 1},
								To:    sqltoken.Pos{Line: 3, Col: 10},
							},
							DataType: &sqlast.UUID{
								From: sqltoken.Pos{Line: 3, Col: 11},
								To:   sqltoken.Pos{Line: 3, Col: 15},
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.UniqueColumnSpec{
										IsPrimaryKey: true,
										Primary:      sqltoken.Pos{Line: 3, Col: 16},
										Key:          sqltoken.Pos{Line: 3, Col: 27},
									},
								},
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not:  sqltoken.Pos{Line: 3, Col: 28},
										Null: sqltoken.Pos{Line: 3, Col: 36},
									},
								},
							},
						},
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "first_name",
								From:  sqltoken.Pos{Line: 4, Col: 1},
								To:    sqltoken.Pos{Line: 4, Col: 11},
							},
							DataType: &sqlast.VarcharType{
								Size:      sqlast.NewSize(255),
								Character: sqltoken.Pos{Line: 4, Col: 12},
								RParen:    sqltoken.Pos{Line: 4, Col: 24},
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.UniqueColumnSpec{
										Unique: sqltoken.Pos{Line: 4, Col: 25},
									},
								},
							},
						},
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "last_name",
								From:  sqltoken.Pos{Line: 5, Col: 1},
								To:    sqltoken.Pos{Line: 5, Col: 10},
							},
							DataType: &sqlast.VarcharType{
								Size:      sqlast.NewSize(255),
								Character: sqltoken.Pos{Line: 5, Col: 11},
								Varying:   sqltoken.Pos{Line: 5, Col: 28},
								RParen:    sqltoken.Pos{Line: 5, Col: 33},
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not:  sqltoken.Pos{Line: 5, Col: 34},
										Null: sqltoken.Pos{Line: 5, Col: 42},
									},
								},
							},
						},
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "created_at",
								From:  sqltoken.Pos{Line: 6, Col: 1},
								To:    sqltoken.Pos{Line: 6, Col: 11},
							},
							DataType: &sqlast.Timestamp{
								Timestamp: sqltoken.Pos{Line: 6, Col: 12},
							},
							Default: &sqlast.Ident{
								Value: "CURRENT_TIMESTAMP",
								From:  sqltoken.Pos{Line: 6, Col: 30},
								To:    sqltoken.Pos{Line: 6, Col: 47},
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not: sqltoken.Pos{
											Line: 6,
											Col:  48,
										},
										Null: sqltoken.Pos{
											Line: 6,
											Col:  56,
										},
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
					Create: sqltoken.Pos{Line: 1, Col: 0},
					Name: &sqlast.ObjectName{
						Idents: []*sqlast.Ident{
							{
								Value: "persons",
								From:  sqltoken.Pos{Line: 1, Col: 13},
								To:    sqltoken.Pos{Line: 1, Col: 20},
							},
						},
					},
					Elements: []sqlast.TableElement{
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "person_id",
								From:  sqltoken.Pos{Line: 2, Col: 0},
								To:    sqltoken.Pos{Line: 2, Col: 9},
							},
							DataType: &sqlast.Int{
								From: sqltoken.Pos{Line: 2, Col: 10},
								To:   sqltoken.Pos{Line: 2, Col: 13},
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.UniqueColumnSpec{
										IsPrimaryKey: true,
										Primary:      sqltoken.Pos{Line: 2, Col: 14},
										Key:          sqltoken.Pos{Line: 2, Col: 25},
									},
								},
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not:  sqltoken.Pos{Line: 2, Col: 26},
										Null: sqltoken.Pos{Line: 2, Col: 34},
									},
								},
							},
						},
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "last_name",
								From:  sqltoken.Pos{Line: 3, Col: 0},
								To:    sqltoken.Pos{Line: 3, Col: 9},
							},
							DataType: &sqlast.VarcharType{
								Size:      sqlast.NewSize(255),
								Character: sqltoken.Pos{Line: 3, Col: 10},
								Varying:   sqltoken.Pos{Line: 3, Col: 27},
								RParen:    sqltoken.Pos{Line: 3, Col: 32},
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not:  sqltoken.Pos{Line: 3, Col: 33},
										Null: sqltoken.Pos{Line: 3, Col: 41},
									},
								},
							},
						},
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "test_id",
								From:  sqltoken.Pos{Line: 4, Col: 0},
								To:    sqltoken.Pos{Line: 4, Col: 7},
							},
							DataType: &sqlast.Int{
								From: sqltoken.Pos{Line: 4, Col: 8},
								To:   sqltoken.Pos{Line: 4, Col: 11},
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not:  sqltoken.Pos{Line: 4, Col: 12},
										Null: sqltoken.Pos{Line: 4, Col: 20},
									},
								},
								{
									Spec: &sqlast.ReferencesColumnSpec{
										References: sqltoken.Pos{
											Line: 4,
											Col:  21,
										},
										RParen: sqltoken.Pos{
											Line: 4,
											Col:  41,
										},
										TableName: &sqlast.ObjectName{
											Idents: []*sqlast.Ident{
												{
													Value: "test",
													From:  sqltoken.Pos{Line: 4, Col: 32},
													To:    sqltoken.Pos{Line: 4, Col: 36},
												},
											},
										},
										Columns: []*sqlast.Ident{
											&sqlast.Ident{
												Value: "id1",
												From:  sqltoken.Pos{Line: 4, Col: 37},
												To:    sqltoken.Pos{Line: 4, Col: 40},
											},
										},
									},
								},
							},
						},
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "email",
								From:  sqltoken.Pos{Line: 5, Col: 0},
								To:    sqltoken.Pos{Line: 5, Col: 5},
							},
							DataType: &sqlast.VarcharType{
								Size:      sqlast.NewSize(255),
								Character: sqltoken.Pos{Line: 5, Col: 6},
								Varying:   sqltoken.Pos{Line: 5, Col: 23},
								RParen:    sqltoken.Pos{Line: 5, Col: 28},
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.UniqueColumnSpec{
										Unique: sqltoken.Pos{Line: 5, Col: 29},
									},
								},
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not:  sqltoken.Pos{Line: 5, Col: 36},
										Null: sqltoken.Pos{Line: 5, Col: 44},
									},
								},
							},
						},
						&sqlast.ColumnDef{
							Name: &sqlast.Ident{
								Value: "age",
								From:  sqltoken.Pos{Line: 6, Col: 0},
								To:    sqltoken.Pos{Line: 6, Col: 3},
							},
							DataType: &sqlast.Int{
								From: sqltoken.Pos{Line: 6, Col: 4},
								To:   sqltoken.Pos{Line: 6, Col: 7},
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not:  sqltoken.Pos{Line: 6, Col: 8},
										Null: sqltoken.Pos{Line: 6, Col: 16},
									},
								},
								{
									Spec: &sqlast.CheckColumnSpec{
										Check: sqltoken.Pos{
											Line: 6,
											Col:  17,
										},
										RParen: sqltoken.Pos{
											Line: 6,
											Col:  45,
										},
										Expr: &sqlast.BinaryExpr{
											Op: &sqlast.Operator{
												Type: sqlast.And,
												From: sqltoken.Pos{Line: 6, Col: 31},
												To:   sqltoken.Pos{Line: 6, Col: 34},
											},
											Left: &sqlast.BinaryExpr{
												Op: &sqlast.Operator{
													Type: sqlast.Gt,
													From: sqltoken.Pos{Line: 6, Col: 27},
													To:   sqltoken.Pos{Line: 6, Col: 28},
												},
												Left: &sqlast.Ident{
													Value: "age",
													From:  sqltoken.Pos{Line: 6, Col: 23},
													To:    sqltoken.Pos{Line: 6, Col: 26},
												},
												Right: &sqlast.LongValue{
													From: sqltoken.Pos{
														Line: 6,
														Col:  29,
													},
													To:   sqltoken.Pos{Line: 6, Col: 30},
													Long: 0,
												},
											},
											Right: &sqlast.BinaryExpr{
												Op: &sqlast.Operator{
													Type: sqlast.Lt,
													From: sqltoken.Pos{Line: 6, Col: 39},
													To: sqltoken.Pos{
														Line: 6,
														Col:  40,
													},
												},
												Left: &sqlast.Ident{
													Value: "age",
													From:  sqltoken.Pos{Line: 6, Col: 35},
													To:    sqltoken.Pos{Line: 6, Col: 38},
												},
												Right: &sqlast.LongValue{
													From: sqltoken.Pos{
														Line: 6,
														Col:  41,
													},
													To: sqltoken.Pos{
														Line: 6,
														Col:  44,
													},
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
								From:  sqltoken.Pos{Line: 7, Col: 0},
								To:    sqltoken.Pos{Line: 7, Col: 10},
							},
							DataType: &sqlast.Timestamp{
								Timestamp: sqltoken.Pos{Line: 7, Col: 11},
							},
							Default: &sqlast.Ident{
								Value: "CURRENT_TIMESTAMP",
								From:  sqltoken.Pos{Line: 7, Col: 29},
								To:    sqltoken.Pos{Line: 7, Col: 46},
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.NotNullColumnSpec{
										Not: sqltoken.Pos{
											Line: 7,
											Col:  47,
										},
										Null: sqltoken.Pos{
											Line: 7,
											Col:  55,
										},
									},
								},
							},
						},
					},
				},
			},
			{
				name: "with table constraint",
				skip: true,
				in: "CREATE TABLE persons (" +
					"person_id int, " +
					"CONSTRAINT production UNIQUE(test_column), " +
					"PRIMARY KEY(person_id), " +
					"CHECK(id > 100), " +
					"FOREIGN KEY(test_id) REFERENCES other_table(col1, col2)" +
					")",
				out: &sqlast.CreateTableStmt{
					Name: sqlast.NewObjectName("persons"),
					Elements: []sqlast.TableElement{
						&sqlast.ColumnDef{
							Name:     sqlast.NewIdent("person_id"),
							DataType: &sqlast.Int{},
						},
						&sqlast.TableConstraint{
							Name: sqlast.NewIdent("production"),
							Spec: &sqlast.UniqueTableConstraint{
								Columns: []*sqlast.Ident{sqlast.NewIdent("test_column")},
							},
						},
						&sqlast.TableConstraint{
							Spec: &sqlast.UniqueTableConstraint{
								Columns:   []*sqlast.Ident{sqlast.NewIdent("person_id")},
								IsPrimary: true,
							},
						},
						&sqlast.TableConstraint{
							Spec: &sqlast.CheckTableConstraint{
								Expr: &sqlast.BinaryExpr{
									Left:  sqlast.NewIdent("id"),
									Op:    &sqlast.Operator{Type: sqlast.Gt},
									Right: sqlast.NewLongValue(100),
								},
							},
						},
						&sqlast.TableConstraint{
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
			},
			{
				name: "create view",
				skip: true,
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
