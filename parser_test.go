package xsqlparser

import (
	"bytes"
	"reflect"
	"testing"
	"unicode"

	"github.com/akito0107/xsqlparser/dialect"
	"github.com/akito0107/xsqlparser/sqlast"
	"github.com/google/go-cmp/cmp"
)

var ignoreMarker = cmp.FilterPath(func(paths cmp.Path) bool {
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
			{
				name: "exists",
				in: "SELECT * FROM user WHERE NOT EXISTS (" +
					"SELECT * FROM user_sub WHERE user.id = user_sub.id AND user_sub.job = 'job'" +
					");",
				out: &sqlast.SQLQuery{
					Body: &sqlast.SQLSelect{
						Projection: []sqlast.SQLSelectItem{
							&sqlast.UnnamedExpression{
								Node: &sqlast.SQLWildcard{},
							},
						},
						Relation: &sqlast.Table{
							Name: sqlast.NewSQLObjectName("user"),
						},
						Selection: &sqlast.SQLExists{
							Negated: true,
							Query: &sqlast.SQLQuery{
								Body: &sqlast.SQLSelect{
									Projection: []sqlast.SQLSelectItem{
										&sqlast.UnnamedExpression{
											Node: &sqlast.SQLWildcard{},
										},
									},
									Relation: &sqlast.Table{
										Name: sqlast.NewSQLObjectName("user_sub"),
									},
									Selection: &sqlast.SQLBinaryExpr{
										Op: sqlast.And,
										Left: &sqlast.SQLBinaryExpr{
											Op: sqlast.Eq,
											Left: &sqlast.SQLCompoundIdentifier{
												Idents: []*sqlast.SQLIdent{
													sqlast.NewSQLIdent("user"),
													sqlast.NewSQLIdent("id"),
												},
											},
											Right: &sqlast.SQLCompoundIdentifier{
												Idents: []*sqlast.SQLIdent{
													sqlast.NewSQLIdent("user_sub"),
													sqlast.NewSQLIdent("id"),
												},
											},
										},
										Right: &sqlast.SQLBinaryExpr{
											Op: sqlast.Eq,
											Left: &sqlast.SQLCompoundIdentifier{
												Idents: []*sqlast.SQLIdent{
													sqlast.NewSQLIdent("user_sub"),
													sqlast.NewSQLIdent("job"),
												},
											},
											Right: sqlast.NewSingleQuotedString("job"),
										},
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

				if diff := cmp.Diff(c.out, ast, ignoreMarker); diff != "" {
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
					Elements: []sqlast.TableElement{
						&sqlast.SQLColumnDef{
							Name:     sqlast.NewSQLIdent("person_id"),
							DateType: &sqlast.UUID{},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.UniqueColumnSpec{
										IsPrimaryKey: true,
									},
								},
								{
									Spec: &sqlast.NotNullColumnSpec{},
								},
							},
						},
						&sqlast.SQLColumnDef{
							Name: sqlast.NewSQLIdent("first_name"),
							DateType: &sqlast.VarcharType{
								Size: sqlast.NewSize(255),
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.UniqueColumnSpec{},
								},
							},
						},
						&sqlast.SQLColumnDef{
							Name: sqlast.NewSQLIdent("last_name"),
							DateType: &sqlast.VarcharType{
								Size: sqlast.NewSize(255),
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.NotNullColumnSpec{},
								},
							},
						},
						&sqlast.SQLColumnDef{
							Name:     sqlast.NewSQLIdent("created_at"),
							DateType: &sqlast.Timestamp{},
							Default:  sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("CURRENT_TIMESTAMP")),
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.NotNullColumnSpec{},
								},
							},
						},
					},
				},
			},
			{
				name: "with case",
				in: "CREATE TABLE persons (" +
					"person_id int PRIMARY KEY NOT NULL, " +
					"last_name character varying(255) NOT NULL, " +
					"test_id int NOT NULL REFERENCES test(id1, id2), " +
					"email character varying(255) UNIQUE NOT NULL, " +
					"age int NOT NULL CHECK(age > 0 AND age < 100), " +
					"created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL)",
				out: &sqlast.SQLCreateTable{
					Name: sqlast.NewSQLObjectName("persons"),
					Elements: []sqlast.TableElement{
						&sqlast.SQLColumnDef{
							Name:     sqlast.NewSQLIdent("person_id"),
							DateType: &sqlast.Int{},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.UniqueColumnSpec{
										IsPrimaryKey: true,
									},
								},
								{
									Spec: &sqlast.NotNullColumnSpec{},
								},
							},
						},
						&sqlast.SQLColumnDef{
							Name: sqlast.NewSQLIdent("last_name"),
							DateType: &sqlast.VarcharType{
								Size: sqlast.NewSize(255),
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.NotNullColumnSpec{},
								},
							},
						},
						&sqlast.SQLColumnDef{
							Name:     sqlast.NewSQLIdent("test_id"),
							DateType: &sqlast.Int{},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.NotNullColumnSpec{},
								},
								{
									Spec: &sqlast.ReferencesColumnSpec{
										TableName: sqlast.NewSQLObjectName("test"),
										Columns:   []*sqlast.SQLIdent{sqlast.NewSQLIdent("id1"), sqlast.NewSQLIdent("id2")},
									},
								},
							},
						},
						&sqlast.SQLColumnDef{
							Name: sqlast.NewSQLIdent("email"),
							DateType: &sqlast.VarcharType{
								Size: sqlast.NewSize(255),
							},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.UniqueColumnSpec{},
								},
								{
									Spec: &sqlast.NotNullColumnSpec{},
								},
							},
						},
						&sqlast.SQLColumnDef{
							Name:     sqlast.NewSQLIdent("age"),
							DateType: &sqlast.Int{},
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.NotNullColumnSpec{},
								},
								{
									Spec: &sqlast.CheckColumnSpec{
										Expr: &sqlast.SQLBinaryExpr{
											Op: sqlast.And,
											Left: &sqlast.SQLBinaryExpr{
												Op:    sqlast.Gt,
												Left:  sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("age")),
												Right: sqlast.NewLongValue(0),
											},
											Right: &sqlast.SQLBinaryExpr{
												Op:    sqlast.Lt,
												Left:  sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("age")),
												Right: sqlast.NewLongValue(100),
											},
										},
									},
								},
							},
						},
						&sqlast.SQLColumnDef{
							Name:     sqlast.NewSQLIdent("created_at"),
							DateType: &sqlast.Timestamp{},
							Default:  sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("CURRENT_TIMESTAMP")),
							Constraints: []*sqlast.ColumnConstraint{
								{
									Spec: &sqlast.NotNullColumnSpec{},
								},
							},
						},
					},
				},
			},
			{
				name: "with table constraint",
				in: "CREATE TABLE persons (" +
					"person_id int, " +
					"CONSTRAINT production UNIQUE(test_column), " +
					"PRIMARY KEY(person_id), " +
					"CHECK(id > 100), " +
					"FOREIGN KEY(test_id) REFERENCES other_table(col1, col2)" +
					")",
				out: &sqlast.SQLCreateTable{
					Name: sqlast.NewSQLObjectName("persons"),
					Elements: []sqlast.TableElement{
						&sqlast.SQLColumnDef{
							Name:     sqlast.NewSQLIdent("person_id"),
							DateType: &sqlast.Int{},
						},
						&sqlast.TableConstraint{
							Name: sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("production")),
							Spec: &sqlast.UniqueTableConstraint{
								Columns: []*sqlast.SQLIdent{sqlast.NewSQLIdent("test_column")},
							},
						},
						&sqlast.TableConstraint{
							Spec: &sqlast.UniqueTableConstraint{
								Columns:   []*sqlast.SQLIdent{sqlast.NewSQLIdent("person_id")},
								IsPrimary: true,
							},
						},
						&sqlast.TableConstraint{
							Spec: &sqlast.CheckTableConstraint{
								Expr: &sqlast.SQLBinaryExpr{
									Left:  sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("id")),
									Op:    sqlast.Gt,
									Right: sqlast.NewLongValue(100),
								},
							},
						},
						&sqlast.TableConstraint{
							Spec: &sqlast.ReferentialTableConstraint{
								Columns: []*sqlast.SQLIdent{sqlast.NewSQLIdent("test_id")},
								KeyExpr: &sqlast.ReferenceKeyExpr{
									TableName: sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("other_table")),
									Columns:   []*sqlast.SQLIdent{sqlast.NewSQLIdent("col1"), sqlast.NewSQLIdent("col2")},
								},
							},
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
					t.Fatalf("%+v", err)
				}

				if diff := cmp.Diff(c.out, ast, ignoreMarker); diff != "" {
					t.Errorf("diff %s", diff)
				}
			})
		}
	})

	t.Run("delete", func(t *testing.T) {
		cases := []struct {
			name string
			in   string
			out  sqlast.SQLStmt
			skip bool
		}{
			{
				in:   "DELETE FROM customers WHERE customer_id = 1",
				name: "simple case",
				out: &sqlast.SQLDelete{
					TableName: sqlast.NewSQLObjectName("customers"),
					Selection: &sqlast.SQLBinaryExpr{
						Op:    sqlast.Eq,
						Left:  sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("customer_id")),
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

				if diff := cmp.Diff(c.out, ast, ignoreMarker); diff != "" {
					t.Errorf("diff %s", diff)
				}
			})
		}
	})

	t.Run("insert", func(t *testing.T) {
		cases := []struct {
			name string
			in   string
			out  sqlast.SQLStmt
			skip bool
		}{
			{
				in:   "INSERT INTO customers (customer_name, contract_name) VALUES('Cardinal', 'Tom B. Erichsen')",
				name: "simple case",
				out: &sqlast.SQLInsert{
					TableName: sqlast.NewSQLObjectName("customers"),
					Columns: []*sqlast.SQLIdent{
						sqlast.NewSQLIdent("customer_name"),
						sqlast.NewSQLIdent("contract_name"),
					},
					Values: [][]sqlast.ASTNode{
						{
							sqlast.NewSingleQuotedString("Cardinal"),
							sqlast.NewSingleQuotedString("Tom B. Erichsen"),
						},
					},
				},
			},
			{
				name: "multi record case",
				in: "INSERT INTO customers (customer_name, contract_name) VALUES" +
					"('Cardinal', 'Tom B. Erichsen')," +
					"('Cardinal', 'Tom B. Erichsen')",
				out: &sqlast.SQLInsert{
					TableName: sqlast.NewSQLObjectName("customers"),
					Columns: []*sqlast.SQLIdent{
						sqlast.NewSQLIdent("customer_name"),
						sqlast.NewSQLIdent("contract_name"),
					},
					Values: [][]sqlast.ASTNode{
						{
							sqlast.NewSingleQuotedString("Cardinal"),
							sqlast.NewSingleQuotedString("Tom B. Erichsen"),
						},
						{
							sqlast.NewSingleQuotedString("Cardinal"),
							sqlast.NewSingleQuotedString("Tom B. Erichsen"),
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

				if diff := cmp.Diff(c.out, ast, ignoreMarker); diff != "" {
					t.Errorf("diff %s", diff)
				}
			})
		}
	})

	t.Run("alter", func(t *testing.T) {
		cases := []struct {
			name string
			in   string
			out  sqlast.SQLStmt
			skip bool
		}{
			{
				name: "add constraint unique",
				in:   "ALTER TABLE customers ADD CONSTRAINT unique_constraint unique (customer_first_name, customer_last_name)",
				out: &sqlast.SQLAlterTable{
					TableName: sqlast.NewSQLObjectName("customers"),
					Operation: &sqlast.AddConstraint{
						TableKey: &sqlast.UniqueKey{
							Key: &sqlast.Key{
								Name:    sqlast.NewSQLIdent("unique_constraint"),
								Columns: []*sqlast.SQLIdent{sqlast.NewSQLIdent("customer_first_name"), sqlast.NewSQLIdent("customer_last_name")},
							},
						},
					},
				},
			},
			{
				name: "add constraint foreign key",
				in:   "ALTER TABLE public.employee ADD CONSTRAINT dfk FOREIGN KEY (dno) REFERENCES public.department(dnumber)",
				out: &sqlast.SQLAlterTable{
					TableName: sqlast.NewSQLObjectName("public", "employee"),
					Operation: &sqlast.AddConstraint{
						TableKey: &sqlast.ForeignKey{
							Key: &sqlast.Key{
								Name:    sqlast.NewSQLIdent("dfk"),
								Columns: []*sqlast.SQLIdent{sqlast.NewSQLIdent("dno")},
							},
							ForeignTable:    sqlast.NewSQLObjectName("public", "department"),
							ReferredColumns: []*sqlast.SQLIdent{sqlast.NewSQLIdent("dnumber")},
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

				if diff := cmp.Diff(c.out, ast, ignoreMarker); diff != "" {
					t.Errorf("diff %s", diff)
				}
			})
		}
	})

	t.Run("update", func(t *testing.T) {
		cases := []struct {
			name string
			in   string
			out  sqlast.SQLStmt
			skip bool
		}{
			{
				name: "simple case",
				in:   "UPDATE customers SET contract_name = 'Alfred Schmidt', city = 'Frankfurt' WHERE customer_id = 1",
				out: &sqlast.SQLUpdate{
					TableName: sqlast.NewSQLObjectName("customers"),
					Assignments: []*sqlast.SQLAssignment{
						{
							ID:    sqlast.NewSQLIdent("contract_name"),
							Value: sqlast.NewSingleQuotedString("Alfred Schmidt"),
						},
						{
							ID:    sqlast.NewSQLIdent("city"),
							Value: sqlast.NewSingleQuotedString("Frankfurt"),
						},
					},
					Selection: &sqlast.SQLBinaryExpr{
						Op:    sqlast.Eq,
						Left:  sqlast.NewSQLIdentifier(sqlast.NewSQLIdent("customer_id")),
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

				if diff := cmp.Diff(c.out, ast, ignoreMarker); diff != "" {
					t.Errorf("diff %s", diff)
				}
			})
		}
	})
}
