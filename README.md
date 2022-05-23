# xsqlparser

[![GoDoc](https://godoc.org/github.com/akito0107/xsqlparser?status.svg)](https://godoc.org/github.com/akito0107/xsqlparser)
[![Actions Status](https://github.com/akito0107/xsqlparser/workflows/Go/badge.svg)](https://github.com/akito0107/xsqlparser/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/akito0107/xsqlparser)](https://goreportcard.com/report/github.com/akito0107/xsqlparser)
[![codecov](https://codecov.io/gh/akito0107/xsqlparser/branch/master/graph/badge.svg)](https://codecov.io/gh/akito0107/xsqlparser)

sql parser for golang.

This repo is ported of [sqlparser-rs](https://github.com/andygrove/sqlparser-rs) in Go.


## Getting Started

### Prerequisites
- Go 1.16+

### Installing
```
$ go get -u github.com/akito0107/xsqlparser/...
```

### How to use

#### Parser

__Currently supports `SELECT`,`CREATE TABLE`, `DROP TABLE`, `CREATE VIEW`,`INSERT`,`UPDATE`,`DELETE`, `ALTER TABLE`, `CREATE INDEX`, `DROP INDEX`, `EXPLAIN`.__

- simple case
```go
package main 

import (
	"bytes"
	"log"

	"github.com/k0kubun/pp"

	"github.com/akito0107/xsqlparser"
	"github.com/akito0107/xsqlparser/dialect"
)

... 
str := "SELECT * from test_table"
parser, err := xsqlparser.NewParser(bytes.NewBufferString(str), &dialect.GenericSQLDialect{})
if err != nil {
	log.Fatal(err)
}

stmt, err := parser.ParseStatement()
if err != nil {
	log.Fatal(err)
}
pp.Println(stmt)
```

got:
```
&sqlast.Query{
  stmt: sqlast.stmt{},
  CTEs: []*sqlast.CTE{},
  Body: &sqlast.SQLSelect{
    sqlSetExpr: sqlast.sqlSetExpr{},
    Distinct:   false,
    Projection: []sqlast.SQLSelectItem{
      &sqlast.UnnamedSelectItem{
        sqlSelectItem: sqlast.sqlSelectItem{},
        Node:          &sqlast.Wildcard{},
      },
    },
    FromClause: []sqlast.TableReference{
      &sqlast.Table{
        tableFactor:    sqlast.tableFactor{},
        tableReference: sqlast.tableReference{},
        Name:           &sqlast.ObjectName{
          Idents: []*sqlast.Ident{
            &"test_table",
          },
        },
        Alias:     (*sqlast.Ident)(nil),
        Args:      []sqlast.Node{},
        WithHints: []sqlast.Node{},
      },
    },
    WhereClause:   nil,
    GroupByClause: []sqlast.Node{},
    HavingClause:  nil,
  },
  OrderBy: []*sqlast.OrderByExpr{},
  Limit:   (*sqlast.LimitExpr)(nil),
}
```

You can also create `sql` from ast via `ToSQLString()`.
```go
log.Println(stmt.ToSQLString())
```

got:
```
2019/05/07 11:59:36 SELECT * FROM test_table
```

- complicated select
```go
str := "SELECT orders.product, SUM(orders.quantity) AS product_units, accounts.* " +
	"FROM orders LEFT JOIN accounts ON orders.account_id = accounts.id " +
	"WHERE orders.region IN (SELECT region FROM top_regions) " +
	"ORDER BY product_units LIMIT 100"

parser, err := xsqlparser.NewParser(bytes.NewBufferString(str), &dialect.GenericSQLDialect{})
if err != nil {
	log.Fatal(err)
}

stmt, err := parser.ParseStatement()
if err != nil {
	log.Fatal(err)
}
pp.Println(stmt)
```

got:
```
&sqlast.Query{
  stmt: sqlast.stmt{},
  CTEs: []*sqlast.CTE{},
  Body: &sqlast.SQLSelect{
    sqlSetExpr: sqlast.sqlSetExpr{},
    Distinct:   false,
    Projection: []sqlast.SQLSelectItem{
      &sqlast.UnnamedSelectItem{
        sqlSelectItem: sqlast.sqlSelectItem{},
        Node:          &sqlast.CompoundIdent{
          Idents: []*sqlast.Ident{
            &"orders",
            &"product",
          },
        },
      },
      &sqlast.AliasSelectItem{
        sqlSelectItem: sqlast.sqlSelectItem{},
        Expr:          &sqlast.Function{
          Name: &sqlast.ObjectName{
            Idents: []*sqlast.Ident{
              &"SUM",
            },
          },
          Args: []sqlast.Node{
            &sqlast.CompoundIdent{
              Idents: []*sqlast.Ident{
                &"orders",
                &"quantity",
              },
            },
          },
          Over: (*sqlast.WindowSpec)(nil),
        },
        Alias: &"product_units",
      },
      &sqlast.QualifiedWildcardSelectItem{
        sqlSelectItem: sqlast.sqlSelectItem{},
        Prefix:        &sqlast.ObjectName{
          Idents: []*sqlast.Ident{
            &"accounts",
          },
        },
      },
    },
    FromClause: []sqlast.TableReference{
      &sqlast.QualifiedJoin{
        tableReference: sqlast.tableReference{},
        LeftElement:    &sqlast.TableJoinElement{
          joinElement: sqlast.joinElement{},
          Ref:         &sqlast.Table{
            tableFactor:    sqlast.tableFactor{},
            tableReference: sqlast.tableReference{},
            Name:           &sqlast.ObjectName{
              Idents: []*sqlast.Ident{
                &"orders",
              },
            },
            Alias:     (*sqlast.Ident)(nil),
            Args:      []sqlast.Node{},
            WithHints: []sqlast.Node{},
          },
        },
        Type:         1,
        RightElement: &sqlast.TableJoinElement{
          joinElement: sqlast.joinElement{},
          Ref:         &sqlast.Table{
            tableFactor:    sqlast.tableFactor{},
            tableReference: sqlast.tableReference{},
            Name:           &sqlast.ObjectName{
              Idents: []*sqlast.Ident{
                &"accounts",
              },
            },
            Alias:     (*sqlast.Ident)(nil),
            Args:      []sqlast.Node{},
            WithHints: []sqlast.Node{},
          },
        },
        Spec: &sqlast.JoinCondition{
          joinSpec:        sqlast.joinSpec{},
          SearchCondition: &sqlast.BinaryExpr{
            Left: &sqlast.CompoundIdent{
              Idents: []*sqlast.Ident{
                &"orders",
                &"account_id",
              },
            },
            Op:    9,
            Right: &sqlast.CompoundIdent{
              Idents: []*sqlast.Ident{
                &"accounts",
                &"id",
              },
            },
          },
        },
      },
    },
    WhereClause: &sqlast.InSubQuery{
      Expr: &sqlast.CompoundIdent{
        Idents: []*sqlast.Ident{
          &"orders",
          &"region",
        },
      },
      SubQuery: &sqlast.Query{
        stmt: sqlast.stmt{},
        CTEs: []*sqlast.CTE{},
        Body: &sqlast.SQLSelect{
          sqlSetExpr: sqlast.sqlSetExpr{},
          Distinct:   false,
          Projection: []sqlast.SQLSelectItem{
            &sqlast.UnnamedSelectItem{
              sqlSelectItem: sqlast.sqlSelectItem{},
              Node:          &"region",
            },
          },
          FromClause: []sqlast.TableReference{
            &sqlast.Table{
              tableFactor:    sqlast.tableFactor{},
              tableReference: sqlast.tableReference{},
              Name:           &sqlast.ObjectName{
                Idents: []*sqlast.Ident{
                  &"top_regions",
                },
              },
              Alias:     (*sqlast.Ident)(nil),
              Args:      []sqlast.Node{},
              WithHints: []sqlast.Node{},
            },
          },
          WhereClause:   nil,
          GroupByClause: []sqlast.Node{},
          HavingClause:  nil,
        },
        OrderBy: []*sqlast.OrderByExpr{},
        Limit:   (*sqlast.LimitExpr)(nil),
      },
      Negated: false,
    },
    GroupByClause: []sqlast.Node{},
    HavingClause:  nil,
  },
  OrderBy: []*sqlast.OrderByExpr{
    &sqlast.OrderByExpr{
      Expr: &"product_units",
      ASC:  (*bool)(nil),
    },
  },
  Limit: &sqlast.LimitExpr{
    All:         false,
    LimitValue:  &100,
    OffsetValue: (*sqlast.LongValue)(nil),
  },
}
```

- with CTE
```go
str := "WITH regional_sales AS (" +
	"SELECT region, SUM(amount) AS total_sales " +
	"FROM orders GROUP BY region) " +
	"SELECT product, SUM(quantity) AS product_units " +
	"FROM orders " +
	"WHERE region IN (SELECT region FROM top_regions) " +
	"GROUP BY region, product"

parser, err := xsqlparser.NewParser(bytes.NewBufferString(str), &dialect.GenericSQLDialect{})
if err != nil {
	log.Fatal(err)
}

stmt, err := parser.ParseStatement()
if err != nil {
	log.Fatal(err)
}
pp.Println(stmt)
```

got:
```
&sqlast.Query{
  stmt: sqlast.stmt{},
  CTEs: []*sqlast.CTE{
    &sqlast.CTE{
      Alias: &"regional_sales",
      Query: &sqlast.Query{
        stmt: sqlast.stmt{},
        CTEs: []*sqlast.CTE{},
        Body: &sqlast.SQLSelect{
          sqlSetExpr: sqlast.sqlSetExpr{},
          Distinct:   false,
          Projection: []sqlast.SQLSelectItem{
            &sqlast.UnnamedSelectItem{
              sqlSelectItem: sqlast.sqlSelectItem{},
              Node:          &"region",
            },
            &sqlast.AliasSelectItem{
              sqlSelectItem: sqlast.sqlSelectItem{},
              Expr:          &sqlast.Function{
                Name: &sqlast.ObjectName{
                  Idents: []*sqlast.Ident{
                    &"SUM",
                  },
                },
                Args: []sqlast.Node{
                  &"amount",
                },
                Over: (*sqlast.WindowSpec)(nil),
              },
              Alias: &"total_sales",
            },
          },
          FromClause: []sqlast.TableReference{
            &sqlast.Table{
              tableFactor:    sqlast.tableFactor{},
              tableReference: sqlast.tableReference{},
              Name:           &sqlast.ObjectName{
                Idents: []*sqlast.Ident{
                  &"orders",
                },
              },
              Alias:     (*sqlast.Ident)(nil),
              Args:      []sqlast.Node{},
              WithHints: []sqlast.Node{},
            },
          },
          WhereClause:   nil,
          GroupByClause: []sqlast.Node{
            &"region",
          },
          HavingClause: nil,
        },
        OrderBy: []*sqlast.OrderByExpr{},
        Limit:   (*sqlast.LimitExpr)(nil),
      },
    },
  },
  Body: &sqlast.SQLSelect{
    sqlSetExpr: sqlast.sqlSetExpr{},
    Distinct:   false,
    Projection: []sqlast.SQLSelectItem{
      &sqlast.UnnamedSelectItem{
        sqlSelectItem: sqlast.sqlSelectItem{},
        Node:          &"product",
      },
      &sqlast.AliasSelectItem{
        sqlSelectItem: sqlast.sqlSelectItem{},
        Expr:          &sqlast.Function{
          Name: &sqlast.ObjectName{
            Idents: []*sqlast.Ident{
              &"SUM",
            },
          },
          Args: []sqlast.Node{
            &"quantity",
          },
          Over: (*sqlast.WindowSpec)(nil),
        },
        Alias: &"product_units",
      },
    },
    FromClause: []sqlast.TableReference{
      &sqlast.Table{
        tableFactor:    sqlast.tableFactor{},
        tableReference: sqlast.tableReference{},
        Name:           &sqlast.ObjectName{
          Idents: []*sqlast.Ident{
            &"orders",
          },
        },
        Alias:     (*sqlast.Ident)(nil),
        Args:      []sqlast.Node{},
        WithHints: []sqlast.Node{},
      },
    },
    WhereClause: &sqlast.InSubQuery{
      Expr:     &"region",
      SubQuery: &sqlast.Query{
        stmt: sqlast.stmt{},
        CTEs: []*sqlast.CTE{},
        Body: &sqlast.SQLSelect{
          sqlSetExpr: sqlast.sqlSetExpr{},
          Distinct:   false,
          Projection: []sqlast.SQLSelectItem{
            &sqlast.UnnamedSelectItem{
              sqlSelectItem: sqlast.sqlSelectItem{},
              Node:          &"region",
            },
          },
          FromClause: []sqlast.TableReference{
            &sqlast.Table{
              tableFactor:    sqlast.tableFactor{},
              tableReference: sqlast.tableReference{},
              Name:           &sqlast.ObjectName{
                Idents: []*sqlast.Ident{
                  &"top_regions",
                },
              },
              Alias:     (*sqlast.Ident)(nil),
              Args:      []sqlast.Node{},
              WithHints: []sqlast.Node{},
            },
          },
          WhereClause:   nil,
          GroupByClause: []sqlast.Node{},
          HavingClause:  nil,
        },
        OrderBy: []*sqlast.OrderByExpr{},
        Limit:   (*sqlast.LimitExpr)(nil),
      },
      Negated: false,
    },
    GroupByClause: []sqlast.Node{
      &"region",
      &"product",
    },
    HavingClause: nil,
  },
  OrderBy: []*sqlast.OrderByExpr{},
  Limit:   (*sqlast.LimitExpr)(nil),
}
```

#### Visitor(s)

- Using `Inspect`

create AST List
```go
package main


import (
	"bytes"
	"log"

	"github.com/k0kubun/pp"

	"github.com/akito0107/xsqlparser"
	"github.com/akito0107/xsqlparser/sqlast"
	"github.com/akito0107/xsqlparser/dialect"
)

func main() {
	src := `WITH regional_sales AS (
		SELECT region, SUM(amount) AS total_sales
		FROM orders GROUP BY region)
		SELECT product, SUM(quantity) AS product_units
		FROM orders
		WHERE region IN (SELECT region FROM top_regions)
		GROUP BY region, product;`

	parser, err := xsqlparser.NewParser(bytes.NewBufferString(src), &dialect.GenericSQLDialect{})
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := parser.ParseStatement()
	if err != nil {
		log.Fatal(err)
	}
	var list []sqlast.Node

	sqlast.Inspect(stmt, func(node sqlast.Node) bool {
		switch node.(type) {
		case nil:
			return false
		default:
			list = append(list, node)
			return true
		}
	})
    pp.Println(list)
}
```

also available `Walk()`.

#### CommentMap

__Experimental Feature__

```go
package main

import (
	"bytes"
	"log"

	"github.com/k0kubun/pp"

	"github.com/akito0107/xsqlparser"
	"github.com/akito0107/xsqlparser/sqlast"
	"github.com/akito0107/xsqlparser/dialect"
)

func main() {
	src := `
/*associate with stmts1*/
CREATE TABLE test (
	/*associate with columndef*/
    col0 int primary key, --columndef
	/*with constraints*/
    col1 integer constraint test_constraint check (10 < col1 and col1 < 100),
    foreign key (col0, col1) references test2(col1, col2), --table constraints1
	--table constraints2
    CONSTRAINT test_constraint check(col1 > 10)
); --associate with stmts2
`

	parser, err := xsqlparser.NewParser(bytes.NewBufferString(src), &dialect.GenericSQLDialect{}, xsqlparser.ParseComment)
	if err != nil {
		log.Fatal(err)
	}

	file, err := parser.ParseFile()
	if err != nil {
		log.Fatal(err)
	}

	m := sqlast.NewCommentMap(file)

	createTable := file.Stmts[0].(*sqlast.CreateTableStmt)

	pp.Println(m[createTable.Elements[0]]) // you can show `associate with columndef` and `columndef` comments
}

```

## License
This project is licensed under the Apache License 2.0 License - see the [LICENSE](LICENSE) file for details
