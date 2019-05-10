# xsqlparser

[![Build Status](https://dev.azure.com/akito01070362/xsqlparser/_apis/build/status/akito0107.xsqlparser?branchName=master)](https://dev.azure.com/akito01070362/xsqlparser/_build/latest?definitionId=2&branchName=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/akito0107/xsqlparser)](https://goreportcard.com/report/github.com/akito0107/xsqlparesr)
[![codecov](https://codecov.io/gh/akito0107/xsqlparser/branch/master/graph/badge.svg)](https://codecov.io/gh/akito0107/xsqlparser)

__[WORK IN PROGRESS] currently only supports very limited queries. DO NOT USE IN PRODUCTION.__

sql parser for golang.

This repo is ported of [sqlparser-rs](https://github.com/andygrove/sqlparser-rs) in Go.


## Getting Started

### Prerequisites
- Go 1.12+

### Installing
```
$ go get -u github.com/akito0107/xsqlparser/...
```

### How to use
__Currently supports `SELECT`,`CREATE TABLE`,`CREATE VIEW`,`INSERT`,`UPDATE`,`DELETE`.__

- simple case
```go
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
&sqlast.SQLQuery{
  CTEs: []*sqlast.CTE{},
  Body: &sqlast.SQLSelect{
    Distinct:   false,
    Projection: []sqlast.SQLSelectItem{
      &sqlast.UnnamedExpression{
        Node: &sqlast.SQLWildcard{},
      },
    },
    Relation: &sqlast.Table{
      Name: &sqlast.SQLObjectName{
        Idents: []*sqlast.SQLIdent{
          &"test_table",
        },
      },
      Alias:     (*sqlast.SQLIdent)(nil),
      Args:      []sqlast.ASTNode{},
      WithHints: []sqlast.ASTNode{},
    },
    Joins:     []*sqlast.Join{},
    Selection: nil,
    GroupBy:   []sqlast.ASTNode{},
    Having:    nil,
  },
  OrderBy: []*sqlast.SQLOrderByExpr{},
  Limit:   nil,
}
```

You can also create `sql` from ast via `Eval()`.
```go
log.Println(stmt.Eval())
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
{
  CTEs: []*sqlast.CTE{},
  Body: &sqlast.SQLSelect{
    Distinct:   false,
    Projection: []sqlast.SQLSelectItem{
      &sqlast.UnnamedExpression{
        Node: &sqlast.SQLCompoundIdentifier{
          Idents: []*sqlast.SQLIdent{
            &"orders",
            &"product",
          },
        },
      },
      &sqlast.ExpressionWithAlias{
        Expr: &sqlast.SQLFunction{
          Name: &sqlast.SQLObjectName{
            Idents: []*sqlast.SQLIdent{
              &"SUM",
            },
          },
          Args: []sqlast.ASTNode{
            &sqlast.SQLCompoundIdentifier{
              Idents: []*sqlast.SQLIdent{
                &"orders",
                &"quantity",
              },
            },
          },
          Over: (*sqlast.SQLWindowSpec)(nil),
        },
        Alias: &"product_units",
      },
      &sqlast.QualifiedWildcard{
        Prefix: &sqlast.SQLObjectName{
          Idents: []*sqlast.SQLIdent{
            &"accounts",
          },
        },
      },
    },
    Relation: &sqlast.Table{
      Name: &sqlast.SQLObjectName{
        Idents: []*sqlast.SQLIdent{
          &"orders",
        },
      },
      Alias:     (*sqlast.SQLIdent)(nil),
      Args:      []sqlast.ASTNode{},
      WithHints: []sqlast.ASTNode{},
    },
    Joins: []*sqlast.Join{
      &sqlast.Join{
        Relation: &sqlast.Table{
          Name: &sqlast.SQLObjectName{
            Idents: []*sqlast.SQLIdent{
              &"accounts",
            },
          },
          Alias:     (*sqlast.SQLIdent)(nil),
          Args:      []sqlast.ASTNode{},
          WithHints: []sqlast.ASTNode{},
        },
        Op:       1,
        Constant: &sqlast.OnJoinConstant{
          Node: &sqlast.SQLBinaryExpr{
            Left: &sqlast.SQLCompoundIdentifier{
              Idents: []*sqlast.SQLIdent{
                &"orders",
                &"account_id",
              },
            },
            Op:    9,
            Right: &sqlast.SQLCompoundIdentifier{
              Idents: []*sqlast.SQLIdent{
                &"accounts",
                &"id",
              },
            },
          },
        },
      },
    },
    Selection: &sqlast.SQLInSubQuery{
      Expr: &sqlast.SQLCompoundIdentifier{
        Idents: []*sqlast.SQLIdent{
          &"orders",
          &"region",
        },
      },
      SubQuery: &sqlast.SQLQuery{
        CTEs: []*sqlast.CTE{},
        Body: &sqlast.SQLSelect{
          Distinct:   false,
          Projection: []sqlast.SQLSelectItem{
            &sqlast.UnnamedExpression{
              Node: &sqlast.SQLIdentifier{
                Ident: &"region",
              },
            },
          },
          Relation: &sqlast.Table{
            Name: &sqlast.SQLObjectName{
              Idents: []*sqlast.SQLIdent{
                &"top_regions",
              },
            },
            Alias:     (*sqlast.SQLIdent)(nil),
            Args:      []sqlast.ASTNode{},
            WithHints: []sqlast.ASTNode{},
          },
          Joins:     []*sqlast.Join{},
          Selection: nil,
          GroupBy:   []sqlast.ASTNode{},
          Having:    nil,
        },
        OrderBy: []*sqlast.SQLOrderByExpr{},
        Limit:   nil,
      },
      Negated: false,
    },
    GroupBy: []sqlast.ASTNode{},
    Having:  nil,
  },
  OrderBy: []*sqlast.SQLOrderByExpr{
    &sqlast.SQLOrderByExpr{
      Expr: &sqlast.SQLIdentifier{
        Ident: &"product_units",
      },
      ASC: (*bool)(nil),
    },
  },
  Limit: &100,
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
&sqlast.SQLQuery{
  CTEs: []*sqlast.CTE{
    &sqlast.CTE{
      Alias: &"regional_sales",
      Query: &sqlast.SQLQuery{
        CTEs: []*sqlast.CTE{},
        Body: &sqlast.SQLSelect{
          Distinct:   false,
          Projection: []sqlast.SQLSelectItem{
            &sqlast.UnnamedExpression{
              Node: &sqlast.SQLIdentifier{
                Ident: &"region",
              },
            },
            &sqlast.ExpressionWithAlias{
              Expr: &sqlast.SQLFunction{
                Name: &sqlast.SQLObjectName{
                  Idents: []*sqlast.SQLIdent{
                    &"SUM",
                  },
                },
                Args: []sqlast.ASTNode{
                  &sqlast.SQLIdentifier{
                    Ident: &"amount",
                  },
                },
                Over: (*sqlast.SQLWindowSpec)(nil),
              },
              Alias: &"total_sales",
            },
          },
          Relation: &sqlast.Table{
            Name: &sqlast.SQLObjectName{
              Idents: []*sqlast.SQLIdent{
                &"orders",
              },
            },
            Alias:     (*sqlast.SQLIdent)(nil),
            Args:      []sqlast.ASTNode{},
            WithHints: []sqlast.ASTNode{},
          },
          Joins:     []*sqlast.Join{},
          Selection: nil,
          GroupBy:   []sqlast.ASTNode{
            &sqlast.SQLIdentifier{
              Ident: &"region",
            },
          },
          Having: nil,
        },
        OrderBy: []*sqlast.SQLOrderByExpr{},
        Limit:   nil,
      },
    },
  },
  Body: &sqlast.SQLSelect{
    Distinct:   false,
    Projection: []sqlast.SQLSelectItem{
      &sqlast.UnnamedExpression{
        Node: &sqlast.SQLIdentifier{
          Ident: &"product",
        },
      },
      &sqlast.ExpressionWithAlias{
        Expr: &sqlast.SQLFunction{
          Name: &sqlast.SQLObjectName{
            Idents: []*sqlast.SQLIdent{
              &"SUM",
            },
          },
          Args: []sqlast.ASTNode{
            &sqlast.SQLIdentifier{
              Ident: &"quantity",
            },
          },
          Over: (*sqlast.SQLWindowSpec)(nil),
        },
        Alias: &"product_units",
      },
    },
    Relation: &sqlast.Table{
      Name: &sqlast.SQLObjectName{
        Idents: []*sqlast.SQLIdent{
          &"orders",
        },
      },
      Alias:     (*sqlast.SQLIdent)(nil),
      Args:      []sqlast.ASTNode{},
      WithHints: []sqlast.ASTNode{},
    },
    Joins:     []*sqlast.Join{},
    Selection: &sqlast.SQLInSubQuery{
      Expr: &sqlast.SQLIdentifier{
        Ident: &"region",
      },
      SubQuery: &sqlast.SQLQuery{
        CTEs: []*sqlast.CTE{},
        Body: &sqlast.SQLSelect{
          Distinct:   false,
          Projection: []sqlast.SQLSelectItem{
            &sqlast.UnnamedExpression{
              Node: &sqlast.SQLIdentifier{
                Ident: &"region",
              },
            },
          },
          Relation: &sqlast.Table{
            Name: &sqlast.SQLObjectName{
              Idents: []*sqlast.SQLIdent{
                &"top_regions",
              },
            },
            Alias:     (*sqlast.SQLIdent)(nil),
            Args:      []sqlast.ASTNode{},
            WithHints: []sqlast.ASTNode{},
          },
          Joins:     []*sqlast.Join{},
          Selection: nil,
          GroupBy:   []sqlast.ASTNode{},
          Having:    nil,
        },
        OrderBy: []*sqlast.SQLOrderByExpr{},
        Limit:   nil,
      },
      Negated: false,
    },
    GroupBy: []sqlast.ASTNode{
      &sqlast.SQLIdentifier{
        Ident: &"region",
      },
      &sqlast.SQLIdentifier{
        Ident: &"product",
      },
    },
    Having: nil,
  },
  OrderBy: []*sqlast.SQLOrderByExpr{},
  Limit:   nil,
}
```


## License
This project is licensed under the Apache License 2.0 License - see the [LICENSE](LICENSE) file for details