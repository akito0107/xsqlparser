package main

import (
	"bytes"
	"log"

	"github.com/k0kubun/pp"

	"github.com/akito0107/xsqlparser"
	"github.com/akito0107/xsqlparser/dialect"
)

func main() {
	simpleSelect()
	complicatedSelect()
	withCTE()
}

func simpleSelect() {
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

	log.Println(stmt.Eval())
}

func complicatedSelect() {
	str := "SELECT product, SUM(quantity) AS product_units, account.* " +
		"FROM orders LEFT JOIN " +
		"WHERE region IN (SELECT region FROM top_regions) " +
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

	log.Println(stmt.Eval())
}

func withCTE() {
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

	log.Println(stmt.Eval())

}
