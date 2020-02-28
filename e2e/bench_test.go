package e2e_test

// All queries are from https://www.w3schools.com/sql/sql_examples.asp

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/akito0107/xsqlparser"
	"github.com/akito0107/xsqlparser/dialect"
)

func BenchmarkParseQuery(b *testing.B) {

	cases := []struct {
		name string
		dir  string
	}{
		{
			name: "SELECT",
			dir:  "select",
		},
		{
			name: "CREATE TABLE",
			dir:  "create_table",
		},
		{
			name: "ALTER TABLE",
			dir:  "alter",
		},
		{
			name: "DROP TABLE",
			dir:  "drop_table",
		},
		{
			name: "CREATE INDEX",
			dir:  "create_index",
		},
		{
			name: "DROP INDEX",
			dir:  "drop_index",
		},
		{
			name: "INSERT",
			dir:  "insert",
		},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			fname := fmt.Sprintf("testdata/%s/", c.dir)
			files, err := ioutil.ReadDir(fname)
			if err != nil {
				b.Fatalf("%+v", err)
			}

			for _, f := range files {
				if !strings.HasSuffix(f.Name(), ".sql") {
					continue
				}
				b.Run(f.Name(), func(b *testing.B) {
					fi, err := os.Open(fname + f.Name())
					if err != nil {
						b.Fatalf("%+v", err)
					}
					b.ResetTimer()

					for i := 0; i < b.N; i++ {
						fi.Seek(0, 0)
						parser, err := xsqlparser.NewParser(fi, &dialect.GenericSQLDialect{})
						if err != nil {
							b.Fatalf("%+v", err)
						}

						if _, err := parser.ParseStatement(); err != nil {
							b.Fatalf("%+v", err)
						}
					}
					fi.Close()
				})
			}
		})
	}
}
