package xsqlparser_test

// All queries are from https://www.w3schools.com/sql/sql_examples.asp

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/akito0107/xsqlparser"
	"github.com/akito0107/xsqlparser/dialect"
)

func TestParseQuery(t *testing.T) {

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
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fname := fmt.Sprintf("testdata/%s/", c.dir)
			files, err := ioutil.ReadDir(fname)
			if err != nil {
				t.Fatalf("%+v", err)
			}

			for _, f := range files {
				if !strings.HasSuffix(f.Name(), ".sql") {
					continue
				}
				t.Run(f.Name(), func(t *testing.T) {
					fi, err := os.Open(fname + f.Name())
					if err != nil {
						t.Fatalf("%+v", err)
					}
					parser, err := xsqlparser.NewParser(fi, &dialect.GenericSQLDialect{})
					if err != nil {
						t.Fatalf("%+v", err)
					}

					stmt, err := parser.ParseStatement()
					if err != nil {
						t.Fatalf("%+v", err)
					}
					recovered := stmt.ToSQLString()

					parser, err = xsqlparser.NewParser(bytes.NewBufferString(recovered), &dialect.GenericSQLDialect{})
					if err != nil {
						t.Fatalf("%+v", err)
					}

					stmt2, err := parser.ParseStatement()
					if err != nil {
						t.Fatalf("%+v", err)
					}

					if astdiff := cmp.Diff(stmt, stmt2, xsqlparser.IgnoreMarker); astdiff != "" {
						t.Logf(recovered)
						t.Errorf("should be same ast but diff:\n %s", astdiff)
					}
				})
			}
		})
	}
}
