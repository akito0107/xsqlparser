package xsqlparser_test

// All queries are from https://www.w3schools.com/sql/sql_examples.asp

import (
	"bytes"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/akito0107/xsqlparser"
	"github.com/akito0107/xsqlparser/dialect"
)

func TestParseQuery(t *testing.T) {

	t.Run("SELECT", func(t *testing.T) {
		files, err := ioutil.ReadDir("testdata/select")
		if err != nil {
			t.Fatalf("%+v", err)
		}

		for _, f := range files {
			if !strings.HasSuffix(f.Name(), ".sql") {
				continue
			}
			t.Run(f.Name(), func(t *testing.T) {
				fi, err := os.Open("testdata/select/" + f.Name())
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

	t.Run("CREATE TABLE", func(t *testing.T) {
		files, err := ioutil.ReadDir("testdata/create_table")
		if err != nil {
			t.Fatalf("%+v", err)
		}

		for _, f := range files {
			if !strings.HasSuffix(f.Name(), ".sql") {
				continue
			}
			t.Run(f.Name(), func(t *testing.T) {
				fi, err := os.Open("testdata/create_table/" + f.Name())
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

	t.Run("ALTER TABLE", func(t *testing.T) {
		files, err := ioutil.ReadDir("testdata/alter")
		if err != nil {
			t.Fatalf("%+v", err)
		}

		for _, f := range files {
			if !strings.HasSuffix(f.Name(), ".sql") {
				continue
			}
			t.Run(f.Name(), func(t *testing.T) {
				fi, err := os.Open("testdata/alter/" + f.Name())
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
