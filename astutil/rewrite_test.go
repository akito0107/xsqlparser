package astutil

import (
	"bytes"
	"testing"

	"github.com/akito0107/xsqlparser"
	"github.com/akito0107/xsqlparser/dialect"
	"github.com/akito0107/xsqlparser/sqlast"
)

func TestApply(t *testing.T) {

	cases := []struct {
		name     string
		src      string
		expect   string
		preFunc  ApplyFunc
		postFunc ApplyFunc
	}{
		{
			name:   "replace long value",
			src:    `SELECT * FROM table_a WHERE id = 1`,
			expect: `SELECT * FROM table_a WHERE id = 2`,
			preFunc: func(cursor *Cursor) bool {
				switch cursor.node.(type) {
				case *sqlast.LongValue:
					cursor.Replace(sqlast.NewLongValue(2))
				}
				return true
			},
		},
		{
			name:   "remove select item",
			src:    "SELECT a, b, c FROM table_a",
			expect: "SELECT a, b FROM table_a",
			preFunc: func(cursor *Cursor) bool {
				switch cursor.node.(type) {
				case *sqlast.UnnamedSelectItem:
					if cursor.Index() == 2 {
						cursor.Delete()
					}
				}
				return true
			},
		},
		{
			name:   "insert after",
			src:    "SELECT a, b FROM table_a",
			expect: "SELECT a, b, c FROM table_a",
			preFunc: func(cursor *Cursor) bool {
				switch cursor.node.(type) {
				case *sqlast.UnnamedSelectItem:
					if cursor.Index() == 1 {
						cursor.InsertAfter(&sqlast.UnnamedSelectItem{
							Node: sqlast.NewIdent("c"),
						})
					}
				}
				return true
			},
		},
		{
			name:   "insert before",
			src:    "SELECT a, b FROM table_a",
			expect: "SELECT c, a, b FROM table_a",
			preFunc: func(cursor *Cursor) bool {
				switch cursor.node.(type) {
				case *sqlast.UnnamedSelectItem:
					if cursor.Index() == 0 {
						cursor.InsertBefore(&sqlast.UnnamedSelectItem{
							Node: sqlast.NewIdent("c"),
						})
					}
				}
				return true
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			parser, err := xsqlparser.NewParser(bytes.NewBufferString(c.src), &dialect.GenericSQLDialect{})
			if err != nil {
				t.Fatalf("%+v", err)
			}
			ast, err := parser.ParseStatement()
			if err != nil {
				t.Fatalf("%+v", err)
			}

			res := Apply(ast, c.preFunc, c.postFunc)
			if c.expect != res.ToSQLString() {
				t.Errorf("should be \n %s but \n %s", c.expect, res.ToSQLString())
			}
		})
	}
}
