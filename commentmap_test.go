package xsqlparser_test

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/akito0107/xsqlparser"
	"github.com/akito0107/xsqlparser/dialect"
	"github.com/akito0107/xsqlparser/sqlast"
	"github.com/akito0107/xsqlparser/sqltoken"
)

func parseFile(t *testing.T, src string) *sqlast.File {
	t.Helper()
	parser, err := xsqlparser.NewParser(strings.NewReader(src), &dialect.GenericSQLDialect{}, xsqlparser.ParseComment)
	if err != nil {
		t.Fatal(err)
	}

	f, err := parser.ParseFile()
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func compareComment(t *testing.T, expect, actual []*sqlast.CommentGroup) {
	t.Helper()
	if diff := cmp.Diff(expect, actual); diff != "" {
		t.Error(diff)
	}
}

func TestNewCommentMap(t *testing.T) {

	t.Run("associate with single statement", func(t *testing.T) {
		f := parseFile(t, `
--test
SELECT * from test;
`)

		m := sqlast.NewCommentMap(f)
		compareComment(t, m[f.Stmts[0]], []*sqlast.CommentGroup{
			{
				List: []*sqlast.Comment{
					{
						Text: "test",
						From: sqltoken.NewPos(2, 0),
						To:   sqltoken.NewPos(2, 6),
					},
				},
			},
		})
	})

	t.Run("associate with multi statements", func(t *testing.T) {

		f := parseFile(t, `
--select
SELECT * from test;

/*
insert
*/
INSERT INTO tbl_name (col1,col2) VALUES(15,col1*2);
`)
		m := sqlast.NewCommentMap(f)

		compareComment(t, m[f.Stmts[0]], []*sqlast.CommentGroup{
			{
				List: []*sqlast.Comment{
					{
						Text: "select",
						From: sqltoken.NewPos(2, 0),
						To:   sqltoken.NewPos(2, 8),
					},
				},
			},
		})

		compareComment(t, m[f.Stmts[1]], []*sqlast.CommentGroup{
			{
				List: []*sqlast.Comment{
					{
						Text: "\ninsert\n",
						From: sqltoken.NewPos(5, 0),
						To:   sqltoken.NewPos(7, 2),
					},
				},
			},
		})
	})

	t.Run("create table", func(t *testing.T) {

		f := parseFile(t, `
/*associate with stmts*/
CREATE TABLE test (
	/*associate with columndef*/
    col0 int primary key, --columndef
	/*with constraints*/
    col1 integer constraint test_constraint check (10 < col1 and col1 < 100),
    foreign key (col0, col1) references test2(col1, col2), --tableconstraints
	--table constrants
    CONSTRAINT test_constraint check(col1 > 10)
);
`)

		m := sqlast.NewCommentMap(f)
		ct := f.Stmts[0].(*sqlast.CreateTableStmt)
		compareComment(t, m[ct], []*sqlast.CommentGroup{
			{
				List: []*sqlast.Comment{
					{
						Text: "associate with stmts",
						From: sqltoken.NewPos(2, 0),
						To:   sqltoken.NewPos(2, 24),
					},
				},
			},
		})

		compareComment(t, m[ct.Elements[0]], []*sqlast.CommentGroup{
			{
				List: []*sqlast.Comment{
					{
						Text: "associate with columndef",
						From: sqltoken.NewPos(4, 4),
						To:   sqltoken.NewPos(4, 32),
					},
				},
			},
			{
				List: []*sqlast.Comment{
					{
						Text: "columndef",
						From: sqltoken.NewPos(5, 26),
						To:   sqltoken.NewPos(5, 37),
					},
				},
			},
		})

		compareComment(t, m[ct.Elements[1]], []*sqlast.CommentGroup{
			{
				List: []*sqlast.Comment{
					{
						Text: "associate with columndef",
						From: sqltoken.NewPos(4, 4),
						To:   sqltoken.NewPos(4, 32),
					},
				},
			},
		})
	})
}
