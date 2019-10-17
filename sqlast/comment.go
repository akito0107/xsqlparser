package sqlast

import (
	"strings"

	"github.com/akito0107/xsqlparser/sqltoken"
)

type CommentGroup struct {
	List []*Comment
}

func (c *CommentGroup) ToSQLString() string {
	var comments []string

	for _, l := range c.List {
		comments = append(comments, l.Text)
	}

	return strings.Join(comments, "\n")
}

func (c *CommentGroup) Pos() sqltoken.Pos {
	return c.List[0].Pos()
}

func (c *CommentGroup) End() sqltoken.Pos {
	return c.List[len(c.List)-1].End()
}

type Comment struct {
	Text     string
	From, To sqltoken.Pos
}

func (c *Comment) ToSQLString() string {
	return c.Text
}

func (c *Comment) Pos() sqltoken.Pos {
	return c.From
}

func (c *Comment) End() sqltoken.Pos {
	return c.To
}
