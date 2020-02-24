package sqlast

import (
	"io"

	"github.com/akito0107/xsqlparser/sqltoken"
)

type CommentGroup struct {
	List []*Comment
}

func (c *CommentGroup) ToSQLString() string {
	return toSQLString(c)
}

func (c *CommentGroup) WriteTo(w io.Writer) (n int64, err error) {
	sw := newSQLWriter(w)
	for i, comment := range c.List {
		sw.JoinNewLine(i, comment)
	}
	return sw.End()
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

func (c *Comment) WriteTo(w io.Writer) (int64, error) {
	return writeSingleString(w, c.Text)
}

func (c *Comment) Pos() sqltoken.Pos {
	return c.From
}

func (c *Comment) End() sqltoken.Pos {
	return c.To
}
