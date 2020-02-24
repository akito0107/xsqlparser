package sqlast

import (
	"io"

	"github.com/akito0107/xsqlparser/sqltoken"
)

//go:generate genmark -t TableOption -e Node

//ENGINE option ( = InnoDB, MyISAM ...)
type MyEngine struct {
	tableOption
	Engine sqltoken.Pos
	Equal  bool
	Name   *Ident
}

func (m *MyEngine) ToSQLString() string {
	return toSQLString(m)
}

func (m *MyEngine) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Bytes([]byte("ENGINE ")).If(m.Equal, []byte("= ")).Node(m.Name)
	return sw.End()
}

func (m *MyEngine) Pos() sqltoken.Pos {
	return m.Engine
}

func (m *MyEngine) End() sqltoken.Pos {
	return m.Name.To
}

type MyCharset struct {
	tableOption
	IsDefault bool
	Default   sqltoken.Pos
	Charset   sqltoken.Pos
	Equal     bool
	Name      *Ident
}

func (m *MyCharset) ToSQLString() string {
	return toSQLString(m)
}

func (m *MyCharset) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.If(m.IsDefault, []byte("DEFAULT ")).Bytes([]byte("CHARSET "))
	sw.If(m.Equal, []byte("= ")).Node(m.Name)
	return sw.End()
}

func (m *MyCharset) Pos() sqltoken.Pos {
	if m.IsDefault {
		return m.Default
	}
	return m.Charset
}

func (m *MyCharset) End() sqltoken.Pos {
	return m.Name.To
}
