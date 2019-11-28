package sqlast

import (
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
	str := "ENGINE "
	if m.Equal {
		str += "= "
	}
	str += m.Name.ToSQLString()
	return str
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
	var s string

	if m.IsDefault {
		s = "DEFAULT "
	}
	s += "CHARSET "

	if m.Equal {
		s +=  "= "
	}
	s += m.Name.ToSQLString()

	return s
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
