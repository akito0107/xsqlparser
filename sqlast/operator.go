package sqlast

import (
	"io"

	"github.com/akito0107/xsqlparser/sqltoken"
)

type Operator struct {
	Type     OperatorType
	From, To sqltoken.Pos
}

func (o *Operator) Pos() sqltoken.Pos {
	return o.From
}

func (o *Operator) End() sqltoken.Pos {
	return o.To
}

type OperatorType int

const (
	Plus OperatorType = iota
	Minus
	Multiply
	Divide
	Modulus
	Gt
	Lt
	GtEq
	LtEq
	Eq
	NotEq
	And
	Or
	Not
	Like
	NotLike
	None
)

func (o *Operator) ToSQLString() string {
	switch o.Type {
	case Plus:
		return "+"
	case Minus:
		return "-"
	case Multiply:
		return "*"
	case Divide:
		return "/"
	case Modulus:
		return "%"
	case Gt:
		return ">"
	case Lt:
		return "<"
	case GtEq:
		return ">="
	case LtEq:
		return "<="
	case Eq:
		return "="
	case NotEq:
		return "!="
	case And:
		return "AND"
	case Or:
		return "OR"
	case Not:
		return "NOT"
	case Like:
		return "LIKE"
	case NotLike:
		return "NOT LIKE"
	}
	return ""
}

func (o *Operator) WriteTo(w io.Writer) (int64, error) {
	switch o.Type {
	case Plus:
		return writeSingleBytes(w, []byte("+"))
	case Minus:
		return writeSingleBytes(w, []byte("-"))
	case Multiply:
		return writeSingleBytes(w, []byte("*"))
	case Divide:
		return writeSingleBytes(w, []byte("/"))
	case Modulus:
		return writeSingleBytes(w, []byte("%"))
	case Gt:
		return writeSingleBytes(w, []byte(">"))
	case Lt:
		return writeSingleBytes(w, []byte("<"))
	case GtEq:
		return writeSingleBytes(w, []byte(">="))
	case LtEq:
		return writeSingleBytes(w, []byte("<="))
	case Eq:
		return writeSingleBytes(w, []byte("="))
	case NotEq:
		return writeSingleBytes(w, []byte("!="))
	case And:
		return writeSingleBytes(w, []byte("AND"))
	case Or:
		return writeSingleBytes(w, []byte("OR"))
	case Not:
		return writeSingleBytes(w, []byte("NOT"))
	case Like:
		return writeSingleBytes(w, []byte("LIKE"))
	case NotLike:
		return writeSingleBytes(w, []byte("NOT LIKE"))
	}
	return 0, nil
}