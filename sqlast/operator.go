package sqlast

import (
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
		return "Or"
	case Not:
		return "NOT"
	case Like:
		return "LIKE"
	case NotLike:
		return "NOT LIKE"
	}
	return ""
}
