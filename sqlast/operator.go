package sqlast

type SQLOperator int

const (
	Plus SQLOperator = iota
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
)

func (s *SQLOperator) String() string {
	switch *s {
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
