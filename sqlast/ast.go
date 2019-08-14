package sqlast

import (
	"fmt"
	"log"
	"strings"
)

type Ident string

func NewIdent(str string) *Ident {
	s := Ident(str)
	return &s
}

func (s *Ident) ToSQLString() string {
	return string(*s)
}

type Node interface {
	ToSQLString() string
}

// *
type Wildcard struct{}

func (s Wildcard) ToSQLString() string {
	return "*"
}

// table.*, schema.table.*
type QualifiedWildcard struct {
	Idents []*Ident
}

func (s *QualifiedWildcard) ToSQLString() string {
	strs := make([]string, 0, len(s.Idents))
	for _, ident := range s.Idents {
		strs = append(strs, string(*ident))
	}
	return fmt.Sprintf("%s.*", strings.Join(strs, "."))
}

// table.column / schema.table.column
type CompoundIdent struct {
	Idents []*Ident
}

func (s *CompoundIdent) ToSQLString() string {
	strs := make([]string, 0, len(s.Idents))
	for _, ident := range s.Idents {
		strs = append(strs, string(*ident))
	}
	return strings.Join(strs, ".")
}

type IsNull struct {
	X Node
}

func (s *IsNull) ToSQLString() string {
	return fmt.Sprintf("%s IS NULl", s.X.ToSQLString())
}

type IsNotNull struct {
	X Node
}

func (s *IsNotNull) ToSQLString() string {
	return fmt.Sprintf("%s IS NOT NULL", s.X.ToSQLString())
}

type InList struct {
	Expr    Node
	List    []Node
	Negated bool
}

func (s *InList) ToSQLString() string {
	return fmt.Sprintf("%s %sIN (%s)", s.Expr.ToSQLString(), negatedString(s.Negated), commaSeparatedString(s.List))
}

//[ NOT ] IN (SELECT ...)
type InSubQuery struct {
	Expr     Node
	SubQuery *Query
	Negated  bool
}

func (s *InSubQuery) ToSQLString() string {
	return fmt.Sprintf("%s %sIN (%s)", s.Expr.ToSQLString(), negatedString(s.Negated), s.SubQuery.ToSQLString())
}

type Between struct {
	Expr    Node
	Negated bool
	Low     Node
	High    Node
}

func (s *Between) ToSQLString() string {
	return fmt.Sprintf("%s %sBETWEEN %s AND %s", s.Expr.ToSQLString(), negatedString(s.Negated), s.Low.ToSQLString(), s.High.ToSQLString())
}

type BinaryExpr struct {
	Left  Node
	Op    Operator
	Right Node
}

func (s *BinaryExpr) ToSQLString() string {
	return fmt.Sprintf("%s %s %s", s.Left.ToSQLString(), s.Op.ToSQLString(), s.Right.ToSQLString())
}

type Cast struct {
	Expr     Node
	DateType Type
}

func (s *Cast) ToSQLString() string {
	return fmt.Sprintf("CAST(%s AS %s)", s.Expr.ToSQLString(), s.DateType.ToSQLString())
}

type Nested struct {
	AST Node
}

func (s *Nested) ToSQLString() string {
	return fmt.Sprintf("(%s)", s.AST.ToSQLString())
}

type Unary struct {
	Operator Operator
	Expr     Node
}

func (s *Unary) ToSQLString() string {
	return fmt.Sprintf("%s %s", s.Operator.ToSQLString(), s.Expr.ToSQLString())
}

type Function struct {
	Name *ObjectName
	Args []Node
	Over *WindowSpec
}

func (s *Function) ToSQLString() string {
	str := fmt.Sprintf("%s(%s)", s.Name.ToSQLString(), commaSeparatedString(s.Args))

	if s.Over != nil {
		str += fmt.Sprintf(" OVER (%s)", s.Over.ToSQLString())
	}

	return str
}

type CaseExpr struct {
	Operand    Node
	Conditions []Node
	Results    []Node
	ElseResult Node
}

func (s *CaseExpr) ToSQLString() string {
	str := "CASE"
	if s.Operand != nil {
		str += fmt.Sprintf(" %s", s.Operand.ToSQLString())
	}
	var conditionsStr []string
	for i := 0; i < len(s.Conditions); i++ {
		conditionsStr = append(conditionsStr, fmt.Sprintf(" WHEN %s THEN %s", s.Conditions[i].ToSQLString(), s.Results[i].ToSQLString()))
	}
	str += strings.Join(conditionsStr, "")
	if s.ElseResult != nil {
		str += fmt.Sprintf(" ELSE %s", s.ElseResult.ToSQLString())
	}
	str += " END"

	return str
}

type Exists struct {
	Negated bool
	Query   *Query
}

func (s *Exists) ToSQLString() string {
	return fmt.Sprintf("%sEXISTS (%s)", negatedString(s.Negated), s.Query.ToSQLString())
}

type SubQuery struct {
	Query *Query
}

func (s *SubQuery) ToSQLString() string {
	return fmt.Sprintf("(%s)", s.Query.ToSQLString())
}

type ObjectName struct {
	Idents []*Ident
}

func NewObjectName(strs ...string) *ObjectName {
	idents := make([]*Ident, 0, len(strs))

	for _, s := range strs {
		idents = append(idents, NewIdent(s))
	}

	return &ObjectName{
		Idents: idents,
	}
}

func (s *ObjectName) ToSQLString() string {
	var strs []string
	for _, l := range s.Idents {
		strs = append(strs, string(*l))
	}
	return strings.Join(strs, ".")
}

func commaSeparatedString(list interface{}) string {
	var strs []string
	switch s := list.(type) {
	case []Node:
		for _, l := range s {
			strs = append(strs, l.ToSQLString())
		}
	case []*ObjectName:
		for _, l := range s {
			strs = append(strs, l.ToSQLString())
		}
	case []TableElement:
		for _, l := range s {
			strs = append(strs, l.ToSQLString())
		}
	case []SQLSelectItem:
		for _, l := range s {
			strs = append(strs, l.ToSQLString())
		}
	case []*Assignment:
		for _, l := range s {
			strs = append(strs, l.ToSQLString())
		}
	case []*Ident:
		for _, l := range s {
			strs = append(strs, l.ToSQLString())
		}
	case []*OrderByExpr:
		for _, l := range s {
			strs = append(strs, l.ToSQLString())
		}
	case []*ColumnDef:
		for _, l := range s {
			strs = append(strs, l.ToSQLString())
		}
	case []*TableConstraint:
		for _, l := range s {
			strs = append(strs, l.ToSQLString())
		}
	case []TableReference:
		for _, l := range s {
			strs = append(strs, l.ToSQLString())
		}
	default:
		log.Fatalf("unexpected type array %+v", list)
	}
	return strings.Join(strs, ", ")

}

func negatedString(negated bool) string {
	var n string
	if negated {
		n = "NOT "
	}

	return n
}

type WindowSpec struct {
	PartitionBy  []Node
	OrderBy      []*OrderByExpr
	WindowsFrame *WindowFrame
}

func (s *WindowSpec) ToSQLString() string {
	var clauses []string
	if len(s.PartitionBy) != 0 {
		clauses = append(clauses, fmt.Sprintf("PARTITION BY %s", commaSeparatedString(s.PartitionBy)))
	}
	if len(s.OrderBy) != 0 {
		clauses = append(clauses, fmt.Sprintf("ORDER BY %s", commaSeparatedString(s.OrderBy)))
	}

	if s.WindowsFrame != nil {
		clauses = append(clauses, s.WindowsFrame.ToSQLString())
	}

	return strings.Join(clauses, " ")
}

type WindowFrame struct {
	Units      WindowFrameUnits
	StartBound SQLWindowFrameBound
	EndBound   SQLWindowFrameBound
}

func (s *WindowFrame) ToSQLString() string {
	if s.EndBound != nil {
		return fmt.Sprintf("%s BETWEEN %s AND %s", s.Units.ToSQLString(), s.StartBound.ToSQLString(), s.EndBound.ToSQLString())
	} else {
		return fmt.Sprintf("%s %s", s.Units.ToSQLString(), s.StartBound.ToSQLString())
	}
}

type WindowFrameUnits int

const (
	RowsUnit WindowFrameUnits = iota
	RangeUnit
	GroupsUnit
)

func (s WindowFrameUnits) ToSQLString() string {
	switch s {
	case RowsUnit:
		return "ROWS"
	case RangeUnit:
		return "RANGE"
	case GroupsUnit:
		return "GROUPS"
	}
	return ""
}

func (WindowFrameUnits) FromStr(str string) WindowFrameUnits {
	if str == "ROWS" {
		return RowsUnit
	} else if str == "RANGE" {
		return RangeUnit
	} else if str == "GROUPS" {
		return GroupsUnit
	}
	log.Fatalf("expected ROWS, RANGE, GROUPS but: %s", str)
	return 0
}

//go:generate genmark -t SQLWindowFrameBound -e Node

type CurrentRow struct {
	sqlWindowFrameBound
}

func (*CurrentRow) ToSQLString() string {
	return "CURRENT ROW"
}

type UnboundedPreceding struct {
	sqlWindowFrameBound
}

func (*UnboundedPreceding) ToSQLString() string {
	return "UNBOUNDED PRECEDING"
}

type UnboundedFollowing struct {
	sqlWindowFrameBound
}

func (*UnboundedFollowing) ToSQLString() string {
	return "UNBOUNDED FOLLOWING"
}

type Preceding struct {
	sqlWindowFrameBound
	Bound *uint64
}

func (p *Preceding) ToSQLString() string {
	return fmt.Sprintf("%d PRECEDING", *p.Bound)
}

type Following struct {
	sqlWindowFrameBound
	Bound *uint64
}

func (f *Following) ToSQLString() string {
	return fmt.Sprintf("%d FOLLOWING", *f.Bound)
}
