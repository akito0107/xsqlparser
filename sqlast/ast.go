package sqlast

import (
	"fmt"
	"log"
	"strings"
)

type SQLIdent string

func NewSQLIdent(str string) *SQLIdent {
	s := SQLIdent(str)
	return &s
}

func (s *SQLIdent) ToSQLString() string {
	return string(*s)
}

type ASTNode interface {
	ToSQLString() string
}

// Identifier e.g. table name or column name
type SQLIdentifier struct {
	Ident *SQLIdent
}

func NewSQLIdentifier(ident *SQLIdent) *SQLIdentifier {
	return &SQLIdentifier{
		Ident: ident,
	}
}

func (s *SQLIdentifier) ToSQLString() string {
	return string(*s.Ident)
}

// *
type SQLWildcard struct{}

func (s SQLWildcard) ToSQLString() string {
	return "*"
}

// table.*, schema.table.*
type SQLQualifiedWildcard struct {
	Idents []*SQLIdent
}

func (s *SQLQualifiedWildcard) ToSQLString() string {
	strs := make([]string, 0, len(s.Idents))
	for _, ident := range s.Idents {
		strs = append(strs, string(*ident))
	}
	return fmt.Sprintf("%s.*", strings.Join(strs, "."))
}

// table.column / schema.table.column
type SQLCompoundIdentifier struct {
	Idents []*SQLIdent
}

func (s *SQLCompoundIdentifier) ToSQLString() string {
	strs := make([]string, 0, len(s.Idents))
	for _, ident := range s.Idents {
		strs = append(strs, string(*ident))
	}
	return strings.Join(strs, ".")
}

type SQLIsNull struct {
	X ASTNode
}

func (s *SQLIsNull) ToSQLString() string {
	return fmt.Sprintf("%s IS NULl", s.X.ToSQLString())
}

type SQLIsNotNull struct {
	X ASTNode
}

func (s *SQLIsNotNull) ToSQLString() string {
	return fmt.Sprintf("%s IS NOT NULL", s.X.ToSQLString())
}

type SQLInList struct {
	Expr    ASTNode
	List    []ASTNode
	Negated bool
}

func (s *SQLInList) ToSQLString() string {
	return fmt.Sprintf("%s %sIN (%s)", s.Expr.ToSQLString(), negatedString(s.Negated), commaSeparatedString(s.List))
}

//[ NOT ] IN (SELECT ...)
type SQLInSubQuery struct {
	Expr     ASTNode
	SubQuery *SQLQuery
	Negated  bool
}

func (s *SQLInSubQuery) ToSQLString() string {
	return fmt.Sprintf("%s %sIN (%s)", s.Expr.ToSQLString(), negatedString(s.Negated), s.SubQuery.ToSQLString())
}

type SQLBetween struct {
	Expr    ASTNode
	Negated bool
	Low     ASTNode
	High    ASTNode
}

func (s *SQLBetween) ToSQLString() string {
	return fmt.Sprintf("%s %sBETWEEN %s AND %s", s.Expr.ToSQLString(), negatedString(s.Negated), s.Low.ToSQLString(), s.High.ToSQLString())
}

type SQLBinaryExpr struct {
	Left  ASTNode
	Op    SQLOperator
	Right ASTNode
}

func (s *SQLBinaryExpr) ToSQLString() string {
	return fmt.Sprintf("%s %s %s", s.Left.ToSQLString(), s.Op.ToSQLString(), s.Right.ToSQLString())
}

type SQLCast struct {
	Expr     ASTNode
	DateType SQLType
}

func (s *SQLCast) ToSQLString() string {
	return fmt.Sprintf("CAST(%s AS %s)", s.Expr.ToSQLString(), s.DateType.ToSQLString())
}

type SQLNested struct {
	AST ASTNode
}

func (s *SQLNested) ToSQLString() string {
	return fmt.Sprintf("(%s)", s.AST.ToSQLString())
}

type SQLUnary struct {
	Operator SQLOperator
	Expr     ASTNode
}

func (s *SQLUnary) ToSQLString() string {
	return fmt.Sprintf("%s %s", s.Operator.ToSQLString(), s.Expr.ToSQLString())
}

type SQLValue struct {
	Value Value
}

func (s *SQLValue) ToSQLString() string {
	return s.Value.ToSQLString()
}

type SQLFunction struct {
	Name *SQLObjectName
	Args []ASTNode
	Over *SQLWindowSpec
}

func (s *SQLFunction) ToSQLString() string {
	str := fmt.Sprintf("%s(%s)", s.Name.ToSQLString(), commaSeparatedString(s.Args))

	if s.Over != nil {
		str += fmt.Sprintf(" OVER (%s)", s.Over.ToSQLString())
	}

	return str
}

type SQLCase struct {
	Operand    ASTNode
	Conditions []ASTNode
	Results    []ASTNode
	ElseResult ASTNode
}

func (s *SQLCase) ToSQLString() string {
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

type SQLExists struct {
	Negated bool
	Query   *SQLQuery
}

func (s *SQLExists) ToSQLString() string {
	return fmt.Sprintf("%sEXISTS (%s)", negatedString(s.Negated), s.Query.ToSQLString())
}

type SQLSubquery struct {
	Query *SQLQuery
}

func (s *SQLSubquery) ToSQLString() string {
	return fmt.Sprintf("(%s)", s.Query.ToSQLString())
}

type SQLObjectName struct {
	Idents []*SQLIdent
}

func NewSQLObjectName(strs ...string) *SQLObjectName {
	idents := make([]*SQLIdent, 0, len(strs))

	for _, s := range strs {
		idents = append(idents, NewSQLIdent(s))
	}

	return &SQLObjectName{
		Idents: idents,
	}
}

func (s *SQLObjectName) ToSQLString() string {
	var strs []string
	for _, l := range s.Idents {
		strs = append(strs, string(*l))
	}
	return strings.Join(strs, ".")
}

func commaSeparatedString(list interface{}) string {
	var strs []string
	switch s := list.(type) {
	case []ASTNode:
		for _, l := range s {
			strs = append(strs, l.ToSQLString())
		}
	case []*SQLObjectName:
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
	case []*SQLAssignment:
		for _, l := range s {
			strs = append(strs, l.ToSQLString())
		}
	case []*SQLIdent:
		for _, l := range s {
			strs = append(strs, l.ToSQLString())
		}
	case []*SQLOrderByExpr:
		for _, l := range s {
			strs = append(strs, l.ToSQLString())
		}
	case []*SQLColumnDef:
		for _, l := range s {
			strs = append(strs, l.ToSQLString())
		}
	case []*TableConstraint:
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

type SQLWindowSpec struct {
	PartitionBy  []ASTNode
	OrderBy      []*SQLOrderByExpr
	WindowsFrame *SQLWindowFrame
}

func (s *SQLWindowSpec) ToSQLString() string {
	var clauses []string
	if len(s.PartitionBy) != 0 {
		clauses = append(clauses, fmt.Sprintf("PARTITION BY %s", commaSeparatedString(s.PartitionBy)))
	}
	if len(s.OrderBy) != 0 {
		clauses = append(clauses, fmt.Sprintf("ORDER BY %s", commaSeparatedString(s.OrderBy)))
	}

	if s.WindowsFrame != nil {
		if s.WindowsFrame.EndBound != nil {
			clauses = append(clauses, fmt.Sprintf("%s BETWEEN %s AND %s", s.WindowsFrame.Units.ToSQLString(), s.WindowsFrame.StartBound.ToSQLString(), s.WindowsFrame.EndBound.ToSQLString()))
		} else {
			clauses = append(clauses, fmt.Sprintf("%s %s", s.WindowsFrame.Units.ToSQLString(), s.WindowsFrame.StartBound.ToSQLString()))
		}
	}

	return strings.Join(clauses, " ")
}

type SQLWindowFrame struct {
	Units      SQLWindowFrameUnits
	StartBound SQLWindowFrameBound
	EndBound   SQLWindowFrameBound
}

type SQLWindowFrameUnits int

const (
	RowsUnit SQLWindowFrameUnits = iota
	RangeUnit
	GroupsUnit
)

func (s *SQLWindowFrameUnits) ToSQLString() string {
	switch *s {
	case RowsUnit:
		return "ROWS"
	case RangeUnit:
		return "RANGE"
	case GroupsUnit:
		return "GROUPS"
	}
	return ""
}

func (SQLWindowFrameUnits) FromStr(str string) SQLWindowFrameUnits {
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

//go:generate genmark -t SQLWindowFrameBound -e ASTNode

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
