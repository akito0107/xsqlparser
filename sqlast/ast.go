package sqlast

import (
	"fmt"
	"log"
	"strings"
)

type SQLIdent string

func (s *SQLIdent) Eval() string {
	return string(*s)
}

type ASTNode interface {
	Eval() string
}

// Identifier e.g. table name or column name
type SQLIdentifier struct {
	Ident SQLIdent
}

func (s *SQLIdentifier) Eval() string {
	return string(s.Ident)
}

// *
type SQLWildcard struct{}

func (s SQLWildcard) Eval() string {
	return "*"
}

// table.*, schema.table.*
type SQLQualifiedWildcard struct {
	Idents []SQLIdent
}

func (s *SQLQualifiedWildcard) Eval() string {
	strs := make([]string, 0, len(s.Idents))
	for _, ident := range s.Idents {
		strs = append(strs, string(ident))
	}
	return fmt.Sprintf("%s.*", strings.Join(strs, "."))
}

// table.column / schema.table.column
type SQLCompoundIdentifier struct {
	Idents []SQLIdent
}

func (s *SQLCompoundIdentifier) Eval() string {
	strs := make([]string, 0, len(s.Idents))
	for _, ident := range s.Idents {
		strs = append(strs, string(ident))
	}
	return strings.Join(strs, ".")
}

type SQLIsNull struct {
	X ASTNode
}

func (s *SQLIsNull) Eval() string {
	return fmt.Sprintf("%s IS NULl", s.X.Eval())
}

type SQLIsNotNull struct {
	X ASTNode
}

func (s *SQLIsNotNull) Eval() string {
	return fmt.Sprintf("%s IS NOT NULL", s.X.Eval())
}

type SQLInList struct {
	Expr    ASTNode
	List    []ASTNode
	Negated bool
}

func (s *SQLInList) Eval() string {
	return fmt.Sprintf("%s %sIN {%s}", s.Expr.Eval(), negatedString(s.Negated), commaSeparatedString(s.List))
}

//[ NOT ] IN (SELECT ...)
type SQLInSubQuery struct {
	Expr     ASTNode
	SubQuery *SQLQuery
	Negated  bool
}

func (s *SQLInSubQuery) Eval() string {
	return fmt.Sprintf("%s %sIN (%s)", s.Expr.Eval(), negatedString(s.Negated), s.SubQuery.Eval())
}

type SQLBetween struct {
	Expr    ASTNode
	Negated bool
	Low     ASTNode
	High    ASTNode
}

func (s *SQLBetween) Eval() string {
	return fmt.Sprintf("%s %sBETWEEN %s AND %s", s.Expr.Eval(), negatedString(s.Negated), s.Low.Eval(), s.High.Eval())
}

type SQLBinaryExpr struct {
	Left  ASTNode
	Op    SQLOperator
	Right ASTNode
}

func (s *SQLBinaryExpr) Eval() string {
	return fmt.Sprintf("%s %s %s", s.Left.Eval(), s.Op.Eval(), s.Right.Eval())
}

type SQLCast struct {
	Expr     ASTNode
	DateType SQLType
}

func (s *SQLCast) Eval() string {
	panic("implement me")
}

type SQLNested struct {
	AST ASTNode
}

func (s *SQLNested) Eval() string {
	panic("implement me")
}

type SQLValue struct {
	Value Value
}

func (*SQLValue) Eval() string {
	panic("implement me")
}

type SQLFunction struct {
	Name SQLObjectName
	Args []ASTNode
	Over *SQLWindowSpec
}

func (s *SQLFunction) Eval() string {
	str := fmt.Sprintf("%s(%s)", s.Name.Eval(), commaSeparatedString(s.Args))

	if s.Over != nil {
		str += fmt.Sprintf(" OVER (%s)", s.Over.Eval())
	}

	return str
}

type SQLCase struct {
	Operand    ASTNode
	Conditions []ASTNode
	Results    []ASTNode
	ElseResult ASTNode
}

func (s *SQLCase) Eval() string {
	str := "CASE"
	if s.Operand != nil {
		str += fmt.Sprintf(" %s", s.Operand.Eval())
	}
	var conditionsStr []string
	for i := 0; i < len(s.Conditions); i++ {
		conditionsStr = append(conditionsStr, fmt.Sprintf(" WHEN %s THEN %s", s.Conditions[i].Eval(), s.Results[i].Eval()))
	}
	str += strings.Join(conditionsStr, "")
	if s.ElseResult != nil {
		str += fmt.Sprintf(" ELSE %s", s.ElseResult.Eval())
	}
	str += " END"

	return str
}

type SQLObjectName struct {
	Idents []SQLIdent
}

func (s *SQLObjectName) Eval() string {
	var strs []string
	for _, l := range s.Idents {
		strs = append(strs, string(l))
	}
	return strings.Join(strs, ".")
}

func commaSeparatedString(list interface{}) string {
	var strs []string
	switch s := list.(type) {
	case []ASTNode:
		for _, l := range s {
			strs = append(strs, l.Eval())
		}
	case []SQLSelectItem:
		for _, l := range s {
			strs = append(strs, l.Eval())
		}
	case []SQLIdent:
		for _, l := range s {
			strs = append(strs, l.Eval())
		}
	case []SQLOrderByExpr:
		for _, l := range s {
			strs = append(strs, l.Eval())
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
	OrderBy      []SQLOrderByExpr
	WindowsFrame *SQLWindowFrame
}

func (s *SQLWindowSpec) Eval() string {
	var clauses []string
	if len(s.PartitionBy) != 0 {
		clauses = append(clauses, fmt.Sprintf("PARTITION BY %s", commaSeparatedString(s.PartitionBy)))
	}
	if len(s.OrderBy) != 0 {
		clauses = append(clauses, fmt.Sprintf("ORDER BY %s", commaSeparatedString(s.OrderBy)))
	}

	if s.WindowsFrame != nil {
		if s.WindowsFrame.EndBound != nil {
			clauses = append(clauses, fmt.Sprintf("%s BETWEEN %s AND %s", s.WindowsFrame.Units.Eval(), s.WindowsFrame.StartBound.Eval(), s.WindowsFrame.EndBound.Eval()))
		} else {
			clauses = append(clauses, fmt.Sprintf("%s %s", s.WindowsFrame.Units.Eval(), s.WindowsFrame.StartBound.Eval()))
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

func (s *SQLWindowFrameUnits) Eval() string {
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

type SQLWindowFrameBound interface {
	ASTNode
}

type CurrentRow struct{}

func (*CurrentRow) Eval() string {
	return "CURRENT ROW"
}

type UnboundedPreceding struct{}

func (*UnboundedPreceding) Eval() string {
	return "UNBOUNDED PRECEDING"
}

type UnboundedFollowing struct{}

func (*UnboundedFollowing) Eval() string {
	return "UNBOUNDED FOLLOWING"
}

type Preceding struct {
	Bound uint64
}

func (p *Preceding) Eval() string {
	return fmt.Sprintf("%d PRECEDING", p.Bound)
}

type Following struct {
	Bound uint64
}

func (f *Following) Eval() string {
	return fmt.Sprintf("%d FOLLOWING", f.Bound)
}
