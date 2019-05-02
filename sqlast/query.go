package sqlast

import (
	"fmt"
	"strings"
)

type SQLQuery struct {
	CTEs []CTE
	Body SQLSetExpr
}

func (s *SQLQuery) String() string {
	return ""
}

type CTE struct {
	Alias SQLIdent
	Query SQLQuery
}

type SQLSetExpr struct {
	Select       SQLSelect
	Query        SQLQuery
	SetOperation struct {
		Op    SQLSetOperator
		All   bool
		Left  SQLSetExpr
		Right SQLSetExpr
	}
}

type SQLSetOperator struct{}

type SQLSelect struct {
	Distinct   bool
	Projection []SQLSelectItem
	Relation   *TableFactor
	Joins      []Join
	Selection  ASTNode
	GroupBy    []ASTNode
	Having     ASTNode
}

func (s *SQLSelect) String() string {

}

/** TableFactor **/
type TableFactor interface {
	String() string
}

type Table struct {
	Name      SQLObjectName
	Alias     *SQLIdent
	Args      []ASTNode
	WithHints []ASTNode
}

func (t *Table) String() string {
	s := t.Name.String()
	if len(t.Args) != 0 {
		s = fmt.Sprintf("%s(%s)", s, commaSeparatedString(t.Args))
	}
	if t.Alias != nil {
		s = fmt.Sprintf("%s AS %s", s, t.Alias.String())
	}
	if len(t.WithHints) != 0 {
		s = fmt.Sprintf("%s WITH (%s)", commaSeparatedString(t.WithHints))
	}
	return s
}

type Derived struct {
	SubQuery SQLQuery
	Alias    *SQLIdent
}

func (d *Derived) String() string {
	s := d.SubQuery.String()
	if d.Alias != nil {
		s = fmt.Sprintf("%s AS %s", s, d.Alias.String())
	}
	return s
}

/** TableFactor end **/

/** SQLSelectItem **/
type SQLSelectItem interface {
	String() string
}

type UnnamedExpression struct {
	Node ASTNode
}

func (u *UnnamedExpression) String() string {
	return u.Node.String()
}

type ExpressionWithAlias struct {
	Expr  ASTNode
	Alias SQLIdent
}

func (e *ExpressionWithAlias) String() string {
	return fmt.Sprintf("%s AS %s", e.Expr.String(), e.Alias.String())
}

type QualifiedWildcard struct {
	Prefix SQLObjectName
}

func (q *QualifiedWildcard) String() string {
	return fmt.Sprintf("%s.*", q.Prefix.String())
}

type Wildcard struct{}

func (w *Wildcard) String() string {
	return "*"
}

/** SQLSelectItem end **/

type Join struct {
	Relation TableFactor
	Op       JoinOperator
	Constant JoinConstant
}

func (j *Join) String() string {
	switch j.Op {
	case Inner:
		return fmt.Sprintf(" %sJOIN %s %s", j.Constant.Prefix(), j.Relation.String(), j.Constant.Suffix())
	case Cross:
		return fmt.Sprintf(" CROSS JOIN %s", j.Relation.String())
	case Implicit:
		return fmt.Sprintf(", %s", j.Relation.String())
	case LeftOuter:
		return fmt.Sprintf("%sLEFT JOIN %s %s", j.Constant.Prefix(), j.Relation.String(), j.Constant.Suffix())
	case RightOuter:
		return fmt.Sprintf("%sRIGHT JOIN %s %s", j.Constant.Prefix(), j.Relation.String(), j.Constant.Suffix())
	case FullOuter:
		return fmt.Sprintf("%sFULL JOIN %s %s", j.Constant.Prefix(), j.Relation.String(), j.Constant.Suffix())
	default:
		return ""
	}
}

type JoinOperator int

const (
	Inner JoinOperator = iota
	LeftOuter
	RightOuter
	FullOuter
	Implicit
	Cross
)

/** JoinConstant **/
type JoinConstant interface {
	Prefix() string
	Suffix() string
}

type OnJoinConstant struct {
	Node ASTNode
}

func (*OnJoinConstant) Prefix() string {
	return ""
}

func (o *OnJoinConstant) Suffix() string {
	return fmt.Sprintf("ON %s", o.Node.String())
}

type UsingConstant struct {
	Idents []SQLIdent
}

func (*UsingConstant) Prefix() string {
	return ""
}

func (u *UsingConstant) Suffix() string {
	var str []string
	for _, i := range u.Idents {
		str = append(str, string(i))
	}
	return fmt.Sprintf("USING(%s)", strings.Join(str, ", "))
}

type NaturalConstant struct {
}

func (*NaturalConstant) Prefix() string {
	return "NATURAL "
}

func (*NaturalConstant) Suffix() string {
	return ""
}

/** JoinConstant end **/
