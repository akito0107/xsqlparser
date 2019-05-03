package sqlast

import (
	"fmt"
	"strings"
)

type SQLQuery struct {
	CTEs []CTE
	Body *SQLSetExpr
}

func (s *SQLQuery) Eval() string {
	return ""
}

type CTE struct {
	Alias SQLIdent
	Query *SQLQuery
}

type SQLSetExpr struct {
	Select       *SQLSelect
	Query        *SQLQuery
	SetOperation struct {
		Op    *SQLSetOperator
		All   bool
		Left  *SQLSetExpr
		Right *SQLSetExpr
	}
}

type SQLSetOperator struct{}

type SQLSelect struct {
	Distinct   bool
	Projection []SQLSelectItem
	Relation   TableFactor
	Joins      []Join
	Selection  ASTNode
	GroupBy    []ASTNode
	Having     ASTNode
}

func (s *SQLSelect) Eval() string {
	q := "SELECT "
	if s.Distinct {
		q += "DISTINCT "
	}
	q += commaSeparatedString(s.Projection)

	if s.Relation != nil {
		q += fmt.Sprintf(" FROM %s", s.Relation.Eval())
	}

	for _, j := range s.Joins {
		q += j.Eval()
	}

	if s.Selection != nil {
		q += fmt.Sprintf(" WHERE %s", s.Selection.Eval())
	}

	if len(s.GroupBy) != 0 {
		q += fmt.Sprintf(" GROUP BY %s", commaSeparatedString(s.GroupBy))
	}

	if s.Having != nil {
		q += fmt.Sprintf(" HAVING %s", s.Having.Eval())
	}

	return q
}

/** TableFactor **/
type TableFactor interface {
	Eval() string
}

type Table struct {
	Name      *SQLObjectName
	Alias     *SQLIdent
	Args      []ASTNode
	WithHints []ASTNode
}

func (t *Table) Eval() string {
	s := t.Name.Eval()
	if len(t.Args) != 0 {
		s = fmt.Sprintf("%s(%s)", s, commaSeparatedString(t.Args))
	}
	if t.Alias != nil {
		s = fmt.Sprintf("%s AS %s", s, t.Alias.Eval())
	}
	if len(t.WithHints) != 0 {
		s = fmt.Sprintf("%s WITH (%s)", s, commaSeparatedString(t.WithHints))
	}
	return s
}

type Derived struct {
	SubQuery *SQLQuery
	Alias    *SQLIdent
}

func (d *Derived) Eval() string {
	s := d.SubQuery.Eval()
	if d.Alias != nil {
		s = fmt.Sprintf("%s AS %s", s, d.Alias.Eval())
	}
	return s
}

/** TableFactor end **/

/** SQLSelectItem **/
type SQLSelectItem interface {
	ASTNode
}

type UnnamedExpression struct {
	Node ASTNode
}

func (u *UnnamedExpression) Eval() string {
	return u.Node.Eval()
}

type ExpressionWithAlias struct {
	Expr  ASTNode
	Alias SQLIdent
}

func (e *ExpressionWithAlias) Eval() string {
	return fmt.Sprintf("%s AS %s", e.Expr.Eval(), e.Alias.Eval())
}

type QualifiedWildcard struct {
	Prefix SQLObjectName
}

func (q *QualifiedWildcard) Eval() string {
	return fmt.Sprintf("%s.*", q.Prefix.Eval())
}

type Wildcard struct{}

func (w *Wildcard) Eval() string {
	return "*"
}

/** SQLSelectItem end **/

type Join struct {
	Relation TableFactor
	Op       JoinOperator
	Constant JoinConstant
}

func (j *Join) Eval() string {
	switch j.Op {
	case Inner:
		return fmt.Sprintf(" %sJOIN %s%s", j.Constant.Prefix(), j.Relation.Eval(), j.Constant.Suffix())
	case Cross:
		return fmt.Sprintf(" CROSS JOIN%s", j.Relation.Eval())
	case Implicit:
		return fmt.Sprintf(", %s", j.Relation.Eval())
	case LeftOuter:
		return fmt.Sprintf("%sLEFT JOIN %s%s", j.Constant.Prefix(), j.Relation.Eval(), j.Constant.Suffix())
	case RightOuter:
		return fmt.Sprintf("%sRIGHT JOIN %s%s", j.Constant.Prefix(), j.Relation.Eval(), j.Constant.Suffix())
	case FullOuter:
		return fmt.Sprintf("%sFULL JOIN %s%s", j.Constant.Prefix(), j.Relation.Eval(), j.Constant.Suffix())
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
	return fmt.Sprintf(" ON %s", o.Node.Eval())
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
	return fmt.Sprintf(" USING(%s)", strings.Join(str, ", "))
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
