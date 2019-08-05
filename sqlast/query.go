package sqlast

import (
	"fmt"
	"strings"
)

type SQLQuery struct {
	sqlStmt
	CTEs    []*CTE
	Body    SQLSetExpr
	OrderBy []*SQLOrderByExpr
	Limit   *LimitExpr
}

func (s *SQLQuery) ToSQLString() string {
	var q string

	if len(s.CTEs) != 0 {
		q += "WITH "
		ctestrs := make([]string, 0, len(s.CTEs))
		for _, cte := range s.CTEs {
			ctestrs = append(ctestrs, cte.ToSQLString())
		}
		q += strings.Join(ctestrs, ", ") + " "
	}

	q += s.Body.ToSQLString()

	if len(s.OrderBy) != 0 {
		q += fmt.Sprintf(" ORDER BY %s", commaSeparatedString(s.OrderBy))
	}

	if s.Limit != nil {
		q += " " + s.Limit.ToSQLString()
	}

	return q
}

type CTE struct {
	Alias *SQLIdent
	Query *SQLQuery
}

func (c *CTE) ToSQLString() string {
	return fmt.Sprintf("%s AS (%s)", c.Alias.ToSQLString(), c.Query.ToSQLString())
}

//go:generate genmark -t SQLSetExpr -e ASTNode

type SelectExpr struct {
	sqlSetExpr
	Select *SQLSelect
}

func (s *SelectExpr) ToSQLString() string {
	return s.Select.ToSQLString()
}

type QueryExpr struct {
	sqlSetExpr
	Query *SQLQuery
}

func (q *QueryExpr) ToSQLString() string {
	return fmt.Sprintf("(%s)", q.Query.ToSQLString())
}

type SetOperationExpr struct {
	sqlSetExpr
	Op    SQLSetOperator
	All   bool
	Left  SQLSetExpr
	Right SQLSetExpr
}

func (s *SetOperationExpr) ToSQLString() string {
	var allStr string
	if s.All {
		allStr = " ALL"
	}
	return fmt.Sprintf("%s %s%s %s", s.Left.ToSQLString(), s.Op.ToSQLString(), allStr, s.Right.ToSQLString())
}

//go:generate genmark -t SQLSetOperator -e ASTNode

type UnionOperator struct {
	sqlSetOperator
}

func (UnionOperator) ToSQLString() string {
	return "UNION"
}

type ExceptOperator struct {
	sqlSetOperator
}

func (ExceptOperator) ToSQLString() string {
	return "EXCEPT"
}

type IntersectOperator struct {
	sqlSetOperator
}

func (IntersectOperator) ToSQLString() string {
	return "INTERSECT"
}

type SQLSelect struct {
	sqlSetExpr
	Distinct   bool
	Projection []SQLSelectItem
	Relation   TableFactor
	Joins      []*Join
	Selection  ASTNode
	GroupBy    []ASTNode
	Having     ASTNode
}

func (s *SQLSelect) ToSQLString() string {
	q := "SELECT "
	if s.Distinct {
		q += "DISTINCT "
	}
	q += commaSeparatedString(s.Projection)

	if s.Relation != nil {
		q += fmt.Sprintf(" FROM %s", s.Relation.ToSQLString())
	}

	for _, j := range s.Joins {
		q += j.ToSQLString()
	}

	if s.Selection != nil {
		q += fmt.Sprintf(" WHERE %s", s.Selection.ToSQLString())
	}

	if len(s.GroupBy) != 0 {
		q += fmt.Sprintf(" GROUP BY %s", commaSeparatedString(s.GroupBy))
	}

	if s.Having != nil {
		q += fmt.Sprintf(" HAVING %s", s.Having.ToSQLString())
	}

	return q
}

//go:generate genmark -t TableFactor -e ASTNode

type Table struct {
	tableFactor
	Name      *SQLObjectName
	Alias     *SQLIdent
	Args      []ASTNode
	WithHints []ASTNode
}

func (t *Table) ToSQLString() string {
	s := t.Name.ToSQLString()
	if len(t.Args) != 0 {
		s = fmt.Sprintf("%s(%s)", s, commaSeparatedString(t.Args))
	}
	if t.Alias != nil {
		s = fmt.Sprintf("%s AS %s", s, t.Alias.ToSQLString())
	}
	if len(t.WithHints) != 0 {
		s = fmt.Sprintf("%s WITH (%s)", s, commaSeparatedString(t.WithHints))
	}
	return s
}

type Derived struct {
	tableFactor
	Lateral  bool
	SubQuery *SQLQuery
	Alias    *SQLIdent
}

func (d *Derived) ToSQLString() string {
	var lateralStr string

	if d.Lateral {
		lateralStr = "LATERAL "
	}

	s := fmt.Sprintf("%s(%s)", lateralStr, d.SubQuery.ToSQLString())
	if d.Alias != nil {
		s = fmt.Sprintf("%s AS %s", s, d.Alias.ToSQLString())
	}
	return s
}

//go:generate genmark -t SQLSelectItem -e ASTNode

type UnnamedExpression struct {
	sqlSelectItem
	Node ASTNode
}

func (u *UnnamedExpression) ToSQLString() string {
	return u.Node.ToSQLString()
}

type ExpressionWithAlias struct {
	sqlSelectItem
	Expr  ASTNode
	Alias *SQLIdent
}

func (e *ExpressionWithAlias) ToSQLString() string {
	return fmt.Sprintf("%s AS %s", e.Expr.ToSQLString(), e.Alias.ToSQLString())
}

// schema.*
type QualifiedWildcard struct {
	sqlSelectItem
	Prefix *SQLObjectName
}

func (q *QualifiedWildcard) ToSQLString() string {
	return fmt.Sprintf("%s.*", q.Prefix.ToSQLString())
}

type Wildcard struct {
	sqlSelectItem
}

func (w *Wildcard) ToSQLString() string {
	return "*"
}

type Join struct {
	Relation TableFactor
	Op       JoinOperator
	Constant JoinConstant
}

func (j *Join) ToSQLString() string {
	switch j.Op {
	case Inner:
		return fmt.Sprintf(" %sJOIN %s%s", j.Constant.Prefix(), j.Relation.ToSQLString(), j.Constant.Suffix())
	case Cross:
		return fmt.Sprintf(" CROSS JOIN%s", j.Relation.ToSQLString())
	case Implicit:
		return fmt.Sprintf(", %s", j.Relation.ToSQLString())
	case LeftOuter:
		return fmt.Sprintf(" %sLEFT JOIN %s%s", j.Constant.Prefix(), j.Relation.ToSQLString(), j.Constant.Suffix())
	case RightOuter:
		return fmt.Sprintf(" %sRIGHT JOIN %s%s", j.Constant.Prefix(), j.Relation.ToSQLString(), j.Constant.Suffix())
	case FullOuter:
		return fmt.Sprintf(" %sFULL JOIN %s%s", j.Constant.Prefix(), j.Relation.ToSQLString(), j.Constant.Suffix())
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
	return fmt.Sprintf(" ON %s", o.Node.ToSQLString())
}

type UsingConstant struct {
	Idents []*SQLIdent
}

func (*UsingConstant) Prefix() string {
	return ""
}

func (u *UsingConstant) Suffix() string {
	var str []string
	for _, i := range u.Idents {
		str = append(str, string(*i))
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

type SQLOrderByExpr struct {
	Expr ASTNode
	ASC  *bool
}

func (s *SQLOrderByExpr) ToSQLString() string {
	if s.ASC == nil {
		return s.Expr.ToSQLString()
	}
	if *s.ASC {
		return fmt.Sprintf("%s ASC", s.Expr.ToSQLString())
	}
	return fmt.Sprintf("%s DESC", s.Expr.ToSQLString())
}

type LimitExpr struct {
	All         bool
	LimitValue  *LongValue
	OffsetValue *LongValue
}

func (l *LimitExpr) ToSQLString() string {
	str := "LIMIT"
	if l.All {
		str += " ALL"
	} else {
		str += " " + l.LimitValue.ToSQLString()
	}

	if l.OffsetValue != nil {
		str += " OFFSET " + l.OffsetValue.ToSQLString()
	}

	return str
}
