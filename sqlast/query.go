package sqlast

import (
	"fmt"
	"log"
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
	Distinct      bool
	Projection    []SQLSelectItem
	FromClause    []TableReference
	WhereClause   ASTNode
	GroupByClause []ASTNode
	HavingClause  ASTNode
}

func (s *SQLSelect) ToSQLString() string {
	q := "SELECT "
	if s.Distinct {
		q += "DISTINCT "
	}
	q += commaSeparatedString(s.Projection)

	if len(s.FromClause) != 0 {
		q += fmt.Sprintf(" FROM %s", commaSeparatedString(s.FromClause))
	}

	if s.WhereClause != nil {
		q += fmt.Sprintf(" WHERE %s", s.WhereClause.ToSQLString())
	}

	if len(s.GroupByClause) != 0 {
		q += fmt.Sprintf(" GROUP BY %s", commaSeparatedString(s.GroupByClause))
	}

	if s.HavingClause != nil {
		q += fmt.Sprintf(" HAVING %s", s.HavingClause.ToSQLString())
	}

	return q
}

//go:generate genmark -t TableReference -e ASTNode

//go:generate genmark -t TableFactor -e TableReference

type Table struct {
	tableFactor
	tableReference
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
	tableReference
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

type CrossJoin struct {
	tableReference
	Reference TableReference
	Factor    TableFactor
}

func (c *CrossJoin) ToSQLString() string {
	return fmt.Sprintf("%s CROSS JOIN %s", c.Reference.ToSQLString(), c.Factor.ToSQLString())
}

//go:generate genmark -t JoinElement -e ASTNode

type TableJoinElement struct {
	joinElement
	Ref TableReference
}

func (t *TableJoinElement) ToSQLString() string {
	return t.Ref.ToSQLString()
}

type PartitionedJoinTable struct {
	joinElement
	tableReference
	Factor     TableFactor
	ColumnList []*SQLIdent
}

func (p *PartitionedJoinTable) ToSQLString() string {
	return fmt.Sprintf("%s PARTITION BY (%s)", p.Factor.ToSQLString(), commaSeparatedString(p.ColumnList))
}

type QualifiedJoin struct {
	tableReference
	LeftElement  *TableJoinElement
	Type         JoinType
	RightElement *TableJoinElement
	Spec         JoinSpec
}

func (q *QualifiedJoin) ToSQLString() string {
	return fmt.Sprintf("%s %sJOIN %s %s", q.LeftElement.ToSQLString(), q.Type.ToSQLString(), q.RightElement.ToSQLString(), q.Spec.ToSQLString())
}

type NaturalJoin struct {
	tableReference
	LeftElement  *TableJoinElement
	Type         JoinType
	RightElement *TableJoinElement
}

func (n *NaturalJoin) ToSQLString() string {
	return fmt.Sprintf("%s NATURAL %sJOIN %s", n.LeftElement.ToSQLString(), n.Type.ToSQLString(), n.RightElement.ToSQLString())
}

//go:generate genmark -t JoinSpec -e ASTNode

type NamedColumnsJoin struct {
	joinSpec
	ColumnList []*SQLIdent
}

func (n *NamedColumnsJoin) ToSQLString() string {
	return fmt.Sprintf("USING (%s)", commaSeparatedString(n.ColumnList))
}

type JoinCondition struct {
	joinSpec
	SearchCondition ASTNode
}

func (j *JoinCondition) ToSQLString() string {
	return fmt.Sprintf("ON %s", j.SearchCondition.ToSQLString())
}

type JoinType int

const (
	INNER JoinType = iota
	LEFT
	RIGHT
	FULL
	LEFTOUTER
	RIGHTOUTER
	FULLOUTER
	IMPLICIT
)

func (j JoinType) ToSQLString() string {
	switch j {
	case INNER:
		return "INNER "
	case LEFT:
		return "LEFT "
	case RIGHT:
		return "RIGHT "
	case FULL:
		return "FULL "
	case LEFTOUTER:
		return "LEFT OUTER "
	case RIGHTOUTER:
		return "RIGHT OUTER "
	case FULLOUTER:
		return "FULL OUTER "
	case IMPLICIT:
		return ""
	default:
		log.Fatalf("unknown join type %d", j)
	}
	return ""
}

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
