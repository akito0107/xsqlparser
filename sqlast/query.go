package sqlast

import (
	"fmt"
	"log"
	"strings"

	"github.com/akito0107/xsqlparser/sqltoken"
)

// QueryStmt stmt
type QueryStmt struct {
	stmt
	With    sqltoken.Pos // first char position of WITH if CTEs is not blank
	CTEs    []*CTE
	Body    SQLSetExpr
	OrderBy []*OrderByExpr
	Limit   *LimitExpr
}

func (q *QueryStmt) Pos() sqltoken.Pos {
	if len(q.CTEs) != 0 {
		return q.With
	}

	return q.Body.Pos()
}

func (q *QueryStmt) End() sqltoken.Pos {
	if q.Limit != nil {
		return q.Limit.End()
	}

	if len(q.OrderBy) != 0 {
		return q.OrderBy[len(q.OrderBy)-1].End()
	}

	return q.Body.End()
}

func (q *QueryStmt) ToSQLString() string {
	var query string

	if len(q.CTEs) != 0 {
		query += "WITH "
		ctestrs := make([]string, 0, len(q.CTEs))
		for _, cte := range q.CTEs {
			ctestrs = append(ctestrs, cte.ToSQLString())
		}
		query += strings.Join(ctestrs, ", ") + " "
	}

	query += q.Body.ToSQLString()

	if len(q.OrderBy) != 0 {
		query += fmt.Sprintf(" ORDER BY %s", commaSeparatedString(q.OrderBy))
	}

	if q.Limit != nil {
		query += " " + q.Limit.ToSQLString()
	}

	return query
}

// CTE
type CTE struct {
	Alias  *Ident
	Query  *QueryStmt
	RParen sqltoken.Pos
}

func (c *CTE) Pos() sqltoken.Pos {
	return c.Alias.Pos()
}

func (c *CTE) End() sqltoken.Pos {
	return c.RParen
}

func (c *CTE) ToSQLString() string {
	return fmt.Sprintf("%s AS (%s)", c.Alias.ToSQLString(), c.Query.ToSQLString())
}

//go:generate genmark -t SQLSetExpr -e Node

// Select
type SelectExpr struct {
	sqlSetExpr
	Select *SQLSelect
}

func (s *SelectExpr) Pos() sqltoken.Pos {
	return s.Select.Pos()
}

func (s *SelectExpr) End() sqltoken.Pos {
	return s.Select.End()
}

func (s *SelectExpr) ToSQLString() string {
	return s.Select.ToSQLString()
}

// (QueryStmt)
type QueryExpr struct {
	sqlSetExpr
	LParen, RParen sqltoken.Pos
	Query          *QueryStmt
}

func (q *QueryExpr) Pos() sqltoken.Pos {
	return q.LParen
}

func (q *QueryExpr) End() sqltoken.Pos {
	return q.RParen
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

func (s *SetOperationExpr) Pos() sqltoken.Pos {
	return s.Left.Pos()
}

func (s *SetOperationExpr) End() sqltoken.Pos {
	return s.Right.End()
}

func (s *SetOperationExpr) ToSQLString() string {
	var allStr string
	if s.All {
		allStr = " ALL"
	}
	return fmt.Sprintf("%s %s%s %s", s.Left.ToSQLString(), s.Op.ToSQLString(), allStr, s.Right.ToSQLString())
}

//go:generate genmark -t SQLSetOperator -e Node

type UnionOperator struct {
	sqlSetOperator
	From, To sqltoken.Pos
}

func (u *UnionOperator) Pos() sqltoken.Pos {
	return u.From
}

func (u *UnionOperator) End() sqltoken.Pos {
	return u.To
}

func (u *UnionOperator) ToSQLString() string {
	return "UNION"
}

type ExceptOperator struct {
	sqlSetOperator
	From, To sqltoken.Pos
}

func (e *ExceptOperator) Pos() sqltoken.Pos {
	return e.From
}

func (e *ExceptOperator) End() sqltoken.Pos {
	return e.To
}

func (*ExceptOperator) ToSQLString() string {
	return "EXCEPT"
}

type IntersectOperator struct {
	sqlSetOperator
	From, To sqltoken.Pos
}

func (i *IntersectOperator) Pos() sqltoken.Pos {
	return i.From
}

func (i *IntersectOperator) End() sqltoken.Pos {
	return i.To
}

func (IntersectOperator) ToSQLString() string {
	return "INTERSECT"
}

type SQLSelect struct {
	sqlSetExpr
	Distinct      bool
	Projection    []SQLSelectItem
	FromClause    []TableReference
	WhereClause   Node
	GroupByClause []Node
	HavingClause  Node
	Select        sqltoken.Pos // first position of SELECT
}

func (s *SQLSelect) Pos() sqltoken.Pos {
	return s.Select
}

func (s *SQLSelect) End() sqltoken.Pos {
	if s.HavingClause != nil {
		return s.HavingClause.End()
	}

	if len(s.GroupByClause) != 0 {
		return s.GroupByClause[len(s.GroupByClause)-1].End()
	}

	if s.WhereClause != nil {
		return s.WhereClause.End()
	}

	if len(s.FromClause) != 0 {
		return s.FromClause[len(s.FromClause)-1].End()
	}

	return s.Projection[len(s.Projection)-1].End()
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

//go:generate genmark -t TableReference -e Node

//go:generate genmark -t TableFactor -e TableReference

// Table
type Table struct {
	tableFactor
	tableReference
	Name            *ObjectName
	Alias           *Ident
	Args            []Node
	ArgsRParen      sqltoken.Pos
	WithHints       []Node
	WithHintsRParen sqltoken.Pos
}

func (t *Table) Pos() sqltoken.Pos {
	return t.Name.Pos()
}

func (t *Table) End() sqltoken.Pos {
	if len(t.WithHints) != 0 {
		return t.WithHintsRParen
	}

	if t.Alias != nil {
		return t.Alias.End()
	}

	if len(t.Args) != 0 {
		return t.ArgsRParen
	}

	return t.Name.End()
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
	Lateral    bool
	LateralPos sqltoken.Pos // last position of LATERAL keyword if Lateral is true
	LParen     sqltoken.Pos
	RParen     sqltoken.Pos
	SubQuery   *QueryStmt
	Alias      *Ident
}

func (d *Derived) Pos() sqltoken.Pos {
	if d.Lateral {
		return d.LateralPos
	}
	return d.LParen
}

func (d *Derived) End() sqltoken.Pos {
	if d.Alias != nil {
		return d.Alias.End()
	}

	return d.LParen
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

//go:generate genmark -t SQLSelectItem -e Node

type UnnamedSelectItem struct {
	sqlSelectItem
	Node Node
}

func (u *UnnamedSelectItem) Pos() sqltoken.Pos {
	return u.Node.Pos()
}

func (u *UnnamedSelectItem) End() sqltoken.Pos {
	return u.Node.End()
}

func (u *UnnamedSelectItem) ToSQLString() string {
	return u.Node.ToSQLString()
}

type AliasSelectItem struct {
	sqlSelectItem
	Expr  Node
	Alias *Ident
}

func (a *AliasSelectItem) Pos() sqltoken.Pos {
	return a.Expr.Pos()
}

func (a *AliasSelectItem) End() sqltoken.Pos {
	return a.Alias.End()
}

func (a *AliasSelectItem) ToSQLString() string {
	return fmt.Sprintf("%s AS %s", a.Expr.ToSQLString(), a.Alias.ToSQLString())
}

// schema.*
type QualifiedWildcardSelectItem struct {
	sqlSelectItem
	Prefix *ObjectName
}

func (q *QualifiedWildcardSelectItem) Pos() sqltoken.Pos {
	return q.Prefix.Pos()
}

func (q *QualifiedWildcardSelectItem) End() sqltoken.Pos {
	return sqltoken.Pos{
		Line: q.Prefix.End().Line,
		Col:  q.Prefix.End().Col + 2,
	}
}

func (q *QualifiedWildcardSelectItem) ToSQLString() string {
	return fmt.Sprintf("%s.*", q.Prefix.ToSQLString())
}

type WildcardSelectItem struct {
	sqlSelectItem
	From, To sqltoken.Pos
}

func (w *WildcardSelectItem) Pos() sqltoken.Pos {
	return w.From
}

func (w *WildcardSelectItem) End() sqltoken.Pos {
	return w.To
}

func (w *WildcardSelectItem) ToSQLString() string {
	return "*"
}

type CrossJoin struct {
	tableReference
	Reference TableReference
	Factor    TableFactor
}

func (c *CrossJoin) Pos() sqltoken.Pos {
	return c.Reference.Pos()
}

func (c *CrossJoin) End() sqltoken.Pos {
	return c.Factor.End()
}

func (c *CrossJoin) ToSQLString() string {
	return fmt.Sprintf("%s CROSS JOIN %s", c.Reference.ToSQLString(), c.Factor.ToSQLString())
}

//go:generate genmark -t JoinElement -e Node

type TableJoinElement struct {
	joinElement
	Ref TableReference
}

func (t *TableJoinElement) Pos() sqltoken.Pos {
	return t.Ref.Pos()
}

func (t *TableJoinElement) End() sqltoken.Pos {
	return t.Ref.End()
}

func (t *TableJoinElement) ToSQLString() string {
	return t.Ref.ToSQLString()
}

type PartitionedJoinTable struct {
	joinElement
	tableReference
	Factor     TableFactor
	ColumnList []*Ident
	RParen     sqltoken.Pos
}

func (p *PartitionedJoinTable) Pos() sqltoken.Pos {
	return p.Factor.Pos()
}

func (p *PartitionedJoinTable) End() sqltoken.Pos {
	return p.RParen
}

func (p *PartitionedJoinTable) ToSQLString() string {
	return fmt.Sprintf("%s PARTITION BY (%s)", p.Factor.ToSQLString(), commaSeparatedString(p.ColumnList))
}

type QualifiedJoin struct {
	tableReference
	LeftElement  *TableJoinElement
	Type         *JoinType
	RightElement *TableJoinElement
	Spec         JoinSpec
}

func (q *QualifiedJoin) Pos() sqltoken.Pos {
	return q.LeftElement.Pos()
}

func (q *QualifiedJoin) End() sqltoken.Pos {
	return q.Spec.End()
}

func (q *QualifiedJoin) ToSQLString() string {
	return fmt.Sprintf("%s %sJOIN %s %s", q.LeftElement.ToSQLString(), q.Type.ToSQLString(), q.RightElement.ToSQLString(), q.Spec.ToSQLString())
}

type NaturalJoin struct {
	tableReference
	LeftElement  *TableJoinElement
	Type         *JoinType
	RightElement *TableJoinElement
}

func (n *NaturalJoin) Pos() sqltoken.Pos {
	return n.LeftElement.Pos()
}

func (n *NaturalJoin) End() sqltoken.Pos {
	return n.RightElement.End()
}

func (n *NaturalJoin) ToSQLString() string {
	return fmt.Sprintf("%s NATURAL %sJOIN %s", n.LeftElement.ToSQLString(), n.Type.ToSQLString(), n.RightElement.ToSQLString())
}

//go:generate genmark -t JoinSpec -e Node

type NamedColumnsJoin struct {
	joinSpec
	ColumnList []*Ident
	Using      sqltoken.Pos
	RParen     sqltoken.Pos
}

func (n *NamedColumnsJoin) Pos() sqltoken.Pos {
	return n.Using
}

func (n *NamedColumnsJoin) End() sqltoken.Pos {
	return n.RParen
}

func (n *NamedColumnsJoin) ToSQLString() string {
	return fmt.Sprintf("USING (%s)", commaSeparatedString(n.ColumnList))
}

type JoinCondition struct {
	joinSpec
	SearchCondition Node
	On              sqltoken.Pos
}

func (j *JoinCondition) Pos() sqltoken.Pos {
	return j.On
}

func (j *JoinCondition) End() sqltoken.Pos {
	return j.SearchCondition.End()
}

func (j *JoinCondition) ToSQLString() string {
	return fmt.Sprintf("ON %s", j.SearchCondition.ToSQLString())
}

type JoinType struct {
	Condition JoinTypeCondition
	From, To  sqltoken.Pos
}

func (j *JoinType) Pos() sqltoken.Pos {
	return j.From
}

func (j *JoinType) End() sqltoken.Pos {
	return j.To
}

type JoinTypeCondition int

const (
	INNER JoinTypeCondition = iota
	LEFT
	RIGHT
	FULL
	LEFTOUTER
	RIGHTOUTER
	FULLOUTER
	IMPLICIT
)

func (j *JoinType) ToSQLString() string {
	switch j.Condition {
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

// ORDER BY Expr [ASC | DESC]
type OrderByExpr struct {
	Expr        Node
	OrderingPos sqltoken.Pos // ASC / DESC keyword position if ASC != nil
	ASC         *bool
}

func (o *OrderByExpr) Pos() sqltoken.Pos {
	return o.Expr.Pos()
}

func (o *OrderByExpr) End() sqltoken.Pos {
	if o.ASC != nil {
		return o.OrderingPos
	}

	return o.Expr.End()
}

func (o *OrderByExpr) ToSQLString() string {
	if o.ASC == nil {
		return o.Expr.ToSQLString()
	}
	if *o.ASC {
		return fmt.Sprintf("%s ASC", o.Expr.ToSQLString())
	}
	return fmt.Sprintf("%s DESC", o.Expr.ToSQLString())
}

// LIMIT [ALL | LimitValue ] [ OFFSET OffsetValue]
type LimitExpr struct {
	All         bool
	AllPos      sqltoken.Pos // ALL keyword position if All is true
	Limit       sqltoken.Pos // Limit keyword position
	LimitValue  *LongValue
	OffsetValue *LongValue
}

func (l *LimitExpr) Pos() sqltoken.Pos {
	return l.Limit
}

func (l *LimitExpr) End() sqltoken.Pos {
	if l.All {
		return l.AllPos
	}

	if l.OffsetValue != nil {
		return l.OffsetValue.To
	}
	return l.LimitValue.To
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
