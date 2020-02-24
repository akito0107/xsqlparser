package sqlast

import (
	"io"
	"log"

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
	return toSQLString(q)
}

func (q *QueryStmt) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	if len(q.CTEs) != 0 {
		sw.Bytes([]byte("WITH "))
		for i, cte := range q.CTEs {
			sw.JoinComma(i, cte)
		}
		sw.Space()
	}
	if sw.Err() == nil {
		sw.Direct(q.Body.WriteTo(w))
	}
	if len(q.OrderBy) != 0 {
		sw.Bytes([]byte(" ORDER BY "))
		for i, col := range q.OrderBy {
			sw.JoinComma(i, col)
		}
	}
	if q.Limit != nil {
		sw.Space().Node(q.Limit)
	}
	return sw.End()
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
	return toSQLString(c)
}

func (c *CTE) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).
		Node(c.Alias).As().LParen().Node(c.Query).RParen().
		End()
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
	return toSQLString(s)
}

func (s *SelectExpr) WriteTo(w io.Writer) (int64, error) {
	return s.Select.WriteTo(w)
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
	return toSQLString(q)
}

func (q *QueryExpr) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).LParen().Node(q.Query).RParen().End()
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
	return toSQLString(s)
}

func (s *SetOperationExpr) WriteTo(w io.Writer) (n int64, err error) {
	return newSQLWriter(w).
		Node(s.Left).Space().Node(s.Op).If(s.All, []byte(" ALL")).Space().Node(s.Right).
		End()
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

func (u *UnionOperator) WriteTo(w io.Writer) (int64, error) {
	return writeSingleBytes(w, []byte("UNION"))
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

func (e *ExceptOperator) WriteTo(w io.Writer) (n int64, err error) {
	return writeSingleBytes(w, []byte("EXCEPT"))
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

func (i *IntersectOperator) WriteTo(w io.Writer) (n int64, err error) {
	return writeSingleBytes(w, []byte("INTERSECT"))
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
	return toSQLString(s)
}

func (s *SQLSelect) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Bytes(selectBytes)
	if s.Distinct {
		sw.Bytes([]byte("DISTINCT "))
	}
	for i, projection := range s.Projection {
		sw.JoinComma(i, projection)
	}
	if len(s.FromClause) != 0 {
		sw.Bytes(fromBytes)
		for i, from := range s.FromClause {
			sw.JoinComma(i, from)
		}
	}
	if s.WhereClause != nil {
		sw.Bytes(whereBytes)
		if sw.Err() == nil {
			sw.Direct(s.WhereClause.WriteTo(w))
		}
	}
	if len(s.GroupByClause) != 0 {
		sw.Bytes([]byte(" GROUP BY ")).Nodes(s.GroupByClause)
	}
	if s.HavingClause != nil {
		sw.Bytes([]byte(" HAVING ")).Node(s.HavingClause)
	}
	return sw.End()
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
	return toSQLString(t)
}

func (t *Table) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Node(t.Name)
	if len(t.Args) != 0 {
		sw.LParen().Nodes(t.Args).RParen()
	}
	if t.Alias != nil {
		sw.As().Node(t.Alias)
	}
	if len(t.WithHints) != 0 {
		sw.Bytes([]byte(" WITH ")).LParen().Nodes(t.WithHints).RParen()
	}
	return sw.End()
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
	return toSQLString(d)
}

func (d *Derived) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.If(d.Lateral, []byte("LATERAL "))
	sw.LParen().Node(d.SubQuery).RParen()
	if d.Alias != nil {
		sw.As().Node(d.Alias)
	}
	return sw.End()
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
	return toSQLString(u)
}

func (u *UnnamedSelectItem) WriteTo(w io.Writer) (int64, error) {
	return u.Node.WriteTo(w)
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
	return toSQLString(a)
}

func (a *AliasSelectItem) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).Node(a.Expr).As().Node(a.Alias).End()
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
	return toSQLString(q)
}

func (q *QualifiedWildcardSelectItem) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).Node(q.Prefix).Bytes([]byte(".*")).End()
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

func (*WildcardSelectItem) WriteTo(w io.Writer) (int64, error) {
	return writeSingleBytes(w, []byte("*"))
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
	return toSQLString(c)
}

func (c *CrossJoin) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).
		Node(c.Reference).Bytes([]byte(" CROSS JOIN ")).Node(c.Factor).
		End()
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
	return toSQLString(t)
}

func (t *TableJoinElement) WriteTo(w io.Writer) (int64, error) {
	return t.Ref.WriteTo(w)
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
	return toSQLString(p)
}

func (p *PartitionedJoinTable) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).
		Node(p.Factor).Bytes([]byte(" PARTITION BY ")).
		LParen().Idents(p.ColumnList, []byte(", ")).RParen().
		End()
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
	return toSQLString(q)
}

func (q *QualifiedJoin) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).
		Node(q.LeftElement).Space().
		Node(q.Type).Bytes([]byte("JOIN ")).
		Node(q.RightElement).Space().Node(q.Spec).
		End()
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
	return toSQLString(n)
}

func (n *NaturalJoin) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).
		Node(n.LeftElement).
		Bytes([]byte(" NATURAL ")).Node(n.Type).Bytes([]byte("JOIN ")).
		Node(n.RightElement).
		End()
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
	return toSQLString(n)
}

func (n *NamedColumnsJoin) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).
		Bytes([]byte("USING ")).
		LParen().Idents(n.ColumnList, []byte(", ")).RParen().
		End()
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
	return toSQLString(j)
}

func (j *JoinCondition) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).Bytes([]byte("ON ")).Node(j.SearchCondition).End()
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
		log.Panicf("unknown join type %d", j)
	}
	return ""
}

func (j *JoinType) WriteTo(w io.Writer) (int64, error) {
	return writeSingleBytes(w, []byte(j.ToSQLString()))
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
	return toSQLString(o)
}

func (o *OrderByExpr) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Node(o.Expr)
	if o.ASC != nil {
		if *o.ASC {
			sw.Bytes([]byte(" ASC"))
		} else {
			sw.Bytes([]byte(" DESC"))
		}
	}
	return sw.End()
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
	return toSQLString(l)
}

func (l *LimitExpr) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Bytes([]byte("LIMIT "))
	if l.All {
		sw.Bytes([]byte("ALL"))
	} else {
		sw.Node(l.LimitValue)
	}
	if l.OffsetValue != nil {
		sw.Bytes([]byte(" OFFSET ")).Node(l.OffsetValue)
	}
	return sw.End()
}
