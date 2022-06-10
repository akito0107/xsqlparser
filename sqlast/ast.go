/*
Package sqlast declares types that used to represent abstract syntax tree of sql.
Basically, these definitions are from https://github.com/andygrove/sqlparser-rs and https://jakewheat.github.io/sql-overview/sql-2008-foundation-grammar.html#_5_lexical_elements.
However, in some cases, the syntax is extended to support RDBMS specific syntax such as PGAlterTableAction.
*/
package sqlast

import (
	"io"

	errors "golang.org/x/xerrors"

	"github.com/akito0107/xsqlparser/sqltoken"
)

// AST Node interface. All node types implements this interface.
type Node interface {
	ToSQLString() string // convert Node as as sql valid string
	Pos() sqltoken.Pos   // position of first character belonging to the node
	End() sqltoken.Pos   // position of last character belonging to the node

	WriteTo(w io.Writer) (n int64, err error)
}

type File struct {
	Stmts    []Stmt
	Comments []*CommentGroup
}

func (f *File) End() sqltoken.Pos {

	if len(f.Comments) != 0 {
		if sqltoken.ComparePos(f.Comments[len(f.Comments)-1].End(), f.Stmts[len(f.Stmts)-1].End()) == 1 {
			return f.Comments[len(f.Comments)-1].End()
		}
	}

	return f.Stmts[len(f.Stmts)-1].End()
}

func (f *File) Pos() sqltoken.Pos {
	if len(f.Comments) != 0 {
		if sqltoken.ComparePos(f.Stmts[0].Pos(), f.Comments[0].Pos()) == 1 {
			return f.Comments[0].Pos()
		}
	}

	return f.Stmts[0].Pos()
}

func (f *File) ToSQLString() string {
	return toSQLString(f)
}

func (f *File) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	for i, stmt := range f.Stmts {
		sw.JoinNewLine(i, stmt)
	}
	return sw.End()
}

// Identifier
type Ident struct {
	Value    string
	From, To sqltoken.Pos
}

func NewIdent(str string) *Ident {
	return &Ident{Value: str}
}

func NewIdentWithPos(str string, from, to sqltoken.Pos) *Ident {
	return &Ident{
		Value: str,
		From:  from,
		To:    to,
	}
}

func (s *Ident) ToSQLString() string {
	return s.Value
}

func (s *Ident) Pos() sqltoken.Pos {
	return s.From
}

func (s *Ident) End() sqltoken.Pos {
	return s.To
}

func (s *Ident) WriteTo(w io.Writer) (int64, error) {
	return writeSingleString(w, s.Value)
}

func (s *Ident) WriteStringTo(w io.StringWriter) (int64, error) {
	n, err := w.WriteString(s.Value)
	return int64(n), err
}

// `*` Node.
type Wildcard struct {
	Wildcard sqltoken.Pos
}

func (s *Wildcard) Pos() sqltoken.Pos {
	return s.Wildcard
}

func (s *Wildcard) End() sqltoken.Pos {
	return sqltoken.Pos{
		Line: s.Wildcard.Line,
		Col:  s.Wildcard.Col + 1,
	}
}

func (s Wildcard) ToSQLString() string {
	return "*"
}

func (s *Wildcard) WriteTo(w io.Writer) (int64, error) {
	return writeSingleBytes(w, wildcardBytes)
}

// `table.*`, schema.table.*
type QualifiedWildcard struct {
	Idents []*Ident
}

func (s *QualifiedWildcard) Pos() sqltoken.Pos {
	return s.Idents[0].Pos()
}

func (s *QualifiedWildcard) End() sqltoken.Pos {
	return s.Idents[len(s.Idents)-1].End()
}

func (s *QualifiedWildcard) ToSQLString() string {
	return toSQLString(s)
}

func (s *QualifiedWildcard) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).Idents(s.Idents, []byte(".")).Bytes([]byte(".*")).End()
}

// table.column / schema.table.column
type CompoundIdent struct {
	Idents []*Ident
}

func (s *CompoundIdent) Pos() sqltoken.Pos {
	return s.Idents[0].Pos()
}

func (s *CompoundIdent) End() sqltoken.Pos {
	return s.Idents[len(s.Idents)-1].End()
}

func (s *CompoundIdent) ToSQLString() string {
	return toSQLString(s)
}

func (s *CompoundIdent) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).Idents(s.Idents, []byte(".")).End()
}

// ` X IS NULL`
type IsNull struct {
	X Node
}

func (s *IsNull) Pos() sqltoken.Pos {
	return s.X.Pos()
}

func (s *IsNull) End() sqltoken.Pos {
	return sqltoken.Pos{
		Line: s.X.End().Line,
		Col:  s.X.End().Col + 8,
	}
}

func (s *IsNull) ToSQLString() string {
	return toSQLString(s)
}

func (s *IsNull) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).Node(s.X).Bytes([]byte(" IS NULL")).End()
}

// `X IS NOT NULL`
type IsNotNull struct {
	X Node
}

func (s *IsNotNull) Pos() sqltoken.Pos {
	return s.X.Pos()
}

func (s *IsNotNull) End() sqltoken.Pos {
	return sqltoken.Pos{
		Line: s.X.End().Line,
		Col:  s.X.End().Col + 12,
	}
}

func (s *IsNotNull) ToSQLString() string {
	return toSQLString(s)
}

func (s *IsNotNull) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).Node(s.X).Bytes([]byte(" IS NOT NULL")).End()
}

// `Expr IN (List...)`
type InList struct {
	Expr    Node
	List    []Node
	Negated bool
	RParen  sqltoken.Pos
}

func (s *InList) Pos() sqltoken.Pos {
	return s.Expr.Pos()
}

func (s *InList) End() sqltoken.Pos {
	return s.RParen
}

func (s *InList) ToSQLString() string {
	return toSQLString(s)
}

func (s *InList) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).Node(s.Expr).Space().
		Negated(s.Negated).
		Bytes([]byte("IN ")).LParen().Nodes(s.List).RParen().
		End()
}

// `Expr [ NOT ] IN SubQuery`
type InSubQuery struct {
	Expr     Node
	SubQuery *QueryStmt
	Negated  bool
	RParen   sqltoken.Pos
}

func (s *InSubQuery) Pos() sqltoken.Pos {
	return s.Expr.Pos()
}

func (s *InSubQuery) End() sqltoken.Pos {
	return s.RParen
}

func (s *InSubQuery) ToSQLString() string {
	return toSQLString(s)
}

func (s *InSubQuery) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).Node(s.Expr).Space().
		Negated(s.Negated).
		Bytes([]byte("IN ")).LParen().Node(s.SubQuery).RParen().
		End()
}

// `Expr [ NOT ] BETWEEN [ LOW expr ] AND [ HIGH expr]`
type Between struct {
	Expr    Node
	Negated bool
	Low     Node
	High    Node
}

func (s *Between) Pos() sqltoken.Pos {
	return s.Expr.Pos()
}

func (s *Between) End() sqltoken.Pos {
	return s.High.End()
}

func (s *Between) ToSQLString() string {
	return toSQLString(s)
}

func (s *Between) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).Node(s.Expr).Space().
		Negated(s.Negated).
		Bytes([]byte("BETWEEN ")).Node(s.Low).Bytes([]byte(" AND ")).Node(s.High).
		End()
}

// `Left Op Right`
type BinaryExpr struct {
	Left  Node
	Op    *Operator
	Right Node
}

func (s *BinaryExpr) Pos() sqltoken.Pos {
	return s.Left.Pos()
}

func (s *BinaryExpr) End() sqltoken.Pos {
	return s.Right.End()
}

func (s *BinaryExpr) ToSQLString() string {
	return toSQLString(s)
}

func (s *BinaryExpr) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Direct(s.Left.WriteTo(w))
	sw.Space()
	if sw.Err() == nil {
		sw.Direct(s.Op.WriteTo(w))
	}
	sw.Space()
	if sw.Err() == nil {
		sw.Direct(s.Right.WriteTo(w))
	}
	return sw.End()
}

// `CAST(Expr AS DataType)`
type Cast struct {
	Expr     Node
	DataType Type
	Cast     sqltoken.Pos // first position of CAST token
	RParen   sqltoken.Pos
}

func (s *Cast) Pos() sqltoken.Pos {
	return s.Cast
}

func (s *Cast) End() sqltoken.Pos {
	return s.RParen
}

func (s *Cast) ToSQLString() string {
	return toSQLString(s)
}

func (s *Cast) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).
		Bytes([]byte("CAST")).
		LParen().
		Node(s.Expr).As().Node(s.DataType).
		RParen().
		End()
}

// (AST)
type Nested struct {
	AST            Node
	LParen, RParen sqltoken.Pos
}

func (s *Nested) Pos() sqltoken.Pos {
	return s.LParen
}

func (s *Nested) End() sqltoken.Pos {
	return s.RParen
}

func (s *Nested) ToSQLString() string {
	return toSQLString(s)
}

func (s *Nested) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).LParen().Node(s.AST).RParen().End()
}

// Op Expr
type UnaryExpr struct {
	From sqltoken.Pos // first position of Op
	Op   *Operator
	Expr Node
}

func (s *UnaryExpr) Pos() sqltoken.Pos {
	return s.From
}

func (s *UnaryExpr) End() sqltoken.Pos {
	return s.Expr.End()
}

func (s *UnaryExpr) ToSQLString() string {
	return toSQLString(s)
}

func (s *UnaryExpr) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).Node(s.Op).Space().Node(s.Expr).End()
}

// Name(Args...) [OVER (Over)]
type Function struct {
	Name       *ObjectName // Function Name
	Args       []Node
	Filter     *Ident
	ArgsRParen sqltoken.Pos // function args RParen position
	Over       *WindowSpec
	OverRparen sqltoken.Pos // Over RParen position (if Over is not nil)
}

func (s *Function) Pos() sqltoken.Pos {
	return s.Name.Pos()
}

func (s *Function) End() sqltoken.Pos {
	if s.Over == nil {
		return s.ArgsRParen
	}
	return s.OverRparen
}

func (s *Function) ToSQLString() string {
	return toSQLString(s)
}

func (s *Function) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Node(s.Name).LParen().Nodes(s.Args).RParen()
	if s.Over != nil {
		sw.Bytes([]byte(" OVER ")).LParen().Node(s.Over).RParen()
	}
	return sw.End()
}

// CASE [Operand] WHEN Conditions... THEN Results... [ELSE ElseResult] END
type CaseExpr struct {
	Case       sqltoken.Pos // first position of CASE keyword
	CaseEnd    sqltoken.Pos // Last position of END keyword
	Operand    Node
	Conditions []Node
	Results    []Node
	ElseResult Node
}

func (s *CaseExpr) Pos() sqltoken.Pos {
	return s.Case
}

func (s *CaseExpr) End() sqltoken.Pos {
	return s.CaseEnd
}

func (s *CaseExpr) ToSQLString() string {
	return toSQLString(s)
}

func (s *CaseExpr) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Bytes([]byte("CASE"))
	if s.Operand != nil {
		sw.Space().Node(s.Operand)
	}
	for i := 0; i < len(s.Conditions); i++ {
		sw.Bytes([]byte(" WHEN ")).Node(s.Conditions[i])
		sw.Bytes([]byte(" THEN ")).Node(s.Results[i])
	}
	if s.ElseResult != nil {
		sw.Bytes([]byte(" ELSE ")).Node(s.ElseResult)
	}
	sw.Bytes([]byte(" END"))
	return sw.End()
}

// [ NOT ] EXISTS (QueryStmt)
type Exists struct {
	Negated bool
	Query   *QueryStmt
	Not     sqltoken.Pos // first position of NOT keyword when Negated is true
	Exists  sqltoken.Pos // first position of EXISTS keyword
	RParen  sqltoken.Pos
}

func (s *Exists) Pos() sqltoken.Pos {
	if s.Negated {
		return s.Not
	}
	return s.Exists
}

func (s *Exists) End() sqltoken.Pos {
	return s.RParen
}

func (s *Exists) ToSQLString() string {
	return toSQLString(s)
}

func (s *Exists) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).
		Negated(s.Negated).Bytes([]byte("EXISTS ")).LParen().Node(s.Query).RParen().
		End()
}

// (QueryStmt)
type SubQuery struct {
	RParen, LParen sqltoken.Pos
	Query          *QueryStmt
}

func (s *SubQuery) Pos() sqltoken.Pos {
	return s.RParen
}

func (s *SubQuery) End() sqltoken.Pos {
	return s.LParen
}

func (s *SubQuery) ToSQLString() string {
	return toSQLString(s)
}

func (s *SubQuery) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).LParen().Node(s.Query).RParen().End()
}

// Table Names (ex public.table_name)
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

func (s *ObjectName) Pos() sqltoken.Pos {
	return s.Idents[0].Pos()
}

func (s *ObjectName) End() sqltoken.Pos {
	return s.Idents[len(s.Idents)-1].End()
}

func (s *ObjectName) ToSQLString() string {
	return toSQLString(s)
}

func (s *ObjectName) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).Idents(s.Idents, dotBytes).End()
}

type WindowSpec struct {
	PartitionBy      []Node
	OrderBy          []*OrderByExpr
	WindowsFrame     *WindowFrame
	Partition, Order sqltoken.Pos
}

func (s *WindowSpec) Pos() sqltoken.Pos {
	if len(s.PartitionBy) != 0 {
		return s.Partition
	}
	if len(s.OrderBy) != 0 {
		return s.Order
	}

	return s.WindowsFrame.Pos()
}

func (s *WindowSpec) End() sqltoken.Pos {
	if s.WindowsFrame != nil {
		return s.WindowsFrame.End()
	}

	if len(s.OrderBy) != 0 {
		return s.OrderBy[len(s.OrderBy)-1].End()
	}

	return s.PartitionBy[len(s.PartitionBy)-1].End()
}

func (s *WindowSpec) ToSQLString() string {
	return toSQLString(s)
}

func (s *WindowSpec) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	space := false
	if len(s.PartitionBy) != 0 {
		space = true
		sw.Bytes([]byte("PARTITION BY ")).Nodes(s.PartitionBy)
	}
	if len(s.OrderBy) != 0 {
		if space {
			sw.Space()
		} else {
			space = true
		}
		sw.Bytes([]byte("ORDER BY "))
		for i, order := range s.OrderBy {
			sw.JoinComma(i, order)
		}
	}
	if s.WindowsFrame != nil {
		if space {
			sw.Space()
		}
		sw.Node(s.WindowsFrame)
	}
	return sw.End()
}

type WindowFrame struct {
	Units      *WindowFrameUnit
	StartBound SQLWindowFrameBound
	EndBound   SQLWindowFrameBound
}

func (s *WindowFrame) Pos() sqltoken.Pos {
	return s.Units.From
}

func (s *WindowFrame) End() sqltoken.Pos {
	if s.EndBound != nil {
		return s.EndBound.End()
	}

	return s.StartBound.End()
}

func (s *WindowFrame) ToSQLString() string {
	return toSQLString(s)
}

func (s *WindowFrame) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	if s.EndBound != nil {
		return sw.Node(s.Units).Bytes([]byte(" BETWEEN ")).
			Node(s.StartBound).Bytes([]byte(" AND ")).Node(s.EndBound).
			End()
	} else {
		return sw.Node(s.Units).Space().Node(s.StartBound).End()
	}
}

type WindowFrameUnit struct {
	From, To sqltoken.Pos
	Type     WindowFrameUnitType
}

func (s *WindowFrameUnit) Pos() sqltoken.Pos {
	return s.From
}

func (s *WindowFrameUnit) End() sqltoken.Pos {
	return s.To
}

type WindowFrameUnitType int

const (
	RowsUnit WindowFrameUnitType = iota
	RangeUnit
	GroupsUnit
)

func (s *WindowFrameUnit) ToSQLString() string {
	switch s.Type {
	case RowsUnit:
		return "ROWS"
	case RangeUnit:
		return "RANGE"
	case GroupsUnit:
		return "GROUPS"
	}
	return ""
}

func (s *WindowFrameUnit) WriteTo(w io.Writer) (int64, error) {
	switch s.Type {
	case RowsUnit:
		return writeSingleBytes(w, []byte("ROWS"))
	case RangeUnit:
		return writeSingleBytes(w, []byte("RANGE"))
	case GroupsUnit:
		return writeSingleBytes(w, []byte("GROUPS"))
	}
	return 0, nil
}

func (WindowFrameUnit) FromStr(str string) (*WindowFrameUnit, error) {
	if str == "ROWS" {
		return &WindowFrameUnit{
			Type: RowsUnit,
		}, nil
	} else if str == "RANGE" {
		return &WindowFrameUnit{
			Type: RangeUnit,
		}, nil
	} else if str == "GROUPS" {
		return &WindowFrameUnit{
			Type: GroupsUnit,
		}, nil
	}
	return nil, errors.Errorf("expected ROWS, RANGE, GROUPS but: %s", str)
}

//go:generate genmark -t SQLWindowFrameBound -e Node

type CurrentRow struct {
	sqlWindowFrameBound
	Current sqltoken.Pos
	Row     sqltoken.Pos
}

func (c *CurrentRow) Pos() sqltoken.Pos {
	return c.Current
}

func (c *CurrentRow) End() sqltoken.Pos {
	return c.Row
}

func (*CurrentRow) ToSQLString() string {
	return "CURRENT ROW"
}

func (c *CurrentRow) WriteTo(w io.Writer) (int64, error) {
	return writeSingleBytes(w, []byte("CURRENT ROW"))
}

type UnboundedPreceding struct {
	sqlWindowFrameBound
	Unbounded sqltoken.Pos // first char position of UNBOUND
	Preceding sqltoken.Pos // last char position of PRECEDING
}

func (u *UnboundedPreceding) Pos() sqltoken.Pos {
	return u.Unbounded
}

func (u *UnboundedPreceding) End() sqltoken.Pos {
	return u.Preceding
}

func (*UnboundedPreceding) ToSQLString() string {
	return "UNBOUNDED PRECEDING"
}

func (u *UnboundedPreceding) WriteTo(w io.Writer) (int64, error) {
	return writeSingleBytes(w, []byte("UNBOUNDED PRECEDING"))
}

type UnboundedFollowing struct {
	sqlWindowFrameBound
	Unbounded sqltoken.Pos // first char position of UNBOUND
	Following sqltoken.Pos // last char position of FOLLOWING
}

func (u *UnboundedFollowing) Pos() sqltoken.Pos {
	return u.Unbounded
}

func (u *UnboundedFollowing) End() sqltoken.Pos {
	return u.Following
}

func (*UnboundedFollowing) ToSQLString() string {
	return "UNBOUNDED FOLLOWING"
}

func (u *UnboundedFollowing) WriteTo(w io.Writer) (int64, error) {
	return writeSingleBytes(w, []byte("UNBOUNDED FOLLOWING"))
}

// `Bound PRECEDING`
type Preceding struct {
	sqlWindowFrameBound
	Bound     *uint64
	From      sqltoken.Pos // first char position of Bound
	Preceding sqltoken.Pos // last char position of PRECEDING
}

func (p *Preceding) Pos() sqltoken.Pos {
	return p.From
}

func (p *Preceding) End() sqltoken.Pos {
	return p.Preceding
}

func (p *Preceding) ToSQLString() string {
	return toSQLString(p)
}

func (p *Preceding) WriteTo(w io.Writer) (n int64, err error) {
	return newSQLWriter(w).Int(int(*p.Bound)).Bytes([]byte(" PRECEDING")).End()
}

// `Bound FOLLOWING`
type Following struct {
	sqlWindowFrameBound
	From      sqltoken.Pos // first char position of Bound
	Following sqltoken.Pos // last char position of FOLLOWING
	Bound     *uint64
}

func (f *Following) Pos() sqltoken.Pos {
	return f.From
}

func (f *Following) End() sqltoken.Pos {
	return f.Following
}

func (f *Following) ToSQLString() string {
	return toSQLString(f)
}

func (f *Following) WriteTo(w io.Writer) (n int64, err error) {
	return newSQLWriter(w).Int(int(*f.Bound)).Bytes([]byte(" FOLLOWING")).End()
}
