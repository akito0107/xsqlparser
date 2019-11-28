/*
Package sqlast declares types that used to represent abstract syntax tree of sql.
Basically, these definitions are from https://github.com/andygrove/sqlparser-rs and https://jakewheat.github.io/sql-overview/sql-2008-foundation-grammar.html#_5_lexical_elements.
However, in some cases, the syntax is extended to support RDBMS specific syntax such as PGAlterTableAction.
*/
package sqlast

import (
	"fmt"
	"log"
	"strings"

	errors "golang.org/x/xerrors"

	"github.com/akito0107/xsqlparser/sqltoken"
)

// AST Node interface. All node types implements this interface.
type Node interface {
	ToSQLString() string // convert Node as as sql valid string
	Pos() sqltoken.Pos   // position of first character belonging to the node
	End() sqltoken.Pos   // position of last character belonging to the node
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
	sqls := make([]string, len(f.Stmts))

	for i, stmt := range f.Stmts {
		sqls[i] += stmt.ToSQLString()
	}

	return strings.Join(sqls, "\n")
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
	strs := make([]string, 0, len(s.Idents))
	for _, ident := range s.Idents {
		strs = append(strs, ident.ToSQLString())
	}
	return fmt.Sprintf("%s.*", strings.Join(strs, "."))
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
	strs := make([]string, 0, len(s.Idents))
	for _, ident := range s.Idents {
		strs = append(strs, ident.ToSQLString())
	}
	return strings.Join(strs, ".")
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
	return fmt.Sprintf("%s IS NULl", s.X.ToSQLString())
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
	return fmt.Sprintf("%s IS NOT NULL", s.X.ToSQLString())
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
	return fmt.Sprintf("%s %sIN (%s)", s.Expr.ToSQLString(), negatedString(s.Negated), commaSeparatedString(s.List))
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
	return fmt.Sprintf("%s %sIN (%s)", s.Expr.ToSQLString(), negatedString(s.Negated), s.SubQuery.ToSQLString())
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
	return fmt.Sprintf("%s %sBETWEEN %s AND %s", s.Expr.ToSQLString(), negatedString(s.Negated), s.Low.ToSQLString(), s.High.ToSQLString())
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
	return fmt.Sprintf("%s %s %s", s.Left.ToSQLString(), s.Op.ToSQLString(), s.Right.ToSQLString())
}

// `CAST(Expr AS DataType)`
type Cast struct {
	Expr     Node
	DateType Type
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
	return fmt.Sprintf("CAST(%s AS %s)", s.Expr.ToSQLString(), s.DateType.ToSQLString())
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
	return fmt.Sprintf("(%s)", s.AST.ToSQLString())
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
	return fmt.Sprintf("%s %s", s.Op.ToSQLString(), s.Expr.ToSQLString())
}

// Name(Args...) [OVER (Over)]
type Function struct {
	Name       *ObjectName // Function Name
	Args       []Node
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
	str := fmt.Sprintf("%s(%s)", s.Name.ToSQLString(), commaSeparatedString(s.Args))

	if s.Over != nil {
		str += fmt.Sprintf(" OVER (%s)", s.Over.ToSQLString())
	}

	return str
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
	return fmt.Sprintf("%sEXISTS (%s)", negatedString(s.Negated), s.Query.ToSQLString())
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
	return fmt.Sprintf("(%s)", s.Query.ToSQLString())
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
	var strs []string
	for _, l := range s.Idents {
		strs = append(strs, l.ToSQLString())
	}
	return strings.Join(strs, ".")
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
	if s.EndBound != nil {
		return fmt.Sprintf("%s BETWEEN %s AND %s", s.Units.ToSQLString(), s.StartBound.ToSQLString(), s.EndBound.ToSQLString())
	} else {
		return fmt.Sprintf("%s %s", s.Units.ToSQLString(), s.StartBound.ToSQLString())
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
	return fmt.Sprintf("%d PRECEDING", *p.Bound)
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
	return fmt.Sprintf("%d FOLLOWING", *f.Bound)
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
	case []TableOption:
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
