package sqlast

import (
	"io"
	"log"

	"github.com/akito0107/xsqlparser/sqltoken"
)

//go:generate genmark -t Stmt -e Node

// Insert Statement
type InsertStmt struct {
	stmt
	Insert            sqltoken.Pos // first position of INSERT keyword
	TableName         *ObjectName
	Columns           []*Ident
	Source            InsertSource  // Insert Source [SubQuery or Constructor]
	UpdateAssignments []*Assignment // MySQL only (ON DUPLICATED KEYS)
}

func (i *InsertStmt) Pos() sqltoken.Pos {
	return i.Insert
}

func (i *InsertStmt) End() sqltoken.Pos {
	if len(i.UpdateAssignments) != 0 {
		return i.UpdateAssignments[len(i.UpdateAssignments)-1].End()
	}

	return i.Source.End()
}

func (i *InsertStmt) ToSQLString() string {
	return toSQLString(i)
}

func (i *InsertStmt) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Bytes([]byte("INSERT INTO ")).Node(i.TableName).Space()
	if len(i.Columns) != 0 {
		sw.LParen().Idents(i.Columns, []byte(", ")).RParen().Space()
	}
	sw.Node(i.Source)
	if len(i.UpdateAssignments) != 0 {
		sw.Bytes([]byte(" ON DUPLICATE KEY UPDATE "))
		for i, assignment := range i.UpdateAssignments {
			sw.JoinComma(i, assignment)
		}
	}
	return sw.End()
}

//go:generate genmark -t InsertSource -e Node

// SubQuery Source
type SubQuerySource struct {
	insertSource
	SubQuery *QueryStmt
}

func (s *SubQuerySource) Pos() sqltoken.Pos {
	return s.SubQuery.Pos()
}

func (s *SubQuerySource) End() sqltoken.Pos {
	return s.SubQuery.End()
}

func (s *SubQuerySource) ToSQLString() string {
	return toSQLString(s)
}

func (s *SubQuerySource) WriteTo(w io.Writer) (int64, error) {
	return s.SubQuery.WriteTo(w)
}

type ConstructorSource struct {
	insertSource
	Values sqltoken.Pos
	Rows   []*RowValueExpr
}

func (c *ConstructorSource) Pos() sqltoken.Pos {
	return c.Values
}

func (c *ConstructorSource) End() sqltoken.Pos {
	return c.Rows[len(c.Rows)-1].End()
}

func (c *ConstructorSource) ToSQLString() string {
	return toSQLString(c)
}

func (c *ConstructorSource) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Bytes([]byte("VALUES "))
	for i, row := range c.Rows {
		sw.JoinComma(i, row)
	}
	return sw.End()
}

type RowValueExpr struct {
	Values         []Node
	LParen, RParen sqltoken.Pos
}

func (r *RowValueExpr) Pos() sqltoken.Pos {
	return r.LParen
}

func (r *RowValueExpr) End() sqltoken.Pos {
	return r.RParen
}

func (r *RowValueExpr) ToSQLString() string {
	return toSQLString(r)
}

func (r *RowValueExpr) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.LParen()
	for i, val := range r.Values {
		sw.JoinComma(i, val)
	}
	sw.RParen()
	return sw.End()
}

// TODO Remove CopyStmt
type CopyStmt struct {
	stmt
	Copy      sqltoken.Pos
	TableName *ObjectName
	Columns   []*Ident
	Values    []*string
}

func (c *CopyStmt) Pos() sqltoken.Pos {
	return c.Copy
}

// TODO
func (c *CopyStmt) End() sqltoken.Pos {
	panic("not implemented")
}

func (c *CopyStmt) ToSQLString() string {
	return toSQLString(c)
}

func (c *CopyStmt) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Bytes([]byte("COPY ")).Node(c.TableName)
	if len(c.Columns) != 0 {
		sw.Space().LParen().Idents(c.Columns, []byte(", ")).RParen()
	}
	sw.Bytes([]byte(" FROM stdin; "))
	if len(c.Values) != 0 {
		sw.Bytes([]byte("\n"))
		for i, val := range c.Values {
			if i > 0 {
				sw.Bytes([]byte("\t"))
			}
			if val == nil {
				sw.Bytes([]byte("\\N"))
			} else {
				sw.Bytes([]byte(*val))
			}
		}
	}
	sw.Bytes([]byte("\n\\."))
	return sw.End()
}

type UpdateStmt struct {
	stmt
	Update      sqltoken.Pos
	TableName   *ObjectName
	Assignments []*Assignment
	Selection   Node
}

func (u *UpdateStmt) Pos() sqltoken.Pos {
	return u.Update
}

func (u *UpdateStmt) End() sqltoken.Pos {
	if u.Selection != nil {
		return u.Selection.End()
	}

	return u.Assignments[len(u.Assignments)-1].End()
}

func (u *UpdateStmt) ToSQLString() string {
	return toSQLString(u)
}

func (u *UpdateStmt) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Bytes([]byte("UPDATE ")).Node(u.TableName).Bytes([]byte(" SET "))
	if u.Assignments != nil {
		for i, assignment := range u.Assignments {
			sw.JoinComma(i, assignment)
		}
	}
	if u.Selection != nil {
		sw.Bytes([]byte(" WHERE ")).Node(u.Selection)
	}
	return sw.End()
}

type DeleteStmt struct {
	stmt
	Delete    sqltoken.Pos
	TableName *ObjectName
	Selection Node
}

func (d *DeleteStmt) Pos() sqltoken.Pos {
	return d.Delete
}

func (d *DeleteStmt) End() sqltoken.Pos {
	if d.Selection != nil {
		return d.Selection.End()
	}

	return d.TableName.End()
}

func (d *DeleteStmt) ToSQLString() string {
	return toSQLString(d)
}

func (d *DeleteStmt) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Bytes([]byte("DELETE FROM ")).Node(d.TableName)
	if d.Selection != nil {
		sw.Bytes([]byte(" WHERE ")).Node(d.Selection)
	}
	return sw.End()
}

type CreateViewStmt struct {
	stmt
	Create       sqltoken.Pos
	Name         *ObjectName
	Query        *QueryStmt
	Materialized bool
}

func (c *CreateViewStmt) Pos() sqltoken.Pos {
	return c.Create
}

func (c *CreateViewStmt) End() sqltoken.Pos {
	return c.Query.End()
}

func (c *CreateViewStmt) ToSQLString() string {
	return toSQLString(c)
}

func (c *CreateViewStmt) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).
		Bytes([]byte("CREATE")).
		If(c.Materialized, []byte(" MATERIALIZED")).
		Bytes([]byte(" VIEW ")).Node(c.Name).As().Node(c.Query).
		End()
}

type CreateTableStmt struct {
	stmt
	Create    sqltoken.Pos
	Name      *ObjectName
	Elements  []TableElement
	Location  *string
	NotExists bool
	Options   []TableOption
}

func (c *CreateTableStmt) Pos() sqltoken.Pos {
	return c.Create
}

func (c *CreateTableStmt) End() sqltoken.Pos {
	return c.Elements[len(c.Elements)-1].End()
}

func (c *CreateTableStmt) ToSQLString() string {
	return toSQLString(c)
}

func (c *CreateTableStmt) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Bytes([]byte("CREATE TABLE "))
	sw.If(c.NotExists, []byte("IF NOT EXISTS "))
	sw.Node(c.Name).Space().LParen()
	for i, element := range c.Elements {
		sw.JoinComma(i, element)
	}
	sw.RParen()
	if len(c.Options) != 0 {
		for i, option := range c.Options {
			sw.JoinComma(i, option)
		}
	}
	return sw.End()
}

type Assignment struct {
	ID    *Ident
	Value Node
}

func (a *Assignment) Pos() sqltoken.Pos {
	return a.ID.Pos()
}

func (a *Assignment) End() sqltoken.Pos {
	return a.Value.End()
}

func (a *Assignment) ToSQLString() string {
	return toSQLString(a)
}

func (a *Assignment) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).Node(a.ID).Bytes([]byte(" = ")).Node(a.Value).End()
}

//go:generate genmark -t TableElement -e Node

type TableConstraint struct {
	tableElement
	Constraint sqltoken.Pos
	Name       *Ident
	Spec       TableConstraintSpec
}

func (t *TableConstraint) Pos() sqltoken.Pos {
	if t.Name != nil {
		return t.Constraint
	}
	return t.Spec.Pos()
}

func (t *TableConstraint) End() sqltoken.Pos {
	return t.Spec.End()
}

func (t *TableConstraint) ToSQLString() string {
	return toSQLString(t)
}

func (t *TableConstraint) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	if t.Name != nil {
		sw.Bytes([]byte("CONSTRAINT ")).Node(t.Name).Space()
	}
	sw.Node(t.Spec)
	return sw.End()
}

//go:generate genmark -t TableConstraintSpec -e Node

type UniqueTableConstraint struct {
	tableConstraintSpec
	IsPrimary       bool
	Primary, Unique sqltoken.Pos
	RParen          sqltoken.Pos
	Columns         []*Ident
}

func (u *UniqueTableConstraint) Pos() sqltoken.Pos {
	if u.IsPrimary {
		return u.Primary
	}
	return u.Unique
}

func (u *UniqueTableConstraint) End() sqltoken.Pos {
	return u.RParen
}

func (u *UniqueTableConstraint) ToSQLString() string {
	return toSQLString(u)
}

func (u *UniqueTableConstraint) WriteTo(w io.Writer) (n int64, err error) {
	sw := newSQLWriter(w)
	if u.IsPrimary {
		sw.Bytes([]byte("PRIMARY KEY"))
	} else {
		sw.Bytes([]byte("UNIQUE"))
	}
	sw.LParen().Idents(u.Columns, []byte(", ")).RParen()
	return sw.End()
}

type ReferentialTableConstraint struct {
	tableConstraintSpec
	Foreign sqltoken.Pos
	Columns []*Ident
	KeyExpr *ReferenceKeyExpr
}

func (r *ReferentialTableConstraint) Pos() sqltoken.Pos {
	return r.Foreign
}

func (r *ReferentialTableConstraint) End() sqltoken.Pos {
	return r.KeyExpr.End()
}

func (r *ReferentialTableConstraint) ToSQLString() string {
	return toSQLString(r)
}

func (r *ReferentialTableConstraint) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).
		Bytes([]byte("FOREIGN KEY")).
		LParen().Idents(r.Columns, []byte(", ")).RParen().
		Bytes([]byte(" REFERENCES ")).Node(r.KeyExpr).
		End()
}

type ReferenceKeyExpr struct {
	TableName *Ident
	Columns   []*Ident
	RParen    sqltoken.Pos
}

func (r *ReferenceKeyExpr) Pos() sqltoken.Pos {
	return r.TableName.Pos()
}

func (r *ReferenceKeyExpr) End() sqltoken.Pos {
	return r.RParen
}

func (r *ReferenceKeyExpr) ToSQLString() string {
	return toSQLString(r)
}

func (r *ReferenceKeyExpr) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).
		Node(r.TableName).LParen().Idents(r.Columns, []byte(", ")).RParen().
		End()
}

type CheckTableConstraint struct {
	tableConstraintSpec
	Check  sqltoken.Pos
	RParen sqltoken.Pos
	Expr   Node
}

func (c *CheckTableConstraint) Pos() sqltoken.Pos {
	return c.Check
}

func (c *CheckTableConstraint) End() sqltoken.Pos {
	return c.RParen
}

func (c *CheckTableConstraint) ToSQLString() string {
	return toSQLString(c)
}

func (c *CheckTableConstraint) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).
		Bytes([]byte("CHECK")).LParen().Node(c.Expr).RParen().
		End()
}

type ColumnDef struct {
	tableElement
	Name                 *Ident
	DataType             Type
	Default              Node
	MyDataTypeDecoration []MyDataTypeDecoration // DataType Decoration for MySQL eg. AUTO_INCREMENT currently, only supports AUTO_INCREMENT
	Constraints          []*ColumnConstraint
}

func (c *ColumnDef) Pos() sqltoken.Pos {
	return c.Name.Pos()
}

func (c *ColumnDef) End() sqltoken.Pos {
	return c.Constraints[len(c.Constraints)-1].End()
}

func (c *ColumnDef) ToSQLString() string {
	return toSQLString(c)
}

func (c *ColumnDef) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Node(c.Name).Space().Node(c.DataType)
	if c.Default != nil {
		sw.Bytes([]byte(" DEFAULT ")).Node(c.Default)
	}
	for _, m := range c.MyDataTypeDecoration {
		sw.Space().Node(m)
	}
	for _, cons := range c.Constraints {
		sw.Node(cons)
	}
	return sw.End()
}

//go:generate genmark -t MyDataTypeDecoration -e Node

type AutoIncrement struct {
	myDataTypeDecoration
	Auto      sqltoken.Pos
	Increment sqltoken.Pos
}

func (a *AutoIncrement) ToSQLString() string {
	return "AUTO_INCREMENT"
}

func (a *AutoIncrement) WriteTo(w io.Writer) (int64, error) {
	return writeSingleBytes(w, []byte("AUTO_INCREMENT"))
}

func (a *AutoIncrement) Pos() sqltoken.Pos {
	return a.Auto
}

func (a *AutoIncrement) End() sqltoken.Pos {
	return a.Increment
}

type ColumnConstraint struct {
	Name       *Ident
	Constraint sqltoken.Pos
	Spec       ColumnConstraintSpec
}

func (c *ColumnConstraint) Pos() sqltoken.Pos {
	if c.Name == nil {
		return c.Constraint
	}
	return c.Name.Pos()
}

func (c *ColumnConstraint) End() sqltoken.Pos {
	return c.Spec.End()
}

func (c *ColumnConstraint) ToSQLString() string {
	return toSQLString(c)
}

func (c *ColumnConstraint) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Space()
	if c.Name != nil {
		sw.Bytes([]byte("CONSTRAINT ")).Node(c.Name).Space()
	}
	sw.Node(c.Spec)
	return sw.End()
}

// https://jakewheat.github.io/sql-overview/sql-2008-foundation-grammar.html#column-constraint
type ColumnConstraintSpec interface {
	Node
}

type NotNullColumnSpec struct {
	Not, Null sqltoken.Pos
}

func (n *NotNullColumnSpec) Pos() sqltoken.Pos {
	return n.Not
}

func (n *NotNullColumnSpec) End() sqltoken.Pos {
	return n.Null
}

func (*NotNullColumnSpec) ToSQLString() string {
	return "NOT NULL"
}

func (*NotNullColumnSpec) WriteTo(w io.Writer) (int64, error) {
	return writeSingleBytes(w, []byte("NOT NULL"))
}

type UniqueColumnSpec struct {
	IsPrimaryKey bool
	Primary, Key sqltoken.Pos
	Unique       sqltoken.Pos
}

func (u *UniqueColumnSpec) Pos() sqltoken.Pos {
	if u.IsPrimaryKey {
		return u.Primary
	}
	return u.Unique
}

func (u *UniqueColumnSpec) End() sqltoken.Pos {
	if u.IsPrimaryKey {
		return u.Key
	}
	return sqltoken.Pos{
		Line: u.Unique.Line,
		Col:  u.Unique.Col + 6,
	}
}

func (u *UniqueColumnSpec) ToSQLString() string {
	if u.IsPrimaryKey {
		return "PRIMARY KEY"
	} else {
		return "UNIQUE"
	}
}

func (u *UniqueColumnSpec) WriteTo(w io.Writer) (int64, error) {
	if u.IsPrimaryKey {
		return writeSingleBytes(w, []byte("PRIMARY KEY"))
	} else {
		return writeSingleBytes(w, []byte("UNIQUE"))
	}
}

type ReferencesColumnSpec struct {
	References sqltoken.Pos
	RParen     sqltoken.Pos
	TableName  *ObjectName
	Columns    []*Ident
}

func (r *ReferencesColumnSpec) Pos() sqltoken.Pos {
	return r.References
}

func (r *ReferencesColumnSpec) End() sqltoken.Pos {
	return r.RParen
}

func (r *ReferencesColumnSpec) ToSQLString() string {
	return toSQLString(r)
}

func (r *ReferencesColumnSpec) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Bytes([]byte("REFERENCES ")).Node(r.TableName)
	sw.LParen().Idents(r.Columns, []byte(", ")).RParen()
	return sw.End()
}

type CheckColumnSpec struct {
	Expr   Node
	Check  sqltoken.Pos
	RParen sqltoken.Pos
}

func (c *CheckColumnSpec) Pos() sqltoken.Pos {
	return c.Check
}

func (c *CheckColumnSpec) End() sqltoken.Pos {
	return c.RParen
}

func (c *CheckColumnSpec) ToSQLString() string {
	return toSQLString(c)
}

func (c *CheckColumnSpec) WriteTo(w io.Writer) (n int64, err error) {
	sw := newSQLWriter(w)
	sw.Bytes([]byte("CHECK")).LParen().Node(c.Expr).RParen()
	return sw.End()
}

//TODO remove
type FileFormat int

const (
	TEXTFILE FileFormat = iota
	SEQUENCEFILE
	ORC
	PARQUET
	AVRO
	RCFILE
	JSONFILE
)

func (f *FileFormat) ToSQLString() string {
	switch *f {
	case TEXTFILE:
		return "TEXTFILE"
	case SEQUENCEFILE:
		return "SEQUENCEFILE"
	case ORC:
		return "ORC"
	case PARQUET:
		return "PARQUET"
	case AVRO:
		return "AVRO"
	case RCFILE:
		return "RCFILE"
	case JSONFILE:
		return "JSONFILE"
	}
	return ""
}

func (FileFormat) FromStr(str string) FileFormat {
	switch str {
	case "TEXTFILE":
		return TEXTFILE
	case "SEQUENCEFILE":
		return SEQUENCEFILE
	case "ORC":
		return ORC
	case "PARQUET":
		return PARQUET
	case "AVRO":
		return AVRO
	case "RCFILE":
		return RCFILE
	case "JSONFILE":
		return JSONFILE
	}
	log.Panicf("unexpected file format %s", str)
	return 0
}

type AlterTableStmt struct {
	stmt
	Alter     sqltoken.Pos
	TableName *ObjectName
	Action    AlterTableAction
}

func (a *AlterTableStmt) Pos() sqltoken.Pos {
	return a.Alter
}

func (a *AlterTableStmt) End() sqltoken.Pos {
	return a.Action.End()
}

func (a *AlterTableStmt) ToSQLString() string {
	return toSQLString(a)
}

func (a *AlterTableStmt) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Bytes([]byte("ALTER TABLE ")).Node(a.TableName).Space().Node(a.Action)
	return sw.End()
}

//go:generate genmark -t AlterTableAction -e Node

type AddColumnTableAction struct {
	alterTableAction
	Add    sqltoken.Pos
	Column *ColumnDef
}

func (a *AddColumnTableAction) Pos() sqltoken.Pos {
	return a.Add
}

func (a *AddColumnTableAction) End() sqltoken.Pos {
	return a.Column.End()
}

func (a *AddColumnTableAction) ToSQLString() string {
	return toSQLString(a)
}

func (a *AddColumnTableAction) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Bytes([]byte("ADD COLUMN ")).Node(a.Column)
	return sw.End()
}

type AlterColumnTableAction struct {
	alterTableAction
	ColumnName *Ident
	Alter      sqltoken.Pos
	Action     AlterColumnAction
}

func (a *AlterColumnTableAction) Pos() sqltoken.Pos {
	return a.Alter
}

func (a *AlterColumnTableAction) End() sqltoken.Pos {
	return a.Action.End()
}

func (a *AlterColumnTableAction) ToSQLString() string {
	return toSQLString(a)
}

func (a *AlterColumnTableAction) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Bytes([]byte("ALTER COLUMN ")).Node(a.ColumnName).Space().Node(a.Action)
	return sw.End()
}

//go:generate genmark -t AlterColumnAction -e Node

// TODO add column scope / drop column scope / alter identity column spec
// https://jakewheat.github.io/sql-overview/sql-2008-foundation-grammar.html#alter-column-definition

type SetDefaultColumnAction struct {
	alterColumnAction
	Set     sqltoken.Pos
	Default Node
}

func (s *SetDefaultColumnAction) Pos() sqltoken.Pos {
	return s.Set
}

func (s *SetDefaultColumnAction) End() sqltoken.Pos {
	return s.Default.End()
}

func (s *SetDefaultColumnAction) ToSQLString() string {
	return toSQLString(s)
}

func (s *SetDefaultColumnAction) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).Bytes([]byte("SET DEFAULT ")).Node(s.Default).End()
}

type DropDefaultColumnAction struct {
	alterColumnAction
	Drop, Default sqltoken.Pos
}

func (d *DropDefaultColumnAction) Pos() sqltoken.Pos {
	return d.Drop
}

func (d *DropDefaultColumnAction) End() sqltoken.Pos {
	return d.Default
}

func (*DropDefaultColumnAction) ToSQLString() string {
	return "DROP DEFAULT"
}

func (d *DropDefaultColumnAction) WriteTo(w io.Writer) (int64, error) {
	return writeSingleBytes(w, []byte("DROP DEFAULT"))
}

// postgres only
type PGAlterDataTypeColumnAction struct {
	alterColumnAction
	Type     sqltoken.Pos
	DataType Type
}

func (p *PGAlterDataTypeColumnAction) Pos() sqltoken.Pos {
	return p.Type
}

func (p *PGAlterDataTypeColumnAction) End() sqltoken.Pos {
	return p.DataType.End()
}

func (p *PGAlterDataTypeColumnAction) ToSQLString() string {
	return toSQLString(p)
}

func (p *PGAlterDataTypeColumnAction) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).Bytes([]byte("TYPE ")).Node(p.DataType).End()
}

type PGSetNotNullColumnAction struct {
	alterColumnAction
	Set, Null sqltoken.Pos
}

func (p *PGSetNotNullColumnAction) Pos() sqltoken.Pos {
	return p.Set
}

func (p *PGSetNotNullColumnAction) End() sqltoken.Pos {
	return p.Null
}

func (p *PGSetNotNullColumnAction) ToSQLString() string {
	return "SET NOT NULL"
}

func (p *PGSetNotNullColumnAction) WriteTo(w io.Writer) (int64, error) {
	return writeSingleBytes(w, []byte("SET NOT NULL"))
}

type PGDropNotNullColumnAction struct {
	alterColumnAction
	Drop, Null sqltoken.Pos
}

func (p *PGDropNotNullColumnAction) Pos() sqltoken.Pos {
	return p.Drop
}

func (p *PGDropNotNullColumnAction) End() sqltoken.Pos {
	return p.Null
}

func (p *PGDropNotNullColumnAction) ToSQLString() string {
	return "DROP NOT NULL"
}

func (p *PGDropNotNullColumnAction) WriteTo(w io.Writer) (int64, error) {
	return writeSingleBytes(w, []byte("DROP NOT NULL"))
}

type RemoveColumnTableAction struct {
	alterTableAction
	Name       *Ident
	Cascade    bool
	CascadePos sqltoken.Pos
	Drop       sqltoken.Pos
}

func (r *RemoveColumnTableAction) Pos() sqltoken.Pos {
	return r.Drop
}

func (r *RemoveColumnTableAction) End() sqltoken.Pos {
	if r.Cascade {
		return r.CascadePos
	}
	return r.Name.End()
}

func (r *RemoveColumnTableAction) ToSQLString() string {
	return toSQLString(r)
}

func (r *RemoveColumnTableAction) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Bytes([]byte("DROP COLUMN ")).Node(r.Name).If(r.Cascade, []byte(" CASCADE"))
	return sw.End()
}

type AddConstraintTableAction struct {
	alterTableAction
	Add        sqltoken.Pos
	Constraint *TableConstraint
}

func (a *AddConstraintTableAction) Pos() sqltoken.Pos {
	return a.Add
}

func (a *AddConstraintTableAction) End() sqltoken.Pos {
	return a.Constraint.End()
}

func (a *AddConstraintTableAction) ToSQLString() string {
	return toSQLString(a)
}

func (a *AddConstraintTableAction) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).Bytes([]byte("ADD ")).Node(a.Constraint).End()
}

type DropConstraintTableAction struct {
	alterTableAction
	Name       *Ident
	Drop       sqltoken.Pos
	Cascade    bool
	CascadePos sqltoken.Pos
}

func (d *DropConstraintTableAction) Pos() sqltoken.Pos {
	return d.Drop
}

func (d *DropConstraintTableAction) End() sqltoken.Pos {
	if d.Cascade {
		return d.CascadePos
	}

	return d.Name.End()
}

func (d *DropConstraintTableAction) ToSQLString() string {
	return toSQLString(d)
}

func (d *DropConstraintTableAction) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Bytes([]byte("DROP CONSTRAINT ")).Node(d.Name).If(d.Cascade, []byte(" CASCADE"))
	return sw.End()
}

type DropTableStmt struct {
	stmt
	TableNames []*ObjectName
	Cascade    bool
	CascadePos sqltoken.Pos
	IfExists   bool
	Drop       sqltoken.Pos
}

func (d *DropTableStmt) Pos() sqltoken.Pos {
	return d.Drop
}

func (d *DropTableStmt) End() sqltoken.Pos {
	if d.Cascade {
		return d.CascadePos
	}

	return d.TableNames[len(d.TableNames)-1].End()
}

func (d *DropTableStmt) ToSQLString() string {
	return toSQLString(d)
}

func (d *DropTableStmt) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Bytes([]byte("DROP TABLE "))
	sw.If(d.IfExists, []byte("IF EXISTS "))
	for i, table := range d.TableNames {
		sw.JoinComma(i, table)
	}
	sw.If(d.Cascade, []byte(" CASCADE"))
	return sw.End()
}

type CreateIndexStmt struct {
	Create sqltoken.Pos
	stmt
	TableName   *ObjectName
	IsUnique    bool
	IndexName   *Ident
	MethodName  *Ident
	ColumnNames []*Ident
	RParen      sqltoken.Pos
	Selection   Node
}

func (c *CreateIndexStmt) Pos() sqltoken.Pos {
	return c.Create
}

func (c *CreateIndexStmt) End() sqltoken.Pos {
	if c.Selection != nil {
		return c.Selection.End()
	}

	return c.RParen
}

func (c *CreateIndexStmt) ToSQLString() string {
	return toSQLString(c)
}

func (c *CreateIndexStmt) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Bytes([]byte("CREATE ")).If(c.IsUnique, []byte("UNIQUE ")).Bytes([]byte("INDEX"))
	if c.IndexName != nil {
		sw.Space().Node(c.IndexName)
	}
	sw.Bytes([]byte(" ON ")).Node(c.TableName)
	if c.MethodName != nil {
		sw.Bytes([]byte(" USING ")).Node(c.MethodName)
	}
	sw.Space().LParen().Idents(c.ColumnNames, []byte(", ")).RParen()
	if c.Selection != nil {
		sw.Bytes([]byte(" WHERE ")).Node(c.Selection)
	}
	return sw.End()
}

type DropIndexStmt struct {
	stmt
	Drop       sqltoken.Pos
	IndexNames []*Ident
}

func (d *DropIndexStmt) Pos() sqltoken.Pos {
	return d.Drop
}

func (d *DropIndexStmt) End() sqltoken.Pos {
	return d.IndexNames[len(d.IndexNames)-1].End()
}

func (d *DropIndexStmt) ToSQLString() string {
	return toSQLString(d)
}

func (d *DropIndexStmt) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Bytes([]byte("DROP INDEX ")).Idents(d.IndexNames, []byte(", "))
	return sw.End()
}

type ExplainStmt struct {
	stmt
	Stmt    Stmt
	Explain sqltoken.Pos
}

func (e *ExplainStmt) Pos() sqltoken.Pos {
	return e.Explain
}

func (e *ExplainStmt) End() sqltoken.Pos {
	return e.Stmt.End()
}

func (e *ExplainStmt) ToSQLString() string {
	return toSQLString(e)
}

func (e *ExplainStmt) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).Bytes([]byte("EXPLAIN ")).Node(e.Stmt).End()
}
