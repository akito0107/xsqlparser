package sqlast

import (
	"fmt"
	"log"
	"strings"

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
	str := fmt.Sprintf("INSERT INTO %s ", i.TableName.ToSQLString())
	if len(i.Columns) != 0 {
		str += fmt.Sprintf("(%s) ", commaSeparatedString(i.Columns))
	}

	str += i.Source.ToSQLString()

	if len(i.UpdateAssignments) != 0 {
		str += " ON DUPLICATE KEY UPDATE " + commaSeparatedString(i.UpdateAssignments)
	}

	return str
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
	return s.SubQuery.ToSQLString()
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
	str := "VALUES "

	for idx, r := range c.Rows {
		str += r.ToSQLString()
		if idx != len(c.Rows)-1 {
			str += ", "
		}
	}

	return str
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
	return fmt.Sprintf("(%s)", commaSeparatedString(r.Values))
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
	str := fmt.Sprintf("COPY %s", c.TableName.ToSQLString())
	if len(c.Columns) != 0 {
		str += fmt.Sprintf(" (%s)", commaSeparatedString(c.Columns))
	}
	str += " FROM stdin; "

	if len(c.Values) != 0 {
		var valuestrs []string
		for _, v := range c.Values {
			if v == nil {
				valuestrs = append(valuestrs, "\\N")
			} else {
				valuestrs = append(valuestrs, *v)
			}
		}
		str += fmt.Sprintf("\n%s", strings.Join(valuestrs, "\t"))
	}
	str += "\n\\."

	return str
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
	str := fmt.Sprintf("UPDATE %s SET ", u.TableName.ToSQLString())
	if u.Assignments != nil {
		str += commaSeparatedString(u.Assignments)
	}
	if u.Selection != nil {
		str += fmt.Sprintf(" WHERE %s", u.Selection.ToSQLString())
	}

	return str
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
	str := fmt.Sprintf("DELETE FROM %s", d.TableName.ToSQLString())

	if d.Selection != nil {
		str += fmt.Sprintf(" WHERE %s", d.Selection.ToSQLString())
	}

	return str
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
	var modifier string
	if c.Materialized {
		modifier = " MATERIALIZED"
	}
	return fmt.Sprintf("CREATE%s VIEW %s AS %s", modifier, c.Name.ToSQLString(), c.Query.ToSQLString())
}

type CreateTableStmt struct {
	stmt
	Create    sqltoken.Pos
	Name      *ObjectName
	Elements  []TableElement
	Location  *string
	NotExists bool
}

func (c *CreateTableStmt) Pos() sqltoken.Pos {
	return c.Create
}

func (c *CreateTableStmt) End() sqltoken.Pos {
	return c.Elements[len(c.Elements)-1].End()
}

func (c *CreateTableStmt) ToSQLString() string {
	ifNotExists := ""
	if c.NotExists {
		ifNotExists = "IF NOT EXISTS "
	}
	return fmt.Sprintf("CREATE TABLE %s%s (%s)", ifNotExists, c.Name.ToSQLString(), commaSeparatedString(c.Elements))
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
	return fmt.Sprintf("%s = %s", a.ID.ToSQLString(), a.Value.ToSQLString())
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
	var str string

	if t.Name != nil {
		str += fmt.Sprintf("CONSTRAINT %s ", t.Name.ToSQLString())
	}

	str += t.Spec.ToSQLString()

	return str
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
	if u.IsPrimary {
		return fmt.Sprintf("PRIMARY KEY(%s)", commaSeparatedString(u.Columns))
	}
	return fmt.Sprintf("UNIQUE(%s)", commaSeparatedString(u.Columns))
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
	return fmt.Sprintf("FOREIGN KEY(%s) REFERENCES %s", commaSeparatedString(r.Columns), r.KeyExpr.ToSQLString())
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
	return fmt.Sprintf("%s(%s)", r.TableName.ToSQLString(), commaSeparatedString(r.Columns))
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
	return fmt.Sprintf("CHECK(%s)", c.Expr.ToSQLString())
}

type ColumnDef struct {
	tableElement
	Name        *Ident
	DataType    Type
	Default     Node
	Constraints []*ColumnConstraint
}

func (c *ColumnDef) Pos() sqltoken.Pos {
	return c.Name.Pos()
}

func (c *ColumnDef) End() sqltoken.Pos {
	return c.Constraints[len(c.Constraints)-1].End()
}

func (c *ColumnDef) ToSQLString() string {
	str := fmt.Sprintf("%s %s", c.Name.ToSQLString(), c.DataType.ToSQLString())
	if c.Default != nil {
		str += fmt.Sprintf(" DEFAULT %s", c.Default.ToSQLString())
	}

	for _, cons := range c.Constraints {
		str += cons.ToSQLString()
	}
	return str
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
	s := " "
	if c.Name != nil {
		s += fmt.Sprintf("CONSTRAINT %s ", c.Name.ToSQLString())
	}
	return s + c.Spec.ToSQLString()
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
	return fmt.Sprintf("NOT NULL")
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
		return fmt.Sprintf("PRIMARY KEY")
	} else {
		return fmt.Sprintf("UNIQUE")
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
	return fmt.Sprintf("REFERENCES %s(%s)", r.TableName.ToSQLString(), commaSeparatedString(r.Columns))
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
	return fmt.Sprintf("CHECK(%s)", c.Expr.ToSQLString())
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
	log.Fatalf("unexpected file format %s", str)
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
	return fmt.Sprintf("ALTER TABLE %s %s", a.TableName.ToSQLString(), a.Action.ToSQLString())
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
	return fmt.Sprintf("ADD COLUMN %s", a.Column.ToSQLString())
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
	return fmt.Sprintf("ALTER COLUMN %s %s", a.ColumnName.ToSQLString(), a.Action.ToSQLString())
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
	return fmt.Sprintf("SET DEFAULT %s", s.Default.ToSQLString())
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
	return fmt.Sprintf("TYPE %s", p.DataType.ToSQLString())
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
	var cascade string
	if r.Cascade {
		cascade += " CASCADE"
	}
	return fmt.Sprintf("DROP COLUMN %s%s", r.Name.ToSQLString(), cascade)
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
	return fmt.Sprintf("ADD %s", a.Constraint.ToSQLString())
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
	var cascade string
	if d.Cascade {
		cascade += " CASCADE"
	}
	return fmt.Sprintf("DROP CONSTRAINT %s%s", d.Name.ToSQLString(), cascade)
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
	var ifexists string
	if d.IfExists {
		ifexists = "IF EXISTS "
	}

	var cascade string
	if d.Cascade {
		cascade = " CASCADE"
	}

	return fmt.Sprintf("DROP TABLE %s%s%s", ifexists, commaSeparatedString(d.TableNames), cascade)
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
	var uniqueStr string
	if c.IsUnique {
		uniqueStr = "UNIQUE "
	}
	str := fmt.Sprintf("CREATE %sINDEX", uniqueStr)

	if c.IndexName != nil {
		str = fmt.Sprintf("%s %s ON %s", str, c.IndexName.ToSQLString(), c.TableName.ToSQLString())
	} else {
		str = fmt.Sprintf("%s ON %s", str, c.TableName.ToSQLString())
	}

	if c.MethodName != nil {
		str = fmt.Sprintf("%s USING %s", str, c.MethodName.ToSQLString())
	}

	str = fmt.Sprintf("%s (%s)", str, commaSeparatedString(c.ColumnNames))

	if c.Selection != nil {
		str = fmt.Sprintf("%s WHERE %s", str, c.Selection.ToSQLString())
	}

	return str
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

func (s *DropIndexStmt) ToSQLString() string {
	return fmt.Sprintf("DROP INDEX %s", commaSeparatedString(s.IndexNames))
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
	return fmt.Sprintf("EXPLAIN %s", e.Stmt.ToSQLString())
}
