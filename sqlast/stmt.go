package sqlast

import (
	"fmt"
	"log"
	"strings"
)

//go:generate genmark -t Stmt -e Node

type InsertStmt struct {
	stmt
	TableName         *ObjectName
	Columns           []*Ident
	Values            [][]Node
	UpdateAssignments []*Assignment // MySQL only (ON DUPLICATED KEYS)
}

func (s *InsertStmt) ToSQLString() string {
	str := fmt.Sprintf("INSERT INTO %s", s.TableName.ToSQLString())
	if len(s.Columns) != 0 {
		str += fmt.Sprintf(" (%s)", commaSeparatedString(s.Columns))
	}
	if len(s.Values) != 0 {
		var valuestrs []string
		for _, v := range s.Values {
			str := commaSeparatedString(v)
			valuestrs = append(valuestrs, fmt.Sprintf("(%s)", str))
		}
		str += fmt.Sprintf(" VALUES %s", strings.Join(valuestrs, ", "))
	}

	if len(s.UpdateAssignments) != 0 {
		str += " ON DUPLICATE KEY UPDATE " + commaSeparatedString(s.UpdateAssignments)
	}

	return str
}

type CopyStmt struct {
	stmt
	TableName *ObjectName
	Columns   []*Ident
	Values    []*string
}

func (s *CopyStmt) ToSQLString() string {
	str := fmt.Sprintf("COPY %s", s.TableName.ToSQLString())
	if len(s.Columns) != 0 {
		str += fmt.Sprintf(" (%s)", commaSeparatedString(s.Columns))
	}
	str += " FROM stdin; "

	if len(s.Values) != 0 {
		var valuestrs []string
		for _, v := range s.Values {
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
	TableName   *ObjectName
	Assignments []*Assignment
	Selection   Node
}

func (s *UpdateStmt) ToSQLString() string {
	str := fmt.Sprintf("UPDATE %s SET ", s.TableName.ToSQLString())
	if s.Assignments != nil {
		str += commaSeparatedString(s.Assignments)
	}
	if s.Selection != nil {
		str += fmt.Sprintf(" WHERE %s", s.Selection.ToSQLString())
	}

	return str
}

type DeleteStmt struct {
	stmt
	TableName *ObjectName
	Selection Node
}

func (s *DeleteStmt) ToSQLString() string {
	str := fmt.Sprintf("DELETE FROM %s", s.TableName.ToSQLString())

	if s.Selection != nil {
		str += fmt.Sprintf(" WHERE %s", s.Selection.ToSQLString())
	}

	return str
}

type CreateViewStmt struct {
	stmt
	Name         *ObjectName
	Query        *Query
	Materialized bool
}

func (s *CreateViewStmt) ToSQLString() string {
	var modifier string
	if s.Materialized {
		modifier = " MATERIALIZED"
	}
	return fmt.Sprintf("CREATE%s VIEW %s AS %s", modifier, s.Name.ToSQLString(), s.Query.ToSQLString())
}

type CreateTableStmt struct {
	stmt
	Name       *ObjectName
	Elements   []TableElement
	External   bool
	FileFormat *FileFormat
	Location   *string
	NotExists  bool
}

func (s *CreateTableStmt) ToSQLString() string {
	ifNotExists := ""
	if s.NotExists {
		ifNotExists = "IF NOT EXISTS "
	}
	if s.External {
		return fmt.Sprintf("CREATE EXETRNAL TABLE %s%s (%s) STORED AS %s LOCATION '%s'",
			ifNotExists, s.Name.ToSQLString(), commaSeparatedString(s.Elements), s.FileFormat.ToSQLString(), *s.Location)
	}
	return fmt.Sprintf("CREATE TABLE %s%s (%s)", ifNotExists, s.Name.ToSQLString(), commaSeparatedString(s.Elements))
}

type Assignment struct {
	ID    *Ident
	Value Node
}

func (s *Assignment) ToSQLString() string {
	return fmt.Sprintf("%s = %s", s.ID.ToSQLString(), s.Value.ToSQLString())
}

//go:generate genmark -t TableElement -e Node

type TableConstraint struct {
	tableElement
	Name *Ident
	Spec TableConstraintSpec
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
	IsPrimary bool
	Columns   []*Ident
}

func (u *UniqueTableConstraint) ToSQLString() string {
	if u.IsPrimary {
		return fmt.Sprintf("PRIMARY KEY(%s)", commaSeparatedString(u.Columns))
	}
	return fmt.Sprintf("UNIQUE(%s)", commaSeparatedString(u.Columns))
}

type ReferentialTableConstraint struct {
	tableConstraintSpec
	Columns []*Ident
	KeyExpr *ReferenceKeyExpr
}

func (r *ReferentialTableConstraint) ToSQLString() string {
	return fmt.Sprintf("FOREIGN KEY(%s) REFERENCES %s", commaSeparatedString(r.Columns), r.KeyExpr.ToSQLString())
}

type ReferenceKeyExpr struct {
	TableName *Ident
	Columns   []*Ident
}

func (r *ReferenceKeyExpr) ToSQLString() string {
	return fmt.Sprintf("%s(%s)", r.TableName.ToSQLString(), commaSeparatedString(r.Columns))
}

type CheckTableConstraint struct {
	tableConstraintSpec
	Expr Node
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

func (s *ColumnDef) ToSQLString() string {
	str := fmt.Sprintf("%s %s", s.Name.ToSQLString(), s.DataType.ToSQLString())
	if s.Default != nil {
		str += fmt.Sprintf(" DEFAULT %s", s.Default.ToSQLString())
	}

	for _, c := range s.Constraints {
		str += fmt.Sprintf("%s", c.ToSQLString())
	}
	return str
}

type ColumnConstraint struct {
	Name *Ident
	Spec ColumnConstraintSpec
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
}

func (*NotNullColumnSpec) ToSQLString() string {
	return fmt.Sprintf("NOT NULL")
}

type UniqueColumnSpec struct {
	IsPrimaryKey bool
}

func (u *UniqueColumnSpec) ToSQLString() string {
	if u.IsPrimaryKey {
		return fmt.Sprintf("PRIMARY KEY")
	} else {
		return fmt.Sprintf("UNIQUE")
	}
}

type ReferencesColumnSpec struct {
	TableName *ObjectName
	Columns   []*Ident
}

func (r *ReferencesColumnSpec) ToSQLString() string {
	return fmt.Sprintf("REFERENCES %s(%s)", r.TableName.ToSQLString(), commaSeparatedString(r.Columns))
}

type CheckColumnSpec struct {
	Expr Node
}

func (c *CheckColumnSpec) ToSQLString() string {
	return fmt.Sprintf("CHECK(%s)", c.Expr.ToSQLString())
}

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
	TableName *ObjectName
	Action    AlterTableAction
}

func (s *AlterTableStmt) ToSQLString() string {
	return fmt.Sprintf("ALTER TABLE %s %s", s.TableName.ToSQLString(), s.Action.ToSQLString())
}

//go:generate genmark -t AlterTableAction -e Node

type AddColumnTableAction struct {
	alterTableAction
	Column *ColumnDef
}

func (a *AddColumnTableAction) ToSQLString() string {
	return fmt.Sprintf("ADD COLUMN %s", a.Column.ToSQLString())
}

type AlterColumnTableAction struct {
	alterTableAction
	ColumnName *Ident
	Action     AlterColumnAction
}

func (a *AlterColumnTableAction) ToSQLString() string {
	return fmt.Sprintf("ALTER COLUMN %s %s", a.ColumnName.ToSQLString(), a.Action.ToSQLString())
}

//go:generate genmark -t AlterColumnAction -e Node

// TODO add column scope / drop column scope / alter identity column spec
// https://jakewheat.github.io/sql-overview/sql-2008-foundation-grammar.html#alter-column-definition

type SetDefaultColumnAction struct {
	alterColumnAction
	Default Node
}

func (s *SetDefaultColumnAction) ToSQLString() string {
	return fmt.Sprintf("SET DEFAULT %s", s.Default.ToSQLString())
}

type DropDefaultColumnAction struct {
	alterColumnAction
}

func (*DropDefaultColumnAction) ToSQLString() string {
	return fmt.Sprintf("DROP DEFAULT")
}

// postgres only
type PGAlterDataTypeColumnAction struct {
	alterColumnAction
	DataType Type
}

func (p *PGAlterDataTypeColumnAction) ToSQLString() string {
	return fmt.Sprintf("TYPE %s", p.DataType.ToSQLString())
}

type PGSetNotNullColumnAction struct {
	alterColumnAction
}

func (p *PGSetNotNullColumnAction) ToSQLString() string {
	return fmt.Sprintf("SET NOT NULL")
}

type PGDropNotNullColumnAction struct {
	alterColumnAction
}

func (p *PGDropNotNullColumnAction) ToSQLString() string {
	return fmt.Sprintf("DROP NOT NULL")
}

type RemoveColumnTableAction struct {
	alterTableAction
	Name    *Ident
	Cascade bool
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
	Constraint *TableConstraint
}

func (a *AddConstraintTableAction) ToSQLString() string {
	return fmt.Sprintf("ADD %s", a.Constraint.ToSQLString())
}

type DropConstraintTableAction struct {
	alterTableAction
	Name    *Ident
	Cascade bool
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
	IfExists   bool
}

func (s *DropTableStmt) ToSQLString() string {
	var ifexists string
	if s.IfExists {
		ifexists = "IF EXISTS "
	}

	var cascade string
	if s.Cascade {
		cascade = " CASCADE"
	}

	return fmt.Sprintf("DROP TABLE %s%s%s", ifexists, commaSeparatedString(s.TableNames), cascade)
}

type CreateIndexStmt struct {
	stmt
	TableName   *ObjectName
	IsUnique    bool
	IndexName   *Ident
	MethodName  *Ident
	ColumnNames []*Ident
	Selection   Node
}

func (s *CreateIndexStmt) ToSQLString() string {
	var uniqueStr string
	if s.IsUnique {
		uniqueStr = "UNIQUE "
	}
	str := fmt.Sprintf("CREATE %sINDEX", uniqueStr)

	if s.IndexName != nil {
		str = fmt.Sprintf("%s %s ON %s", str, s.IndexName.ToSQLString(), s.TableName.ToSQLString())
	} else {
		str = fmt.Sprintf("%s ON %s", str, s.TableName.ToSQLString())
	}

	if s.MethodName != nil {
		str = fmt.Sprintf("%s USING %s", str, s.MethodName.ToSQLString())
	}

	str = fmt.Sprintf("%s (%s)", str, commaSeparatedString(s.ColumnNames))

	if s.Selection != nil {
		str = fmt.Sprintf("%s WHERE %s", str, s.Selection.ToSQLString())
	}

	return str
}

type DropIndexStmt struct {
	stmt
	IndexNames []*Ident
}

func (s *DropIndexStmt) ToSQLString() string {
	return fmt.Sprintf("DROP INDEX %s", commaSeparatedString(s.IndexNames))
}

type ExplainStmt struct {
	stmt
	Stmt Stmt
}

func (s *ExplainStmt) ToSQLString() string {
	return fmt.Sprintf("EXPLAIN %s", s.Stmt.ToSQLString())
}
