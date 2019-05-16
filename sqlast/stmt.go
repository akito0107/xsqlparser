package sqlast

import (
	"fmt"
	"log"
	"strings"
)

//go:generate genmark -t SQLStmt -e ASTNode

type SQLInsert struct {
	sqlStmt
	TableName *SQLObjectName
	Columns   []*SQLIdent
	Values    [][]ASTNode
}

func (s *SQLInsert) ToSQLString() string {
	str := fmt.Sprintf("INSERT INTO %s", s.TableName.ToSQLString())
	if len(s.Columns) != 0 {
		str += fmt.Sprintf(" (%s)", commaSeparatedString(s.Columns))
	}
	if len(s.Values) != 0 {
		var valuestrs []string
		for _, v := range s.Values {
			valuestrs = append(valuestrs, commaSeparatedString(v))
		}
		str += fmt.Sprintf(" VALUES(%s)", strings.Join(valuestrs, ", "))
	}

	return str
}

type SQLCopy struct {
	sqlStmt
	TableName SQLObjectName
	Columns   []SQLIdent
	Values    []*string
}

func (s *SQLCopy) ToSQLString() string {
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

type SQLUpdate struct {
	sqlStmt
	TableName   *SQLObjectName
	Assignments []*SQLAssignment
	Selection   ASTNode
}

func (s *SQLUpdate) ToSQLString() string {
	str := fmt.Sprintf("UPDATE %s SET ", s.TableName.ToSQLString())
	if s.Assignments != nil {
		str += commaSeparatedString(s.Assignments)
	}
	if s.Selection != nil {
		str += fmt.Sprintf(" WHERE %s", s.Selection.ToSQLString())
	}

	return str
}

type SQLDelete struct {
	sqlStmt
	TableName *SQLObjectName
	Selection ASTNode
}

func (s *SQLDelete) ToSQLString() string {
	str := fmt.Sprintf("DELETE FROM %s", s.TableName.ToSQLString())

	if s.Selection != nil {
		str += fmt.Sprintf(" WHERE %s", s.Selection.ToSQLString())
	}

	return str
}

type SQLCreateView struct {
	sqlStmt
	Name         *SQLObjectName
	Query        *SQLQuery
	Materialized bool
}

func (s *SQLCreateView) ToSQLString() string {
	var modifier string
	if s.Materialized {
		modifier = " MATERIALIZED"
	}
	return fmt.Sprintf("CREATE%s VIEW %s AS %s", modifier, s.Name.ToSQLString(), s.Query.ToSQLString())
}

type SQLCreateTable struct {
	sqlStmt
	Name       *SQLObjectName
	Elements   []TableElement
	External   bool
	FileFormat *FileFormat
	Location   *string
	NotExists  bool
}

func (s *SQLCreateTable) ToSQLString() string {
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

type SQLAssignment struct {
	ID    *SQLIdent
	Value ASTNode
}

func (s *SQLAssignment) ToSQLString() string {
	return fmt.Sprintf("%s = %s", s.ID.ToSQLString(), s.Value.ToSQLString())
}

//go:generate genmark -t TableElement -e ASTNode

type TableConstraint struct {
	tableElement
	Name *SQLIdentifier
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

//go:generate genmark -t TableConstraintSpec -e ASTNode

type UniqueTableConstraint struct {
	tableConstraintSpec
	IsPrimary bool
	Columns   []*SQLIdent
}

func (u *UniqueTableConstraint) ToSQLString() string {
	if u.IsPrimary {
		return fmt.Sprintf("PRIMARY KEY(%s)", commaSeparatedString(u.Columns))
	}
	return fmt.Sprintf("UNIQUE(%s)", commaSeparatedString(u.Columns))
}

type ReferentialTableConstraint struct {
	tableConstraintSpec
	Columns []*SQLIdent
	KeyExpr *ReferenceKeyExpr
}

func (r *ReferentialTableConstraint) ToSQLString() string {
	return fmt.Sprintf("FOREIGN KEY(%s) REFERENCES %s", commaSeparatedString(r.Columns), r.KeyExpr.ToSQLString())
}

type ReferenceKeyExpr struct {
	TableName *SQLIdentifier
	Columns   []*SQLIdent
}

func (r *ReferenceKeyExpr) ToSQLString() string {
	return fmt.Sprintf("%s(%s)", r.TableName.ToSQLString(), commaSeparatedString(r.Columns))
}

type CheckTableConstraint struct {
	tableConstraintSpec
	Expr ASTNode
}

func (c *CheckTableConstraint) ToSQLString() string {
	return fmt.Sprintf("CHECK(%s)", c.Expr.ToSQLString())
}

type SQLColumnDef struct {
	tableElement
	Name        *SQLIdent
	DataType    SQLType
	Default     ASTNode
	Constraints []*ColumnConstraint
}

func (s *SQLColumnDef) ToSQLString() string {
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
	Name *SQLIdentifier
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
	ASTNode
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
	TableName *SQLObjectName
	Columns   []*SQLIdent
}

func (r *ReferencesColumnSpec) ToSQLString() string {
	return fmt.Sprintf("REFERENCES %s(%s)", r.TableName.ToSQLString(), commaSeparatedString(r.Columns))
}

type CheckColumnSpec struct {
	Expr ASTNode
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

type SQLAlterTable struct {
	sqlStmt
	TableName *SQLObjectName
	Action    AlterTableAction
}

func (s *SQLAlterTable) ToSQLString() string {
	return fmt.Sprintf("ALTER TABLE %s %s", s.TableName.ToSQLString(), s.Action.ToSQLString())
}

//go:generate genmark -t AlterTableAction -e ASTNode

type AddColumnTableAction struct {
	alterTableAction
	Column *SQLColumnDef
}

func (a *AddColumnTableAction) ToSQLString() string {
	return fmt.Sprintf("ADD COLUMN %s", a.Column.ToSQLString())
}

type AlterColumnTableAction struct {
	alterTableAction
	ColumnName *SQLIdent
	Action     AlterColumnAction
}

func (a *AlterColumnTableAction) ToSQLString() string {
	return fmt.Sprintf("ALTER COLUMN %s %s", a.ColumnName.ToSQLString(), a.Action.ToSQLString())
}

//go:generate genmark -t AlterColumnAction -e ASTNode

// TODO add column scope / drop column scope / alter identity column spec
// https://jakewheat.github.io/sql-overview/sql-2008-foundation-grammar.html#alter-column-definition

type SetDefaultColumnAction struct {
	alterColumnAction
	Default ASTNode
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
	DataType SQLType
}

func (p *PGAlterDataTypeColumnAction) ToSQLString() string {
	return fmt.Sprintf("TYPE %s", p.DataType.ToSQLString())
}

type RemoveColumnTableAction struct {
	alterTableAction
	Name    *SQLIdent
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
	Name    *SQLIdent
	Cascade bool
}

func (d *DropConstraintTableAction) ToSQLString() string {
	var cascade string
	if d.Cascade {
		cascade += " CASCADE"
	}
	return fmt.Sprintf("DROP CONSTRAINT %s%s", d.Name.ToSQLString(), cascade)
}

type SQLDropTable struct {
	sqlStmt
	TableNames []*SQLObjectName
	Cascade    bool
	IfExists   bool
}

func (s *SQLDropTable) ToSQLString() string {
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
