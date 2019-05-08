package sqlast

import (
	"fmt"
	"log"
	"strings"
)

type SQLStmt interface {
	ASTNode
}

type SQLInsert struct {
	TableName *SQLObjectName
	Columns   []*SQLIdent
	Values    [][]ASTNode
}

func (s *SQLInsert) Eval() string {
	str := fmt.Sprintf("INSERT INTO %s", s.TableName.Eval())
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
	TableName SQLObjectName
	Columns   []SQLIdent
	Values    []*string
}

func (s *SQLCopy) Eval() string {
	str := fmt.Sprintf("COPY %s", s.TableName.Eval())
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
	TableName   *SQLObjectName
	Assignments []*SQLAssignment
	Selection   ASTNode
}

func (s *SQLUpdate) Eval() string {
	str := fmt.Sprintf("UPDATE %s SET ", s.TableName.Eval())
	if s.Assignments != nil {
		str += commaSeparatedString(s.Assignments)
	}
	if s.Selection != nil {
		str += fmt.Sprintf(" WHERE %s", s.Selection.Eval())
	}

	return str
}

type SQLDelete struct {
	TableName *SQLObjectName
	Selection ASTNode
}

func (s *SQLDelete) Eval() string {
	str := fmt.Sprintf("DELETE FROM %s", s.TableName.Eval())

	if s.Selection != nil {
		str += fmt.Sprintf(" WHERE %s", s.Selection.Eval())
	}

	return str
}

type SQLCreateView struct {
	Name         *SQLObjectName
	Query        *SQLQuery
	Materialized bool
}

func (s *SQLCreateView) Eval() string {
	var modifier string
	if s.Materialized {
		modifier = " MATERIALIZED"
	}
	return fmt.Sprintf("CREATE%s VIEW %s AS %s", modifier, s.Name.Eval(), s.Query.Eval())
}

type SQLCreateTable struct {
	Name       *SQLObjectName
	Columns    []*SQLColumnDef
	External   bool
	FileFormat *FileFormat
	Location   *string
}

func (s *SQLCreateTable) Eval() string {
	if s.External {
		return fmt.Sprintf("CREATE EXETRNAL TABLE %s (%s) STORED AS %s LOCATION '%s'",
			s.Name.Eval(), commaSeparatedString(s.Columns), s.FileFormat.Eval(), *s.Location)
	}
	return fmt.Sprintf("CREATE TABLE %s (%s)", s.Name.Eval(), commaSeparatedString(s.Columns))
}

type SQLAlterTable struct {
	TableName *SQLObjectName
	Operation AlterOperation
}

func (s *SQLAlterTable) Eval() string {
	return fmt.Sprintf("ALTER TABLE %s %s", s.TableName.Eval(), s.Operation.Eval())
}

type SQLAssignment struct {
	ID    *SQLIdent
	Value ASTNode
}

func (s *SQLAssignment) Eval() string {
	return fmt.Sprintf("%s = %s", s.ID.Eval(), s.Value.Eval())
}

type SQLColumnDef struct {
	Name      *SQLIdent
	DateType  SQLType
	IsPrimary bool
	IsUnique  bool
	Default   ASTNode
	AllowNull bool
}

func (s *SQLColumnDef) Eval() string {
	str := fmt.Sprintf("%s %s", s.Name.Eval(), s.DateType.Eval())
	if s.IsPrimary {
		str += " PRIMARY KEY"
	}
	if s.IsUnique {
		str += " UNIQUE"
	}
	if s.Default != nil {
		str += fmt.Sprintf(" DEFAULT %s", s.Default.Eval())
	}
	if !s.AllowNull {
		str += " NOT NULL"
	}
	return str
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

func (f *FileFormat) Eval() string {
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
