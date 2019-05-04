package sqlast

import (
	"fmt"
	"log"
)

type SQLStmt interface {
	ASTNode
}

type SQLInsert struct {
	TableName SQLObjectName
	Columns   []SQLIdent
	Values    [][]ASTNode
}

type SQLCopy struct {
	TableName SQLObjectName
	Columns   []SQLIdent
	Values    []*string
}

type SQLUpdate struct {
	TableName   SQLObjectName
	Assignments *SQLAssignment
	Selection   ASTNode
}

type SQLDelete struct {
	TableName SQLObjectName
	Selection ASTNode
}

type SQLCreateView struct {
	Name         SQLObjectName
	Query        SQLQuery
	Materialized bool
}

type SQLCreateTable struct {
	Name       SQLObjectName
	Columns    []SQLColumnDef
	External   bool
	FileFormat *FileFormat
	Location   *string
}

type SQLAlterTable struct {
	Name SQLObjectName
}

type SQLAssignment struct {
	ID    SQLIdent
	Value ASTNode
}

func (s *SQLAssignment) Eval() string {
	return fmt.Sprintf("SET %s = %s", s.ID.Eval(), s.Value.Eval())
}

type SQLColumnDef struct {
	Name      SQLIdent
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
