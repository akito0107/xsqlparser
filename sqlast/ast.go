package sqlast

import (
	"fmt"
	"strings"
)

type SQLIdent string

func (s *SQLIdent) String() string {
	return string(*s)
}

type ASTNode interface {
	String() string
}

// Identifier e.g. table name or column name
type SQLIdentifier struct {
	Ident SQLIdent
}

func (s *SQLIdentifier) String() string {
	return string(s.Ident)
}

// *
type SQLWildcard struct{}

func (s SQLWildcard) String() string {
	return "*"
}

// table.*, schema.table.*
type SQLQualifiedWildcard struct {
	Idents []SQLIdent
}

func (s *SQLQualifiedWildcard) String() string {
	strs := make([]string, 0, len(s.Idents))
	for _, ident := range s.Idents {
		strs = append(strs, string(ident))
	}
	return fmt.Sprintf("%s.*", strings.Join(strs, "."))
}

// table.column / schema.table.column
type SQLCompoundIdentifier struct {
	Idents []SQLIdent
}

func (s *SQLCompoundIdentifier) String() string {
	strs := make([]string, 0, len(s.Idents))
	for _, ident := range s.Idents {
		strs = append(strs, string(ident))
	}
	return strings.Join(strs, ".")
}

type SQLIsNull struct {
	X ASTNode
}

func (s *SQLIsNull) String() string {
	return fmt.Sprintf("%s IS NULl", s.X.String())
}

type SQLIsNotNull struct {
	X ASTNode
}

func (s *SQLIsNotNull) String() string {
	return fmt.Sprintf("%s IS NOT NULL", s.X.String())
}

type SQLInList struct {
	Expr    ASTNode
	List    []ASTNode
	Negated bool
}

func (s *SQLInList) String() string {
	var n string
	if s.Negated {
		n = "NOT "
	}
	return fmt.Sprintf("%s %sIN {%s}", s.Expr.String(), n, commaSeparatedString(s.List))
}

type SQLObjectName struct {
	Idents []SQLIdent
}

func (s *SQLObjectName) String() string {
	var strs []string
	for _, l := range s.Idents {
		strs = append(strs, string(l))
	}
	return strings.Join(strs, ".")
}

func commaSeparatedString(list []ASTNode) string {
	var strs []string
	for _, l := range list {
		strs = append(strs, l.String())
	}
	return strings.Join(strs, ", ")
}
