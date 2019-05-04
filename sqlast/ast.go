package sqlast

import (
	"fmt"
	"strings"
)

type SQLIdent string

func (s *SQLIdent) Eval() string {
	return string(*s)
}

type ASTNode interface {
	Eval() string
}

// Identifier e.g. table name or column name
type SQLIdentifier struct {
	Ident SQLIdent
}

func (s *SQLIdentifier) Eval() string {
	return string(s.Ident)
}

// *
type SQLWildcard struct{}

func (s SQLWildcard) Eval() string {
	return "*"
}

// table.*, schema.table.*
type SQLQualifiedWildcard struct {
	Idents []SQLIdent
}

func (s *SQLQualifiedWildcard) Eval() string {
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

func (s *SQLCompoundIdentifier) Eval() string {
	strs := make([]string, 0, len(s.Idents))
	for _, ident := range s.Idents {
		strs = append(strs, string(ident))
	}
	return strings.Join(strs, ".")
}

type SQLIsNull struct {
	X ASTNode
}

func (s *SQLIsNull) Eval() string {
	return fmt.Sprintf("%s IS NULl", s.X.Eval())
}

type SQLIsNotNull struct {
	X ASTNode
}

func (s *SQLIsNotNull) Eval() string {
	return fmt.Sprintf("%s IS NOT NULL", s.X.Eval())
}

type SQLInList struct {
	Expr    ASTNode
	List    []ASTNode
	Negated bool
}

func (s *SQLInList) Eval() string {
	return fmt.Sprintf("%s %sIN {%s}", s.Expr.Eval(), negatedString(s.Negated), commaSeparatedString(s.List))
}

//[ NOT ] IN (SELECT ...)
type SQLInSubQuery struct {
	Expr     ASTNode
	SubQuery *SQLQuery
	Negated  bool
}

func (s *SQLInSubQuery) Eval() string {
	return fmt.Sprintf("%s %sIN (%s)", s.Expr.Eval(), negatedString(s.Negated), s.SubQuery.Eval())
}

type SQLBetween struct {
	Expr    ASTNode
	Negated bool
	Low     ASTNode
	High    ASTNode
}

func (s *SQLBetween) Eval() string {
	return fmt.Sprintf("%s %sBETWEEN %s AND %s", s.Expr.Eval(), negatedString(s.Negated), s.Low.Eval(), s.High.Eval())
}

type SQLBinaryExpr struct {
	Left  ASTNode
	Op    SQLOperator
	Right ASTNode
}

func (s *SQLBinaryExpr) Eval() string {
	return fmt.Sprintf("%s %s %s", s.Left.Eval(), s.Op.String(), s.Right.Eval())
}

type SQLCast struct {
	Expr     ASTNode
	DateType SQLType
}

type SQLObjectName struct {
	Idents []SQLIdent
}

func (s *SQLObjectName) Eval() string {
	var strs []string
	for _, l := range s.Idents {
		strs = append(strs, string(l))
	}
	return strings.Join(strs, ".")
}

func commaSeparatedString(list interface{}) string {
	var strs []string
	switch s := list.(type) {
	case []ASTNode:
		for _, l := range s {
			strs = append(strs, l.Eval())
		}
	case []SQLSelectItem:
		for _, l := range s {
			strs = append(strs, l.Eval())
		}
	case []SQLIdent:
		for _, l := range s {
			strs = append(strs, l.Eval())
		}
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
