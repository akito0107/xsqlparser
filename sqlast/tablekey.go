package sqlast

import "fmt"

type TableKey interface {
	ASTNode
}

type Key struct {
	Name    *SQLIdent
	Columns []*SQLIdent
}

func (k *Key) ToSQLString() string {
	return fmt.Sprintf("%s KEY (%s)", k.Name.ToSQLString(), commaSeparatedString(k.Columns))
}

type PrimaryKey struct {
	Key *Key
}

func (p *PrimaryKey) ToSQLString() string {
	return fmt.Sprintf("%s PRIMARY KEY (%s)", p.Key.Name.ToSQLString(), commaSeparatedString(p.Key.Columns))
}

type UniqueKey struct {
	Key *Key
}

func (u *UniqueKey) ToSQLString() string {
	return fmt.Sprintf("%s UNIQUE KEY (%s)", u.Key.Name.ToSQLString(), commaSeparatedString(u.Key.Columns))
}

type ForeignKey struct {
	Key             *Key
	ForeignTable    *SQLObjectName
	ReferredColumns []*SQLIdent
}

func (f *ForeignKey) ToSQLString() string {
	return fmt.Sprintf("%s FOREIGN KEY (%s) REFERENCES%s(%s)", f.Key.Name.ToSQLString(), commaSeparatedString(f.Key.Columns), f.ForeignTable.ToSQLString(), commaSeparatedString(f.ReferredColumns))
}
