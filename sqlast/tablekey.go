package sqlast

import "fmt"

type AlterOperation interface {
	ASTNode
}

type AddColumn struct {
	Column *SQLColumnDef
}

func (a *AddColumn) ToSQLString() string {
	return fmt.Sprintf("ADD COLUMN %s", a.Column.ToSQLString())
}

type RemoveColumn struct {
	Name    *SQLIdent
	Cascade bool
}

func (r *RemoveColumn) ToSQLString() string {
	var cascade string
	if r.Cascade {
		cascade += " CASCADE"
	}
	return fmt.Sprintf("DROP COLUMN %s%s", r.Name.ToSQLString(), cascade)
}

// postgres
type AddForeignKey struct {
	ForeignTable   *SQLObjectName
	ReferredColumn *SQLIdent
}

func (a *AddForeignKey) ToSQLString() string {
	return fmt.Sprintf("ADD FOREIGN KEY (%s) REFERENCES %s", a.ReferredColumn.ToSQLString(), a.ForeignTable.ToSQLString())
}

type AddConstraint struct {
	TableKey TableKey
}

func (a *AddConstraint) ToSQLString() string {
	return fmt.Sprintf("ADD CONSTRAINT %s", a.TableKey.ToSQLString())
}

type AlterColumn struct {
	Expr   ASTNode
	Column *SQLIdent
}

func (a *AlterColumn) ToSQLString() string {
	return fmt.Sprintf("ALTER COLUMN %s %s", a.Column.ToSQLString(), a.Expr.ToSQLString())
}

type RemoveConstraint struct {
	Name SQLIdent
}

func (r *RemoveConstraint) ToSQLString() string {
	return fmt.Sprintf("REMOVE CONSTRAINT %s", r.Name.ToSQLString())
}

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
