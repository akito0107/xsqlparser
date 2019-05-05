package sqlast

import "fmt"

type AlterOperation interface {
	ASTNode
}

type AddColumn struct {
	Column *SQLColumnDef
}

func (a *AddColumn) Eval() string {
	return fmt.Sprintf("ADD COLUMN %s", a.Column.Eval())
}

type RemoveColumn struct {
	Name    *SQLIdent
	Cascade bool
}

func (r *RemoveColumn) Eval() string {
	var cascade string
	if r.Cascade {
		cascade += " CASCADE"
	}
	return fmt.Sprintf("DROP COLUMN %s%s", r.Name.Eval(), cascade)
}

// postgres
type AddForeignKey struct {
	ForeignTable   *SQLObjectName
	ReferredColumn *SQLIdent
}

func (a *AddForeignKey) Eval() string {
	return fmt.Sprintf("ADD FOREIGN KEY (%s) REFERENCES %s", a.ReferredColumn.Eval(), a.ForeignTable)
}

type AddConstraint struct {
	TableKey SQLIdent
}

func (a *AddConstraint) Eval() string {
	return fmt.Sprintf("ADD CONSTRAINT %s", a.TableKey.Eval())
}

type AlterColumn struct {
	Expr   ASTNode
	Column *SQLIdent
}

func (a *AlterColumn) Eval() string {
	return fmt.Sprintf("ALTER COLUMN %s %s", a.Column.Eval(), a.Expr.Eval())
}

type RemoveConstraint struct {
	Name SQLIdent
}

func (r *RemoveConstraint) Eval() string {
	return fmt.Sprintf("REMOVE CONSTRAINT %s", r.Name.Eval())
}

type TableKey interface {
	ASTNode
}

type Key struct {
	Name    SQLIdent
	Columns []SQLIdent
}

func (k *Key) Eval() string {
	return fmt.Sprintf("%s KEY (%s)", k.Name.Eval(), commaSeparatedString(k.Columns))
}

type PrimaryKey struct {
	Key Key
}

func (p *PrimaryKey) Eval() string {
	return fmt.Sprintf("%s PRIMARY KEY (%s)", p.Key.Name.Eval(), commaSeparatedString(p.Key.Columns))
}

type UniqueKey struct {
	Key Key
}

func (u *UniqueKey) Eval() string {
	return fmt.Sprintf("%s UNIQUE KEY (%s)", u.Key.Name.Eval(), commaSeparatedString(u.Key.Columns))
}

type ForeignKey struct {
	Key             Key
	ForeignTable    SQLObjectName
	ReferredColumns []SQLIdent
}

func (f *ForeignKey) Eval() string {
	return fmt.Sprintf("%s FOREIGN KEY (%s) REFERENCES%s(%s)", f.Key.Name.Eval(), commaSeparatedString(f.Key.Columns), f.ForeignTable.Eval(), commaSeparatedString(f.ReferredColumns))
}
