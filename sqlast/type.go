package sqlast

import (
	"fmt"

	"github.com/akito0107/xsqlparser/sqltoken"
)

type Type interface {
	Node
}

type CharType struct {
	Size             *uint
	From, To, RParen sqltoken.Pos
}

func (c *CharType) Pos() sqltoken.Pos {
	return c.From
}

func (c *CharType) End() sqltoken.Pos {
	if c.Size != nil {
		return c.RParen
	}
	return c.To
}

func (c *CharType) ToSQLString() string {
	return formatTypeWithOptionalLength("char", c.Size)
}

type VarcharType struct {
	Size                       *uint
	Character, Varying, RParen sqltoken.Pos
}

func (v *VarcharType) Pos() sqltoken.Pos {
	return v.Character
}

func (v *VarcharType) End() sqltoken.Pos {
	if v.Size != nil {
		return v.RParen
	}
	return v.Varying
}

func (v *VarcharType) ToSQLString() string {
	return formatTypeWithOptionalLength("character varying", v.Size)
}

type UUID struct {
	From, To sqltoken.Pos
}

func (u *UUID) Pos() sqltoken.Pos {
	return u.From
}

func (u *UUID) End() sqltoken.Pos {
	return u.To
}

func (*UUID) ToSQLString() string {
	return "uuid"
}

type Clob struct {
	Size         uint
	Clob, RParen sqltoken.Pos
}

func (c *Clob) Pos() sqltoken.Pos {
	return c.Clob
}

func (c *Clob) End() sqltoken.Pos {
	return c.RParen
}

func (c *Clob) ToSQLString() string {
	return fmt.Sprintf("clob(%d)", c.Size)
}

type Binary struct {
	Size           uint
	Binary, RParen sqltoken.Pos
}

func (b *Binary) Pos() sqltoken.Pos {
	return b.Binary
}

func (b *Binary) End() sqltoken.Pos {
	return b.RParen
}

func (b *Binary) ToSQLString() string {
	return fmt.Sprintf("birany(%d)", b.Size)
}

type Varbinary struct {
	Size              uint
	Varbinary, RParen sqltoken.Pos
}

func (v *Varbinary) Pos() sqltoken.Pos {
	return v.Varbinary
}

func (v *Varbinary) End() sqltoken.Pos {
	return v.RParen
}

func (v *Varbinary) ToSQLString() string {
	return fmt.Sprintf("varbinary(%d)", v.Size)
}

type Blob struct {
	Size         uint
	Blob, RParen sqltoken.Pos
}

func (b *Blob) Pos() sqltoken.Pos {
	return b.Blob
}

func (b *Blob) End() sqltoken.Pos {
	return b.RParen
}

func (b *Blob) ToSQLString() string {
	return fmt.Sprintf("blob(%d)", b.Size)
}

type Decimal struct {
	Precision       *uint
	Scale           *uint
	Numeric, RParen sqltoken.Pos
}

func (d *Decimal) Pos() sqltoken.Pos {
	return d.Numeric
}

func (d *Decimal) End() sqltoken.Pos {
	return d.RParen
}

func (d *Decimal) ToSQLString() string {
	if d.Scale != nil {
		return fmt.Sprintf("numeric(%d,%d)", *d.Precision, *d.Scale)
	}
	return formatTypeWithOptionalLength("numeric", d.Precision)
}

type Float struct {
	Size             *uint
	From, To, RParen sqltoken.Pos
}

func (f *Float) Pos() sqltoken.Pos {
	return f.From
}

func (f *Float) End() sqltoken.Pos {
	if f.Size != nil {
		return f.RParen
	}
	return f.To
}

func (f *Float) ToSQLString() string {
	return formatTypeWithOptionalLength("float", f.Size)
}

type SmallInt struct {
	From, To sqltoken.Pos
}

func (s *SmallInt) Pos() sqltoken.Pos {
	return s.From
}

func (s *SmallInt) End() sqltoken.Pos {
	return s.To
}

func (s *SmallInt) ToSQLString() string {
	return "smallint"
}

type Int struct {
	From, To sqltoken.Pos
}

func (i *Int) Pos() sqltoken.Pos {
	return i.From
}

func (i *Int) End() sqltoken.Pos {
	return i.To
}

func (i *Int) ToSQLString() string {
	return "int"
}

type BigInt struct {
	From, To sqltoken.Pos
}

func (b *BigInt) Pos() sqltoken.Pos {
	return b.From
}

func (b *BigInt) End() sqltoken.Pos {
	return b.To
}

func (b *BigInt) ToSQLString() string {
	return "bigint"
}

type Real struct {
	From, To sqltoken.Pos
}

func (r *Real) Pos() sqltoken.Pos {
	return r.From
}

func (r *Real) End() sqltoken.Pos {
	return r.To
}

func (*Real) ToSQLString() string {
	return "real"
}

type Double struct {
	From, To sqltoken.Pos
}

func (d *Double) Pos() sqltoken.Pos {
	return d.From
}

func (d *Double) End() sqltoken.Pos {
	return d.To
}

func (*Double) ToSQLString() string {
	return "double precision"
}

type Boolean struct {
	From, To sqltoken.Pos
}

func (b *Boolean) Pos() sqltoken.Pos {
	return b.From
}

func (b *Boolean) End() sqltoken.Pos {
	return b.To
}

func (*Boolean) ToSQLString() string {
	return "boolean"
}

type Date struct {
	From, To sqltoken.Pos
}

func (d *Date) Pos() sqltoken.Pos {
	return d.From
}

func (d *Date) End() sqltoken.Pos {
	return d.To
}

func (*Date) ToSQLString() string {
	return "date"
}

type Time struct {
	From, To sqltoken.Pos
}

func (t *Time) Pos() sqltoken.Pos {
	return t.From
}

func (t *Time) End() sqltoken.Pos {
	return t.To
}

func (*Time) ToSQLString() string {
	return "time"
}

type Timestamp struct {
	WithTimeZone bool
	Timestamp    sqltoken.Pos
	Zone         sqltoken.Pos
}

func (t *Timestamp) Pos() sqltoken.Pos {
	return t.Timestamp
}

func (t *Timestamp) End() sqltoken.Pos {
	if t.WithTimeZone {
		return t.Zone
	}

	return sqltoken.Pos{
		Line: t.Timestamp.Line,
		Col:  t.Timestamp.Col + 9,
	}
}

func (t *Timestamp) ToSQLString() string {
	var timezone string
	if t.WithTimeZone {
		timezone = " with time zone"
	}
	return "timestamp" + timezone
}

type Regclass struct {
	From, To sqltoken.Pos
}

func (r *Regclass) Pos() sqltoken.Pos {
	return r.From
}

func (r *Regclass) End() sqltoken.Pos {
	return r.To
}

func (*Regclass) ToSQLString() string {
	return "regclass"
}

type Text struct {
	From, To sqltoken.Pos
}

func (t *Text) Pos() sqltoken.Pos {
	return t.From
}

func (t *Text) End() sqltoken.Pos {
	return t.To
}

func (*Text) ToSQLString() string {
	return "text"
}

type Bytea struct {
	From, To sqltoken.Pos
}

func (b *Bytea) Pos() sqltoken.Pos {
	return b.From
}

func (b *Bytea) End() sqltoken.Pos {
	return b.To
}

func (*Bytea) ToSQLString() string {
	return "bytea"
}

type Array struct {
	Ty     Type
	RParen sqltoken.Pos
}

func (a *Array) Pos() sqltoken.Pos {
	return a.Ty.Pos()
}

func (a *Array) End() sqltoken.Pos {
	return a.RParen
}

func (a *Array) ToSQLString() string {
	return fmt.Sprintf("%s[]", a.Ty.ToSQLString())
}

type Custom struct {
	Ty *ObjectName
}

func (c *Custom) Pos() sqltoken.Pos {
	return c.Ty.Pos()
}

func (c *Custom) End() sqltoken.Pos {
	return c.Ty.End()
}

func (c *Custom) ToSQLString() string {
	return c.Ty.ToSQLString()
}

func formatTypeWithOptionalLength(sqltype string, len *uint) string {
	s := sqltype
	if len != nil {
		s += fmt.Sprintf("(%d)", *len)
	}

	return s
}

func NewSize(s uint) *uint {
	return &s
}
