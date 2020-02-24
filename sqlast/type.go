package sqlast

import (
	"io"

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
	return toSQLString(c)
}

func (c *CharType) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).TypeWithOptionalLength([]byte("char"), c.Size).End()
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
	return toSQLString(v)
}

func (v *VarcharType) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).TypeWithOptionalLength([]byte("character varying"), v.Size).End()
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

func (u *UUID) WriteTo(w io.Writer) (int64, error) {
	return writeSingleBytes(w, []byte("uuid"))
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
	return toSQLString(c)
}

func (c *Clob) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).TypeWithOptionalLength([]byte("clob"), &c.Size).End()
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
	return toSQLString(b)
}

func (b *Binary) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).TypeWithOptionalLength([]byte("binary"), &b.Size).End()
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
	return toSQLString(v)
}

func (v *Varbinary) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).TypeWithOptionalLength([]byte("varbinary"), &v.Size).End()
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
	return toSQLString(b)
}

func (b *Blob) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).TypeWithOptionalLength([]byte("blob"), &b.Size).End()
}

// All unsigned props are only available on MySQL

type Decimal struct {
	Precision       *uint
	Scale           *uint
	Numeric, RParen sqltoken.Pos
	IsUnsigned      bool
	Unsigned        sqltoken.Pos
}

func (d *Decimal) Pos() sqltoken.Pos {
	return d.Numeric
}

func (d *Decimal) End() sqltoken.Pos {
	if d.IsUnsigned {
		return d.Unsigned
	}
	return d.RParen
}

func (d *Decimal) ToSQLString() string {
	return toSQLString(d)
}

func (d *Decimal) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Bytes([]byte("numeric"))
	if d.Precision != nil {
		sw.LParen()
		sw.Int(int(*d.Precision))
		if d.Scale != nil {
			sw.Bytes([]byte(","))
			sw.Int(int(*d.Scale))
		}
		sw.RParen()
	}
	sw.If(d.IsUnsigned, []byte(" unsigned"))
	return sw.End()
}

type Float struct {
	Size             *uint
	From, To, RParen sqltoken.Pos
	IsUnsigned       bool
	Unsigned         sqltoken.Pos
}

func (f *Float) Pos() sqltoken.Pos {
	return f.From
}

func (f *Float) End() sqltoken.Pos {
	if f.IsUnsigned {
		return f.Unsigned
	}
	if f.Size != nil {
		return f.RParen
	}
	return f.To
}

func (f *Float) ToSQLString() string {
	return toSQLString(f)
}

func (f *Float) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.TypeWithOptionalLength([]byte("float"), f.Size).If(f.IsUnsigned, []byte(" unsigned"))
	return sw.End()
}

type SmallInt struct {
	From, To   sqltoken.Pos
	IsUnsigned bool
	Unsigned   sqltoken.Pos
}

func (s *SmallInt) Pos() sqltoken.Pos {
	return s.From
}

func (s *SmallInt) End() sqltoken.Pos {
	if s.IsUnsigned {
		return s.Unsigned
	}
	return s.To
}

func (s *SmallInt) ToSQLString() string {
	return toSQLString(s)
}

func (s *SmallInt) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Bytes([]byte("smallint")).If(s.IsUnsigned, []byte(" unsigned"))
	return sw.End()
}

type Int struct {
	From, To   sqltoken.Pos
	IsUnsigned bool
	Unsigned   sqltoken.Pos
}

func (i *Int) Pos() sqltoken.Pos {
	return i.From
}

func (i *Int) End() sqltoken.Pos {
	if i.IsUnsigned {
		return i.Unsigned
	}
	return i.To
}

func (i *Int) ToSQLString() string {
	return toSQLString(i)
}

func (i *Int) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Bytes([]byte("int")).If(i.IsUnsigned, []byte(" unsigned"))
	return sw.End()
}

type BigInt struct {
	From, To   sqltoken.Pos
	IsUnsigned bool
	Unsigned   sqltoken.Pos
}

func (b *BigInt) Pos() sqltoken.Pos {
	return b.From
}

func (b *BigInt) End() sqltoken.Pos {
	if b.IsUnsigned {
		return b.Unsigned
	}
	return b.To
}

func (b *BigInt) ToSQLString() string {
	return toSQLString(b)
}

func (b *BigInt) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Bytes([]byte("bigint")).If(b.IsUnsigned, []byte(" unsigned"))
	return sw.End()
}

type Real struct {
	From, To   sqltoken.Pos
	IsUnsigned bool
	Unsigned   sqltoken.Pos
}

func (r *Real) Pos() sqltoken.Pos {
	return r.From
}

func (r *Real) End() sqltoken.Pos {
	if r.IsUnsigned {
		return r.Unsigned
	}
	return r.To
}

func (r *Real) ToSQLString() string {
	return toSQLString(r)
}

func (r *Real) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Bytes([]byte("real")).If(r.IsUnsigned, []byte(" unsigned"))
	return sw.End()
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

func (*Double) WriteTo(w io.Writer) (int64, error) {
	return writeSingleBytes(w, []byte("double precision"))
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

func (*Boolean) WriteTo(w io.Writer) (int64, error) {
	return writeSingleBytes(w, []byte("boolean"))
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

func (*Date) WriteTo(w io.Writer) (int64, error) {
	return writeSingleBytes(w, []byte("date"))
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

func (*Time) WriteTo(w io.Writer) (int64, error) {
	return writeSingleBytes(w, []byte("time"))
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
	return toSQLString(t)
}

func (t *Timestamp) WriteTo(w io.Writer) (int64, error) {
	sw := newSQLWriter(w)
	sw.Bytes([]byte("timestamp")).If(t.WithTimeZone, []byte(" with time zone"))
	return sw.End()
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

func (*Regclass) WriteTo(w io.Writer) (int64, error) {
	return writeSingleBytes(w, []byte("regclass"))
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

func (*Text) WriteTo(w io.Writer) (int64, error) {
	return writeSingleBytes(w, []byte("text"))
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

func (*Bytea) WriteTo(w io.Writer) (int64, error) {
	return writeSingleBytes(w, []byte("bytea"))
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
	return toSQLString(a)
}

func (a *Array) WriteTo(w io.Writer) (int64, error) {
	return newSQLWriter(w).Node(a.Ty).Bytes([]byte("[]")).End()
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

func (c *Custom) WriteTo(w io.Writer) (int64, error) {
	return c.Ty.WriteTo(w)
}

func NewSize(s uint) *uint {
	return &s
}
