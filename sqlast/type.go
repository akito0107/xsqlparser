package sqlast

import (
	"fmt"
)

type SQLType interface {
	ToSQLString() string
}

type CharType struct {
	Size *uint8
}

func (c *CharType) ToSQLString() string {
	return formatTypeWithOptionalLength("char", c.Size)
}

type VarcharType struct {
	Size *uint8
}

func (v *VarcharType) ToSQLString() string {
	return formatTypeWithOptionalLength("character varying", v.Size)
}

type UUID struct {
}

func (*UUID) ToSQLString() string {
	return "uuid"
}

type Clob struct {
	Size uint8
}

func (c *Clob) ToSQLString() string {
	return fmt.Sprintf("clob(%d)", c.Size)
}

type Binary struct {
	Size uint8
}

func (b *Binary) ToSQLString() string {
	return fmt.Sprintf("birany(%d)", b.Size)
}

type Varbinary struct {
	Size uint8
}

func (v *Varbinary) ToSQLString() string {
	return fmt.Sprintf("varbinary(%d)", v.Size)
}

type Blob struct {
	Size uint8
}

func (b *Blob) ToSQLString() string {
	return fmt.Sprintf("blob(%d)", b.Size)
}

type Decimal struct {
	Precision *uint8
	Scale     *uint8
}

func (d *Decimal) ToSQLString() string {
	if d.Scale != nil {
		return fmt.Sprintf("numeric(%d,%d)", *d.Precision, *d.Scale)
	}
	return formatTypeWithOptionalLength("numeric", d.Precision)
}

type Float struct {
	Size *uint8
}

func (f *Float) ToSQLString() string {
	return formatTypeWithOptionalLength("float", f.Size)
}

type SmallInt struct {
}

func (s *SmallInt) ToSQLString() string {
	return "smallint"
}

type Int struct{}

func (i *Int) ToSQLString() string {
	return "int"
}

type BigInt struct{}

func (b *BigInt) ToSQLString() string {
	return "bigint"
}

type Real struct {
}

func (*Real) ToSQLString() string {
	return "real"
}

type Double struct{}

func (*Double) ToSQLString() string {
	return "double precision"
}

type Boolean struct{}

func (*Boolean) ToSQLString() string {
	return "boolean"
}

type Date struct{}

func (*Date) ToSQLString() string {
	return "date"
}

type Time struct{}

func (*Time) ToSQLString() string {
	return "time"
}

type Timestamp struct {
	WithTimeZone bool
}

func (t *Timestamp) ToSQLString() string {
	var timezone string
	if t.WithTimeZone {
		timezone = " with timezone"
	}
	return "timestamp" + timezone
}

type Regclass struct{}

func (*Regclass) ToSQLString() string {
	return "regclass"
}

type Text struct{}

func (*Text) ToSQLString() string {
	return "text"
}

type Bytea struct{}

func (*Bytea) ToSQLString() string {
	return "bytea"
}

type Array struct {
	Ty SQLType
}

func (a *Array) ToSQLString() string {
	return fmt.Sprintf("%s[]", a.Ty.ToSQLString())
}

type Custom struct {
	Ty *SQLObjectName
}

func (c *Custom) ToSQLString() string {
	return c.Ty.ToSQLString()
}

func formatTypeWithOptionalLength(sqltype string, len *uint8) string {
	s := sqltype
	if len != nil {
		s += fmt.Sprintf("(%d)", *len)
	}

	return s
}

func NewSize(s uint8) *uint8 {
	return &s
}
