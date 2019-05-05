package sqlast

import (
	"fmt"
)

type SQLType interface {
	Eval() string
}

type CharType struct {
	Size *uint8
}

func (c *CharType) Eval() string {
	return formatTypeWithOptionalLength("char", c.Size)
}

type VarcharType struct {
	Size *uint8
}

func (v *VarcharType) Eval() string {
	return formatTypeWithOptionalLength("character varying", v.Size)
}

type UUID struct {
}

func (*UUID) Eval() string {
	return "uuid"
}

type Clob struct {
	Size uint8
}

func (c *Clob) Eval() string {
	return fmt.Sprintf("clob(%d)", c.Size)
}

type Binary struct {
	Size uint8
}

func (b *Binary) Eval() string {
	return fmt.Sprintf("birany(%d)", b.Size)
}

type Varbinary struct {
	Size uint8
}

func (v *Varbinary) Eval() string {
	return fmt.Sprintf("varbinary(%d)", v.Size)
}

type Blob struct {
	Size uint8
}

func (b *Blob) Eval() string {
	return fmt.Sprintf("blob(%d)", b.Size)
}

type Decimal struct {
	Precision *uint8
	Scale     *uint8
}

func (d *Decimal) Eval() string {
	if d.Scale != nil {
		return fmt.Sprintf("numeric(%d,%d)", *d.Precision, *d.Scale)
	}
	return formatTypeWithOptionalLength("numeric", d.Precision)
}

type Float struct {
	Size *uint8
}

func (f *Float) Eval() string {
	return formatTypeWithOptionalLength("float", f.Size)
}

type SmallInt struct {
}

func (s *SmallInt) Eval() string {
	return "smallint"
}

type Int struct{}

func (i *Int) Eval() string {
	return "int"
}

type BigInt struct{}

func (b *BigInt) Eval() string {
	return "bigint"
}

type Real struct {
}

func (*Real) Eval() string {
	return "real"
}

type Double struct{}

func (*Double) Eval() string {
	return "double"
}

type Boolean struct{}

func (*Boolean) Eval() string {
	return "boolean"
}

type Date struct{}

func (*Date) Eval() string {
	return "date"
}

type Time struct{}

func (*Time) Eval() string {
	return "time"
}

type Timestamp struct{}

func (*Timestamp) Eval() string {
	return "timestamp"
}

type Regclass struct{}

func (*Regclass) Eval() string {
	return "regclass"
}

type Text struct{}

func (*Text) Eval() string {
	return "text"
}

type Bytea struct{}

func (*Bytea) Eval() string {
	return "bytea"
}

type Array struct {
	Ty SQLType
}

func (a *Array) Eval() string {
	return fmt.Sprintf("%s[]", a.Ty.Eval())
}

type Custom struct {
	Ty SQLObjectName
}

func (c *Custom) Eval() string {
	return c.Ty.Eval()
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
