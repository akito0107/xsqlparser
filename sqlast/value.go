package sqlast

import (
	"fmt"
	"time"
)

type Value interface {
	Value() interface{}
	Node
}

type LongValue int64

func NewLongValue(i int64) *LongValue {
	l := LongValue(i)
	return &l
}

func (l *LongValue) Value() interface{} {
	return *l
}

func (l *LongValue) ToSQLString() string {
	return fmt.Sprintf("%d", *l)
}

type DoubleValue float64

func NewDoubleValue(f float64) *DoubleValue {
	d := DoubleValue(f)
	return &d
}

func (d *DoubleValue) Value() interface{} {
	return *d
}

func (d *DoubleValue) ToSQLString() string {
	return fmt.Sprintf("%f", *d)
}

type SingleQuotedString string

func NewSingleQuotedString(str string) *SingleQuotedString {
	s := SingleQuotedString(str)
	return &s
}

func (s *SingleQuotedString) Value() interface{} {
	return *s
}

func (s *SingleQuotedString) ToSQLString() string {
	return fmt.Sprintf("'%s'", *s)
}

type NationalStringLiteral string

func NewNationalStringLiteral(str string) *NationalStringLiteral {
	n := NationalStringLiteral(str)
	return &n
}

func (n *NationalStringLiteral) Value() interface{} {
	return *n
}

func (n *NationalStringLiteral) ToSQLString() string {
	return fmt.Sprintf("N'%s'", *n)
}

type BooleanValue bool

func NewBooleanValue(b bool) *BooleanValue {
	v := BooleanValue(b)
	return &v
}

func (b *BooleanValue) Value() interface{} {
	return *b
}

func (b *BooleanValue) ToSQLString() string {
	return fmt.Sprintf("%t", *b)
}

type DateValue time.Time

func (d *DateValue) Value() interface{} {
	return *d
}

func (d *DateValue) ToSQLString() string {
	return time.Time(*d).Format("2006-01-02")
}

type TimeValue time.Time

func NewTimeValue(t time.Time) *TimeValue {
	v := TimeValue(t)
	return &v
}

func (t *TimeValue) Value() interface{} {
	return *t
}

func (t *TimeValue) ToSQLString() string {
	return time.Time(*t).Format("15:04:05")
}

type DateTimeValue time.Time

func NewDateTiemValue(t time.Time) *DateTimeValue {
	v := DateTimeValue(t)
	return &v
}

func (d *DateTimeValue) Value() interface{} {
	return *d
}

func (d *DateTimeValue) ToSQLString() string {
	return time.Time(*d).Format("2006-01-02 15:04:05")
}

// TODO
type TimestampValue time.Time

func NewTimestampValue(t time.Time) *TimestampValue {
	v := TimestampValue(t)
	return &v
}

func (t *TimestampValue) Value() interface{} {
	return *t
}

func (t *TimestampValue) ToSQLString() string {
	return time.Time(*t).Format("2006-01-02 15:04:05")
}

type NullValue struct{}

func NewNullValue() *NullValue {
	return &NullValue{}
}

func (n *NullValue) Value() interface{} {
	return nil
}

func (n *NullValue) ToSQLString() string {
	return "NULL"
}
