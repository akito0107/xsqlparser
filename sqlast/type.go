package sqlast

import (
	"fmt"
	"time"
)

type Value interface {
	Value() interface{}
	Eval() string
}

type LongValue int64

func (l *LongValue) Value() interface{} {
	return *l
}

func (l *LongValue) Eval() string {
	return fmt.Sprintf("%d", *l)
}

type DoubleValue float64

func (d *DoubleValue) Value() interface{} {
	return *d
}

func (d *DoubleValue) Eval() string {
	return fmt.Sprintf("%f", *d)
}

type SingleQuotedString string

func (s *SingleQuotedString) Value() interface{} {
	return *s
}

func (s *SingleQuotedString) Eval() interface{} {
	return fmt.Sprintf("%s", *s)
}

type NationalStringLiteral string

func (n *NationalStringLiteral) Value() interface{} {
	return *n
}

func (n *NationalStringLiteral) Eval() string {
	return fmt.Sprintf("N'%s'", *n)
}

type BooleanValue bool

func (b *BooleanValue) Value() interface{} {
	return *b
}

func (b *BooleanValue) Eval() string {
	return fmt.Sprintf("%t", *b)
}

type DateValue time.Time

func (d *DateValue) Value() interface{} {
	return *d
}

func (d *DateValue) Eval() string {
	return time.Time(*d).Format("2006-01-02")
}

type TimeValue time.Time

func (t *TimeValue) Value() interface{} {
	return *t
}

func (t *TimeValue) Eval() string {
	return time.Time(*t).Format("15:04:05")
}

type DateTimeValue time.Time

func (d *DateTimeValue) Value() interface{} {
	return *d
}

func (d *DateTimeValue) Eval() string {
	return time.Time(*d).Format("2006-01-02 15:04:05")
}

// TODO
type TimestampValue time.Time

func (t *TimestampValue) Value() interface{} {
	return *t
}

func (t *TimestampValue) Eval() string {
	return time.Time(*t).Format("2006-01-02 15:04:05")
}

type NullValue struct{}

func (n *NullValue) Value() interface{} {
	return nil
}

func (n *NullValue) Eval() string {
	return "NULL"
}
