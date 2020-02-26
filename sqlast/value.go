package sqlast

import (
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/akito0107/xsqlparser/sqltoken"
)

type Value interface {
	Value() interface{}
	Node
}

type LongValue struct {
	From, To sqltoken.Pos
	Long     int64
}

func NewLongValue(i int64) *LongValue {
	return &LongValue{
		Long: i,
	}
}

func (l *LongValue) Pos() sqltoken.Pos {
	return l.From
}

func (l *LongValue) End() sqltoken.Pos {
	return l.To
}

func (l *LongValue) Value() interface{} {
	return l
}

func (l *LongValue) ToSQLString() string {
	return toSQLString(l)
}

func (l *LongValue) WriteTo(w io.Writer) (int64, error) {
	n, err := io.WriteString(w, strconv.FormatInt(l.Long, 10))
	return int64(n), err
}

type DoubleValue struct {
	From, To sqltoken.Pos
	Double   float64
}

func NewDoubleValue(f float64) *DoubleValue {
	return &DoubleValue{
		Double: f,
	}
}

func (d *DoubleValue) Pos() sqltoken.Pos {
	return d.From
}

func (d *DoubleValue) End() sqltoken.Pos {
	return d.To
}

func (d *DoubleValue) Value() interface{} {
	return d.Double
}

func (d *DoubleValue) ToSQLString() string {
	return toSQLString(d)
}

func (d *DoubleValue) WriteTo(w io.Writer) (int64, error) {
	var b [32] byte
	buf := strconv.AppendFloat(b[:0], d.Double, 'f', -1, 64)
	n, err := w.Write(buf)
	return int64(n), err
}

type SingleQuotedString struct {
	From, To sqltoken.Pos
	String   string
}

func NewSingleQuotedString(str string) *SingleQuotedString {
	return &SingleQuotedString{
		String: str,
	}
}

func (s *SingleQuotedString) Pos() sqltoken.Pos {
	return s.From
}

func (s *SingleQuotedString) End() sqltoken.Pos {
	return s.To
}

func (s *SingleQuotedString) Value() interface{} {
	return s.String
}

func (s *SingleQuotedString) ToSQLString() string {
	return toSQLString(s)
}

func (s *SingleQuotedString) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write([]byte("'"))
	if err != nil {
		return int64(n), err
	}
	n1, err := io.WriteString(w, s.String)
	if err != nil {
		return int64(n + n1), err
	}
	n2, err := w.Write([]byte("'"))
	return int64(n + n1 + n2), err
}

type NationalStringLiteral struct {
	From, To sqltoken.Pos
	String   string
}

func NewNationalStringLiteral(str string) *NationalStringLiteral {
	return &NationalStringLiteral{
		String: str,
	}
}

func (n *NationalStringLiteral) Pos() sqltoken.Pos {
	return n.From
}

func (n *NationalStringLiteral) End() sqltoken.Pos {
	return n.To
}

func (n *NationalStringLiteral) Value() interface{} {
	return n.String
}

func (n *NationalStringLiteral) ToSQLString() string {
	return fmt.Sprintf("N'%s'", n.String)
}

func (n *NationalStringLiteral) WriteTo(w io.Writer) (int64, error) {
	n0, err := w.Write([]byte("N'"))
	if err != nil {
		return int64(n0), err
	}
	n1, err := io.WriteString(w, n.String)
	if err != nil {
		return int64(n0 + n1), err
	}
	n2, err := w.Write([]byte("'"))
	return int64(n0 + n1 + n2), err
}

type BooleanValue struct {
	From, To sqltoken.Pos
	Boolean  bool
}

func NewBooleanValue(b bool) *BooleanValue {
	return &BooleanValue{
		Boolean: b,
	}
}

func (b *BooleanValue) Pos() sqltoken.Pos {
	return b.From
}

func (b *BooleanValue) End() sqltoken.Pos {
	return b.To
}

func (b *BooleanValue) Value() interface{} {
	return b.Boolean
}

func (b *BooleanValue) ToSQLString() string {
	return toSQLString(b)
}

func (b *BooleanValue) WriteTo(w io.Writer) (int64, error) {
	if b.Boolean {
		return writeSingleBytes(w, []byte("true"))
	} else {
		return writeSingleBytes(w, []byte("false"))
	}
}

type DateValue struct {
	From, To sqltoken.Pos
	Date     time.Time
}

func (d *DateValue) Pos() sqltoken.Pos {
	return d.From
}

func (d *DateValue) End() sqltoken.Pos {
	return d.To
}

func (d *DateValue) Value() interface{} {
	return d.Date
}

func (d *DateValue) ToSQLString() string {
	return toSQLString(d)
}

func (d *DateValue) WriteTo(w io.Writer) (int64, error) {
	var b [16]byte
	buf := d.Date.AppendFormat(b[:0], "2006-01-02")
	n, err := w.Write(buf)
	return int64(n), err
}

type TimeValue struct {
	From, To sqltoken.Pos
	Time     time.Time
}

func NewTimeValue(t time.Time) *TimeValue {
	return &TimeValue{
		Time: t,
	}
}

func (t *TimeValue) Pos() sqltoken.Pos {
	return t.From
}

func (t *TimeValue) End() sqltoken.Pos {
	return t.To
}

func (t *TimeValue) Value() interface{} {
	return t.Time
}

func (t *TimeValue) ToSQLString() string {
	return toSQLString(t)
}

func (t *TimeValue) WriteTo(w io.Writer) (int64, error) {
	var b [16]byte
	buf := t.Time.AppendFormat(b[:0], "15:04:05")
	n, err := w.Write(buf)
	return int64(n), err
}

type DateTimeValue struct {
	From, To sqltoken.Pos
	DateTime time.Time
}

func NewDateTimeValue(t time.Time) *DateTimeValue {
	return &DateTimeValue{
		DateTime: t,
	}
}

func (d *DateTimeValue) Pos() sqltoken.Pos {
	return d.From
}

func (d *DateTimeValue) End() sqltoken.Pos {
	return d.To
}

func (d *DateTimeValue) Value() interface{} {
	return d.DateTime
}

func (d *DateTimeValue) ToSQLString() string {
	return d.DateTime.Format("2006-01-02 15:04:05")
}

func (d *DateTimeValue) WriteTo(w io.Writer) (int64, error) {
	var b [32]byte
	buf := d.DateTime.AppendFormat(b[:0], "2006-01-02 15:04:05")
	n, err := w.Write(buf)
	return int64(n), err
}

type TimestampValue struct {
	From, To  sqltoken.Pos
	Timestamp time.Time
}

func NewTimestampValue(t time.Time) *TimestampValue {
	return &TimestampValue{Timestamp: t}
}

func (t *TimestampValue) Pos() sqltoken.Pos {
	return t.From
}

func (t *TimestampValue) End() sqltoken.Pos {
	return t.To
}

func (t *TimestampValue) Value() interface{} {
	return t.Timestamp
}

func (t *TimestampValue) ToSQLString() string {
	return toSQLString(t)
}

func (t *TimestampValue) WriteTo(w io.Writer) (int64, error) {
	var b [32]byte
	buf := t.Timestamp.AppendFormat(b[:0], "2006-01-02 15:04:05")
	n, err := w.Write(buf)
	return int64(n), err
}

type NullValue struct {
	From, To sqltoken.Pos
}

func NewNullValue() *NullValue {
	return &NullValue{}
}

func (n *NullValue) Pos() sqltoken.Pos {
	return n.From
}

func (n *NullValue) End() sqltoken.Pos {
	return n.To
}

func (n *NullValue) Value() interface{} {
	return nil
}

func (n *NullValue) ToSQLString() string {
	return "NULL"
}

func (*NullValue) WriteTo(w io.Writer) (int64, error) {
	return writeSingleBytes(w, []byte("NULL"))
}
