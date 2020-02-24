package sqlast

import (
	"io"
	"strconv"
	"strings"
)

type sqlWriter struct {
	w   io.Writer
	n   int64
	err error
}

func newSQLWriter(w io.Writer) *sqlWriter {
	return &sqlWriter{w: w}
}

var selectBytes = []byte("SELECT ")
var fromBytes = []byte(" FROM ")
var whereBytes = []byte(" WHERE ")
var wildcardBytes = []byte("*")
var dotBytes = []byte(".")
var spaceBytes = []byte(" ")

func (w *sqlWriter) Bytes(b []byte) *sqlWriter {
	if w.err != nil {
		return w
	}
	n, err := w.w.Write(b)
	w.n += int64(n)
	if err != nil {
		w.err = err
	}
	return w
}

func (w *sqlWriter) Space() *sqlWriter {
	return w.Bytes(spaceBytes)
}

func (w *sqlWriter) LParen() *sqlWriter {
	return w.Bytes([]byte("("))
}

func (w *sqlWriter) RParen() *sqlWriter {
	return w.Bytes([]byte(")"))
}

func (w *sqlWriter) Int(i int) *sqlWriter {
	if w.err != nil {
		return w
	}
	var buf [32]byte
	b := buf[:0]
	b = strconv.AppendInt(b, int64(i), 10)
	n, err := w.w.Write(b)
	w.n += int64(n)
	if err != nil {
		w.err = err
	}
	return w
}

func (w *sqlWriter) Node(wt io.WriterTo) *sqlWriter {
	if w.err != nil {
		return w
	}
	n, err := wt.WriteTo(w.w)
	w.n += n
	if err != nil {
		w.err = err
	}
	return w
}

func (w *sqlWriter) Join(i int, wt io.WriterTo, sep []byte) *sqlWriter {
	if i > 0 {
		w.Bytes(sep)
	}
	return w.Node(wt)
}

func (w *sqlWriter) JoinComma(i int, wt io.WriterTo) *sqlWriter {
	if i > 0 {
		w.Bytes([]byte(", "))
	}
	return w.Node(wt)
}

func (w *sqlWriter) JoinNewLine(i int, wt io.WriterTo) *sqlWriter {
	if i > 0 {
		w.Bytes([]byte("\n"))
	}
	return w.Node(wt)
}

func (w *sqlWriter) Idents(idents []*Ident, sep []byte) *sqlWriter {
	if w.err != nil {
		return w
	}
	sw, ok := w.w.(io.StringWriter)
	if ok {
		for i, ident := range idents {
			if i > 0 {
				w.Bytes(sep)
			}
			if w.err == nil {
				w.Direct(ident.WriteStringTo(sw))
			}
		}
		return w
	}
	for i, ident := range idents {
		w.Join(i, ident, sep)
	}
	return w
}

func (w *sqlWriter) Nodes(nodes []Node) *sqlWriter {
	if w.err != nil {
		return w
	}
	for i, node := range nodes {
		w.Join(i, node, []byte(", "))
	}
	return w
}

func (w *sqlWriter) TypeWithOptionalLength(sqltype []byte, size *uint) *sqlWriter {
	w.Bytes(sqltype)
	if size != nil {
		w.Bytes([]byte("(")).Int(int(*size)).Bytes([]byte(")"))
	}
	return w
}

func (w *sqlWriter) Negated(negated bool) *sqlWriter {
	return w.If(negated, []byte("NOT "))
}

func (w *sqlWriter) If(ok bool, b []byte) *sqlWriter {
	if ok {
		w.Bytes(b)
	}
	return w
}

func (w *sqlWriter) As() *sqlWriter {
	return w.Bytes([]byte(" AS "))
}

func (w *sqlWriter) End() (int64, error) {
	return w.n, w.err
}

func (w *sqlWriter) Err() error {
	return w.err
}

func (w *sqlWriter) Direct(n int64, err error) *sqlWriter {
	w.n += n
	if err != nil {
		w.err = err
	}
	return w
}

func writeSingleBytes(w io.Writer, b []byte) (int64, error) {
	n, err := w.Write(b)
	return int64(n), err
}

func writeSingleString(w io.Writer, s string) (int64, error) {
	n, err := io.WriteString(w, s)
	return int64(n), err
}

func toSQLString(n Node) string {
	var b strings.Builder
	_, _ = n.WriteTo(&b)
	return b.String()
}
