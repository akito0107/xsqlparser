package xsqlparser

import (
	"reflect"
	"unicode"

	"github.com/google/go-cmp/cmp"
)

var IgnoreMarker = cmp.FilterPath(func(paths cmp.Path) bool {
	s := paths.Last().Type()
	name := s.Name()
	r := []rune(name)
	return s.Kind() == reflect.Struct && len(r) > 0 && unicode.IsLower(r[0])
}, cmp.Ignore())

func CompareWithoutMarker(a, b interface{}) string {
	return cmp.Diff(a, b, IgnoreMarker)
}