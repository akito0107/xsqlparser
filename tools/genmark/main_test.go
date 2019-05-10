package main

import (
	"testing"
)

func TestToSnake(t *testing.T) {
	ts := []struct {
		in string
		expected string
	} {
		{in: "Foo", expected: "foo"},
		{in: "FooBar", expected: "foo_bar"},
		{in: "FooB", expected: "foo_b"},
		{in: "SQLBar", expected: "sql_bar"},
		{in: "BarSQL", expected: "bar_sql"},
	}
	for _, tc := range ts {
		got := toSnake(tc.in)
		if got != tc.expected {
			t.Errorf("unexpected snake case. expected: %v, but got: %v", tc.expected, got)
		}
	}
}
