package sqlast

import (
	"testing"

	"github.com/andreyvit/diff"
)

func TestSQLSelect_Eval(t *testing.T) {
	cases := []struct {
		name string
		in   *SQLSelect
		out  string
	}{
		{
			name: "simple select",
			in: &SQLSelect{
				Projection: []SQLSelectItem{
					&Table{
						Name: &SQLObjectName{
							Idents: []SQLIdent{"test"},
						},
					},
				},
				Relation: &Table{
					Name: &SQLObjectName{
						Idents: []SQLIdent{"test_table"},
					},
				},
			},
			out: "SELECT test FROM test_table",
		},
		{
			name: "join",
			in: &SQLSelect{
				Projection: []SQLSelectItem{
					&Table{
						Name: &SQLObjectName{
							Idents: []SQLIdent{"test"},
						},
					},
				},
				Relation: &Table{
					Name: &SQLObjectName{
						Idents: []SQLIdent{"test_table"},
					},
				},
				Joins: []Join{
					{
						Relation: &Table{
							Name: &SQLObjectName{
								Idents: []SQLIdent{"test_table2"},
							},
						},
						Op:       Inner,
						Constant: &NaturalConstant{},
					},
				},
			},
			out: "SELECT test FROM test_table NATURAL JOIN test_table2",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			act := c.in.Eval()

			if act != c.out {
				t.Errorf("must be %s but %s \n diff: %s", c.out, act, diff.CharacterDiff(c.out, act))
			}
		})
	}

}
