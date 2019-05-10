package sqlast

import (
	"testing"

	"github.com/andreyvit/diff"
)

// from https://www.w3schools.com/sql/sql_insert.asp

func TestSQLInsert_Eval(t *testing.T) {
	cases := []struct {
		name string
		in   *SQLInsert
		out  string
	}{
		{
			name: "simple case",
			in: &SQLInsert{
				TableName: NewSQLObjectName("customers"),
				Columns: []*SQLIdent{
					NewSQLIdent("customer_name"),
					NewSQLIdent("contract_name"),
				},
				Values: [][]ASTNode{
					{
						NewSingleQuotedString("Cardinal"),
						NewSingleQuotedString("Tom B. Erichsen"),
					},
				},
			},
			out: "INSERT INTO customers (customer_name, contract_name) VALUES('Cardinal', 'Tom B. Erichsen')",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			act := c.in.Eval()

			if act != c.out {
				t.Errorf("must be \n%s but \n%s \n diff: %s", c.out, act, diff.CharacterDiff(c.out, act))
			}
		})
	}
}

func TestSQLUpdate_Eval(t *testing.T) {
	cases := []struct {
		name string
		in   *SQLUpdate
		out  string
	}{
		{
			name: "simple case",
			in: &SQLUpdate{
				TableName: NewSQLObjectName("customers"),
				Assignments: []*SQLAssignment{
					{
						ID:    NewSQLIdent("contract_name"),
						Value: NewSingleQuotedString("Alfred Schmidt"),
					},
					{
						ID:    NewSQLIdent("city"),
						Value: NewSingleQuotedString("Frankfurt"),
					},
				},
				Selection: &SQLBinaryExpr{
					Op:    Eq,
					Left:  NewSQLIdent("customer_id"),
					Right: NewLongValue(1),
				},
			},
			out: "UPDATE customers SET contract_name = 'Alfred Schmidt', city = 'Frankfurt' WHERE customer_id = 1",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			act := c.in.Eval()

			if act != c.out {
				t.Errorf("must be \n%s but \n%s \n diff: %s", c.out, act, diff.CharacterDiff(c.out, act))
			}
		})
	}
}

func TestSQLDelete_Eval(t *testing.T) {
	cases := []struct {
		name string
		in   *SQLDelete
		out  string
	}{
		{
			name: "simple case",
			in: &SQLDelete{
				TableName: NewSQLObjectName("customers"),
				Selection: &SQLBinaryExpr{
					Op:    Eq,
					Left:  NewSQLIdent("customer_id"),
					Right: NewLongValue(1),
				},
			},
			out: "DELETE FROM customers WHERE customer_id = 1",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			act := c.in.Eval()

			if act != c.out {
				t.Errorf("must be \n%s but \n%s \n diff: %s", c.out, act, diff.CharacterDiff(c.out, act))
			}
		})
	}
}

func TestSQLCreateView_Eval(t *testing.T) {
	cases := []struct {
		name string
		in   *SQLCreateView
		out  string
	}{
		{
			name: "simple case",
			in: &SQLCreateView{
				Name: NewSQLObjectName("customers_view"),
				Query: &SQLQuery{
					Body: &SelectExpr{
						Select: &SQLSelect{
							Projection: []SQLSelectItem{
								&UnnamedExpression{
									Node: NewSQLIdent("customer_name"),
								},
								&UnnamedExpression{
									Node: NewSQLIdent("contract_name"),
								},
							},
							Relation: NewSQLIdent("customers"),
							Selection: &SQLBinaryExpr{
								Op:    Eq,
								Left:  NewSQLIdent("country"),
								Right: NewSingleQuotedString("Brazil"),
							},
						},
					},
				},
			},
			out: "CREATE VIEW customers_view AS " +
				"SELECT customer_name, contract_name " +
				"FROM customers " +
				"WHERE country = 'Brazil'",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			act := c.in.Eval()

			if act != c.out {
				t.Errorf("must be \n%s but \n%s \n diff: %s", c.out, act, diff.CharacterDiff(c.out, act))
			}
		})
	}
}

func TestSQLCreateTable_Eval(t *testing.T) {
	cases := []struct {
		name string
		in   *SQLCreateTable
		out  string
	}{
		{
			name: "simple case",
			in: &SQLCreateTable{
				Name: NewSQLObjectName("persons"),
				Elements: []TableElement{
					&SQLColumnDef{
						Name:     NewSQLIdent("person_id"),
						DateType: &Int{},
						Constraints: []*ColumnConstraint{
							{
								Spec: &UniqueColumnSpec{
									IsPrimaryKey: true,
								},
							},
							{
								Spec: &NotNullColumnSpec{},
							},
						},
					},
					&SQLColumnDef{
						Name: NewSQLIdent("last_name"),
						DateType: &VarcharType{
							Size: NewSize(255),
						},
						Constraints: []*ColumnConstraint{
							{
								Spec: &NotNullColumnSpec{},
							},
						},
					},
					&SQLColumnDef{
						Name:     NewSQLIdent("test_id"),
						DateType: &Int{},
						Constraints: []*ColumnConstraint{
							{
								Spec: &NotNullColumnSpec{},
							},
							{
								Spec: &ReferencesColumnSpec{
									TableName: NewSQLObjectName("test"),
									Columns:   []*SQLIdent{NewSQLIdent("id1"), NewSQLIdent("id2")},
								},
							},
						},
					},
					&SQLColumnDef{
						Name: NewSQLIdent("email"),
						DateType: &VarcharType{
							Size: NewSize(255),
						},
						Constraints: []*ColumnConstraint{
							{
								Spec: &UniqueColumnSpec{},
							},
							{
								Spec: &NotNullColumnSpec{},
							},
						},
					},
					&SQLColumnDef{
						Name:     NewSQLIdent("age"),
						DateType: &Int{},
						Constraints: []*ColumnConstraint{
							{
								Spec: &NotNullColumnSpec{},
							},
							{
								Spec: &CheckColumnSpec{
									Expr: &SQLBinaryExpr{
										Op: And,
										Left: &SQLBinaryExpr{
											Op:    Gt,
											Left:  NewSQLIdent("age"),
											Right: NewLongValue(0),
										},
										Right: &SQLBinaryExpr{
											Op:    Lt,
											Left:  NewSQLIdent("age"),
											Right: NewLongValue(100),
										},
									},
								},
							},
						},
					},
					&SQLColumnDef{
						Name:     NewSQLIdent("created_at"),
						DateType: &Timestamp{},
						Default:  NewSQLIdent("CURRENT_TIMESTAMP"),
						Constraints: []*ColumnConstraint{
							{
								Spec: &NotNullColumnSpec{},
							},
						},
					},
				},
			},
			out: "CREATE TABLE persons (" +
				"person_id int PRIMARY KEY NOT NULL, " +
				"last_name character varying(255) NOT NULL, " +
				"test_id int NOT NULL REFERENCES test(id1, id2), " +
				"email character varying(255) UNIQUE NOT NULL, " +
				"age int NOT NULL CHECK(age > 0 AND age < 100), " +
				"created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL)",
		},
		{
			name: "with table constraint",
			in: &SQLCreateTable{
				Name: NewSQLObjectName("persons"),
				Elements: []TableElement{
					&SQLColumnDef{
						Name:     NewSQLIdent("person_id"),
						DateType: &Int{},
					},
					&TableConstraint{
						Name: NewSQLIdentifier(NewSQLIdent("production")),
						Spec: &UniqueTableConstraint{
							Columns: []*SQLIdent{NewSQLIdent("test_column")},
						},
					},
					&TableConstraint{
						Spec: &UniqueTableConstraint{
							Columns:   []*SQLIdent{NewSQLIdent("person_id")},
							IsPrimary: true,
						},
					},
					&TableConstraint{
						Spec: &CheckTableConstraint{
							Expr: &SQLBinaryExpr{
								Left:  NewSQLIdentifier(NewSQLIdent("id")),
								Op:    Gt,
								Right: NewLongValue(100),
							},
						},
					},
					&TableConstraint{
						Spec: &ReferentialTableConstraint{
							Columns: []*SQLIdent{NewSQLIdent("test_id")},
							KeyExpr: &ReferenceKeyExpr{
								TableName: NewSQLIdentifier(NewSQLIdent("other_table")),
								Columns:   []*SQLIdent{NewSQLIdent("col1"), NewSQLIdent("col2")},
							},
						},
					},
				},
			},
			out: "CREATE TABLE persons (" +
				"person_id int, " +
				"CONSTRAINT production UNIQUE(test_column), " +
				"PRIMARY KEY(person_id), " +
				"CHECK(id > 100), " +
				"FOREIGN KEY(test_id) REFERENCES other_table(col1, col2)" +
				")",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			act := c.in.Eval()

			if act != c.out {
				t.Errorf("must be \n%s but \n%s \n diff: %s", c.out, act, diff.CharacterDiff(c.out, act))
			}
		})
	}
}

func TestSQLAlterTable_Eval(t *testing.T) {
	cases := []struct {
		name string
		in   *SQLAlterTable
		out  string
	}{
		{
			name: "add column",
			in: &SQLAlterTable{
				TableName: NewSQLObjectName("customers"),
				Operation: &AddColumn{
					Column: &SQLColumnDef{
						Name: NewSQLIdent("email"),
						DateType: &VarcharType{
							Size: NewSize(255),
						},
						AllowNull: true,
					},
				},
			},
			out: "ALTER TABLE customers " +
				"ADD COLUMN email character varying(255)",
		},
		{
			name: "remove column",
			in: &SQLAlterTable{
				TableName: NewSQLObjectName("products"),
				Operation: &RemoveColumn{
					Name:    NewSQLIdent("description"),
					Cascade: true,
				},
			},
			out: "ALTER TABLE products " +
				"DROP COLUMN description CASCADE",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			act := c.in.Eval()

			if act != c.out {
				t.Errorf("must be \n%s but \n%s \n diff: %s", c.out, act, diff.CharacterDiff(c.out, act))
			}
		})
	}
}
