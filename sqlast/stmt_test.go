package sqlast

import (
	"testing"

	"github.com/andreyvit/diff"
)

// from https://www.w3schools.com/sql/sql_insert.asp

func TestSQLInsert_ToSQLString(t *testing.T) {
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
			act := c.in.ToSQLString()

			if act != c.out {
				t.Errorf("must be \n%s but \n%s \n diff: %s", c.out, act, diff.CharacterDiff(c.out, act))
			}
		})
	}
}

func TestSQLUpdate_ToSQLString(t *testing.T) {
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
			act := c.in.ToSQLString()

			if act != c.out {
				t.Errorf("must be \n%s but \n%s \n diff: %s", c.out, act, diff.CharacterDiff(c.out, act))
			}
		})
	}
}

func TestSQLDelete_ToSQLString(t *testing.T) {
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
			act := c.in.ToSQLString()

			if act != c.out {
				t.Errorf("must be \n%s but \n%s \n diff: %s", c.out, act, diff.CharacterDiff(c.out, act))
			}
		})
	}
}

func TestSQLCreateView_ToSQLString(t *testing.T) {
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
							Relation: &Table{
								Name: &SQLObjectName{
									Idents: []*SQLIdent{
										NewSQLIdent("customers"),
									},
								},
							},
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
			act := c.in.ToSQLString()

			if act != c.out {
				t.Errorf("must be \n%s but \n%s \n diff: %s", c.out, act, diff.CharacterDiff(c.out, act))
			}
		})
	}
}

func TestSQLCreateTable_ToSQLString(t *testing.T) {
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
						DataType: &Int{},
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
						DataType: &VarcharType{
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
						DataType: &Int{},
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
						DataType: &VarcharType{
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
						DataType: &Int{},
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
						DataType: &Timestamp{},
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
						DataType: &Int{},
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
		{
			name: "NotExists",
			in: &SQLCreateTable{
				Name:      NewSQLObjectName("persons"),
				NotExists: true,
				Elements: []TableElement{
					&SQLColumnDef{
						Name:     NewSQLIdent("person_id"),
						DataType: &Int{},
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
						DataType: &VarcharType{
							Size: NewSize(255),
						},
						Constraints: []*ColumnConstraint{
							{
								Spec: &NotNullColumnSpec{},
							},
						},
					},
					&SQLColumnDef{
						Name:     NewSQLIdent("created_at"),
						DataType: &Timestamp{},
						Default:  NewSQLIdent("CURRENT_TIMESTAMP"),
						Constraints: []*ColumnConstraint{
							{
								Spec: &NotNullColumnSpec{},
							},
						},
					},
				},
			},
			out: "CREATE TABLE IF NOT EXISTS persons (" +
				"person_id int PRIMARY KEY NOT NULL, " +
				"last_name character varying(255) NOT NULL, " +
				"created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL)",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			act := c.in.ToSQLString()

			if act != c.out {
				t.Errorf("must be \n%s but \n%s \n diff: %s", c.out, act, diff.CharacterDiff(c.out, act))
			}
		})
	}
}

func TestSQLAlterTable_ToSQLString(t *testing.T) {
	cases := []struct {
		name string
		in   *SQLAlterTable
		out  string
	}{
		{
			name: "add column",
			in: &SQLAlterTable{
				TableName: NewSQLObjectName("customers"),
				Action: &AddColumnTableAction{
					Column: &SQLColumnDef{
						Name: NewSQLIdent("email"),
						DataType: &VarcharType{
							Size: NewSize(255),
						},
					},
				},
			},
			out: "ALTER TABLE customers " +
				"ADD COLUMN email character varying(255)",
		},
		{
			name: "add column over uint8",
			in: &SQLAlterTable{
				TableName: NewSQLObjectName("customers"),
				Action: &AddColumnTableAction{
					Column: &SQLColumnDef{
						Name: NewSQLIdent("email"),
						DataType: &VarcharType{
							Size: NewSize(256),
						},
					},
				},
			},
			out: "ALTER TABLE customers " +
				"ADD COLUMN email character varying(256)",
		},
		{
			name: "remove column",
			in: &SQLAlterTable{
				TableName: NewSQLObjectName("products"),
				Action: &RemoveColumnTableAction{
					Name:    NewSQLIdent("description"),
					Cascade: true,
				},
			},
			out: "ALTER TABLE products " +
				"DROP COLUMN description CASCADE",
		},
		{
			name: "add constraint",
			in: &SQLAlterTable{
				TableName: NewSQLObjectName("products"),
				Action: &AddConstraintTableAction{
					Constraint: &TableConstraint{
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
			out: "ALTER TABLE products " +
				"ADD FOREIGN KEY(test_id) REFERENCES other_table(col1, col2)",
		},
		{
			name: "alter column",
			in: &SQLAlterTable{
				TableName: NewSQLObjectName("products"),
				Action: &AlterColumnTableAction{
					ColumnName: NewSQLIdent("created_at"),
					Action: &SetDefaultColumnAction{
						Default: NewSQLIdentifier(NewSQLIdent("current_timestamp")),
					},
				},
			},
			out: "ALTER TABLE products " +
				"ALTER COLUMN created_at SET DEFAULT current_timestamp",
		},
		{
			name: "pg change type",
			in: &SQLAlterTable{
				TableName: NewSQLObjectName("products"),
				Action: &AlterColumnTableAction{
					ColumnName: NewSQLIdent("number"),
					Action: &PGAlterDataTypeColumnAction{
						DataType: &Decimal{
							Scale:     NewSize(10),
							Precision: NewSize(255),
						},
					},
				},
			},
			out: "ALTER TABLE products " +
				"ALTER COLUMN number TYPE numeric(255,10)",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			act := c.in.ToSQLString()

			if act != c.out {
				t.Errorf("must be \n%s but \n%s \n diff: %s", c.out, act, diff.CharacterDiff(c.out, act))
			}
		})
	}
}
