package sqlast

import (
	"testing"

	"github.com/andreyvit/diff"
)

// from https://www.w3schools.com/sql/sql_insert.asp

func TestSQLInsert_ToSQLString(t *testing.T) {
	cases := []struct {
		name string
		in   *InsertStmt
		out  string
	}{
		{
			name: "simple case",
			in: &InsertStmt{
				TableName: NewObjectName("customers"),
				Columns: []*Ident{
					NewIdent("customer_name"),
					NewIdent("contract_name"),
				},
				Values: [][]Node{
					{
						NewSingleQuotedString("Cardinal"),
						NewSingleQuotedString("Tom B. Erichsen"),
					},
				},
			},
			out: "INSERT INTO customers (customer_name, contract_name) VALUES ('Cardinal', 'Tom B. Erichsen')",
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
		in   *UpdateStmt
		out  string
	}{
		{
			name: "simple case",
			in: &UpdateStmt{
				TableName: NewObjectName("customers"),
				Assignments: []*Assignment{
					{
						ID:    NewIdent("contract_name"),
						Value: NewSingleQuotedString("Alfred Schmidt"),
					},
					{
						ID:    NewIdent("city"),
						Value: NewSingleQuotedString("Frankfurt"),
					},
				},
				Selection: &BinaryExpr{
					Op:    Eq,
					Left:  NewIdent("customer_id"),
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
		in   *DeleteStmt
		out  string
	}{
		{
			name: "simple case",
			in: &DeleteStmt{
				TableName: NewObjectName("customers"),
				Selection: &BinaryExpr{
					Op:    Eq,
					Left:  NewIdent("customer_id"),
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
		in   *CreateViewStmt
		out  string
	}{
		{
			name: "simple case",
			in: &CreateViewStmt{
				Name: NewObjectName("customers_view"),
				Query: &Query{
					Body: &SelectExpr{
						Select: &SQLSelect{
							Projection: []SQLSelectItem{
								&UnnamedSelectItem{
									Node: NewIdent("customer_name"),
								},
								&UnnamedSelectItem{
									Node: NewIdent("contract_name"),
								},
							},
							FromClause: []TableReference{
								&Table{
									Name: &ObjectName{
										Idents: []*Ident{
											NewIdent("customers"),
										},
									},
								},
							},
							WhereClause: &BinaryExpr{
								Op:    Eq,
								Left:  NewIdent("country"),
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
		in   *CreateTableStmt
		out  string
	}{
		{
			name: "simple case",
			in: &CreateTableStmt{
				Name: NewObjectName("persons"),
				Elements: []TableElement{
					&ColumnDef{
						Name:     NewIdent("person_id"),
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
					&ColumnDef{
						Name: NewIdent("last_name"),
						DataType: &VarcharType{
							Size: NewSize(255),
						},
						Constraints: []*ColumnConstraint{
							{
								Spec: &NotNullColumnSpec{},
							},
						},
					},
					&ColumnDef{
						Name:     NewIdent("test_id"),
						DataType: &Int{},
						Constraints: []*ColumnConstraint{
							{
								Spec: &NotNullColumnSpec{},
							},
							{
								Spec: &ReferencesColumnSpec{
									TableName: NewObjectName("test"),
									Columns:   []*Ident{NewIdent("id1"), NewIdent("id2")},
								},
							},
						},
					},
					&ColumnDef{
						Name: NewIdent("email"),
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
					&ColumnDef{
						Name:     NewIdent("age"),
						DataType: &Int{},
						Constraints: []*ColumnConstraint{
							{
								Spec: &NotNullColumnSpec{},
							},
							{
								Spec: &CheckColumnSpec{
									Expr: &BinaryExpr{
										Op: And,
										Left: &BinaryExpr{
											Op:    Gt,
											Left:  NewIdent("age"),
											Right: NewLongValue(0),
										},
										Right: &BinaryExpr{
											Op:    Lt,
											Left:  NewIdent("age"),
											Right: NewLongValue(100),
										},
									},
								},
							},
						},
					},
					&ColumnDef{
						Name:     NewIdent("created_at"),
						DataType: &Timestamp{},
						Default:  NewIdent("CURRENT_TIMESTAMP"),
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
			in: &CreateTableStmt{
				Name: NewObjectName("persons"),
				Elements: []TableElement{
					&ColumnDef{
						Name:     NewIdent("person_id"),
						DataType: &Int{},
					},
					&TableConstraint{
						Name: NewIdent("production"),
						Spec: &UniqueTableConstraint{
							Columns: []*Ident{NewIdent("test_column")},
						},
					},
					&TableConstraint{
						Spec: &UniqueTableConstraint{
							Columns:   []*Ident{NewIdent("person_id")},
							IsPrimary: true,
						},
					},
					&TableConstraint{
						Spec: &CheckTableConstraint{
							Expr: &BinaryExpr{
								Left:  NewIdent("id"),
								Op:    Gt,
								Right: NewLongValue(100),
							},
						},
					},
					&TableConstraint{
						Spec: &ReferentialTableConstraint{
							Columns: []*Ident{NewIdent("test_id")},
							KeyExpr: &ReferenceKeyExpr{
								TableName: NewIdent("other_table"),
								Columns:   []*Ident{NewIdent("col1"), NewIdent("col2")},
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
			in: &CreateTableStmt{
				Name:      NewObjectName("persons"),
				NotExists: true,
				Elements: []TableElement{
					&ColumnDef{
						Name:     NewIdent("person_id"),
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
					&ColumnDef{
						Name: NewIdent("last_name"),
						DataType: &VarcharType{
							Size: NewSize(255),
						},
						Constraints: []*ColumnConstraint{
							{
								Spec: &NotNullColumnSpec{},
							},
						},
					},
					&ColumnDef{
						Name:     NewIdent("created_at"),
						DataType: &Timestamp{},
						Default:  NewIdent("CURRENT_TIMESTAMP"),
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
		in   *AlterTableStmt
		out  string
	}{
		{
			name: "add column",
			in: &AlterTableStmt{
				TableName: NewObjectName("customers"),
				Action: &AddColumnTableAction{
					Column: &ColumnDef{
						Name: NewIdent("email"),
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
			in: &AlterTableStmt{
				TableName: NewObjectName("customers"),
				Action: &AddColumnTableAction{
					Column: &ColumnDef{
						Name: NewIdent("email"),
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
			in: &AlterTableStmt{
				TableName: NewObjectName("products"),
				Action: &RemoveColumnTableAction{
					Name:    NewIdent("description"),
					Cascade: true,
				},
			},
			out: "ALTER TABLE products " +
				"DROP COLUMN description CASCADE",
		},
		{
			name: "add constraint",
			in: &AlterTableStmt{
				TableName: NewObjectName("products"),
				Action: &AddConstraintTableAction{
					Constraint: &TableConstraint{
						Spec: &ReferentialTableConstraint{
							Columns: []*Ident{NewIdent("test_id")},
							KeyExpr: &ReferenceKeyExpr{
								TableName: NewIdent("other_table"),
								Columns:   []*Ident{NewIdent("col1"), NewIdent("col2")},
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
			in: &AlterTableStmt{
				TableName: NewObjectName("products"),
				Action: &AlterColumnTableAction{
					ColumnName: NewIdent("created_at"),
					Action: &SetDefaultColumnAction{
						Default: NewIdent("current_timestamp"),
					},
				},
			},
			out: "ALTER TABLE products " +
				"ALTER COLUMN created_at SET DEFAULT current_timestamp",
		},
		{
			name: "pg change type",
			in: &AlterTableStmt{
				TableName: NewObjectName("products"),
				Action: &AlterColumnTableAction{
					ColumnName: NewIdent("number"),
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

func TestSQLCreateIndex_ToSQLString(t *testing.T) {
	cases := []struct {
		name string
		in   *CreateIndexStmt
		out  string
	}{
		{
			name: "create index",
			in: &CreateIndexStmt{
				TableName:   NewObjectName("customers"),
				ColumnNames: []*Ident{NewIdent("name")},
			},
			out: "CREATE INDEX ON customers (name)",
		},
		{
			name: "create unique index",
			in: &CreateIndexStmt{
				TableName:   NewObjectName("customers"),
				IsUnique:    true,
				ColumnNames: []*Ident{NewIdent("name")},
			},
			out: "CREATE UNIQUE INDEX ON customers (name)",
		},
		{
			name: "create index with name",
			in: &CreateIndexStmt{
				TableName:   NewObjectName("customers"),
				IndexName:   NewIdent("customers_idx"),
				IsUnique:    true,
				ColumnNames: []*Ident{NewIdent("name"), NewIdent("email")},
			},
			out: "CREATE UNIQUE INDEX customers_idx ON customers (name, email)",
		},
		{
			name: "create index with name",
			in: &CreateIndexStmt{
				TableName:   NewObjectName("customers"),
				IndexName:   NewIdent("customers_idx"),
				IsUnique:    true,
				MethodName:  NewIdent("gist"),
				ColumnNames: []*Ident{NewIdent("name")},
			},
			out: "CREATE UNIQUE INDEX customers_idx ON customers USING gist (name)",
		},
		{
			name: "create partial index with name",
			in: &CreateIndexStmt{
				TableName:   NewObjectName("customers"),
				IndexName:   NewIdent("customers_idx"),
				IsUnique:    true,
				MethodName:  NewIdent("gist"),
				ColumnNames: []*Ident{NewIdent("name")},
				Selection: &BinaryExpr{
					Left:  NewIdent("name"),
					Op:    Eq,
					Right: NewSingleQuotedString("test"),
				},
			},
			out: "CREATE UNIQUE INDEX customers_idx ON customers USING gist (name) WHERE name = 'test'",
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
