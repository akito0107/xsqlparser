package sqlastutil

import (
	"log"
	"reflect"

	"github.com/akito0107/xsqlparser/sqlast"
)

type ApplyFunc func(*Cursor) bool

var abort = new(int)

func Apply(root sqlast.Node, pre, post ApplyFunc) (result sqlast.Node) {
	parent := &struct {
		sqlast.Node
	}{root}

	defer func() {
		if r := recover(); r != nil && r != abort {
			panic(r)
		}
		result = parent.Node
	}()
	a := &application{pre: pre, post: post}
	a.apply(parent, "Node", nil, root)
	return
}

type Cursor struct {
	parent sqlast.Node
	name   string
	iter   *iterator
	node   sqlast.Node
}

func (c *Cursor) Node() sqlast.Node { return c.node }

func (c *Cursor) Parent() sqlast.Node { return c.parent }

func (c *Cursor) Name() string { return c.name }

func (c *Cursor) Index() int {
	if c.iter != nil {
		return c.iter.index
	}
	return -1
}

func (c *Cursor) field() reflect.Value {
	return reflect.Indirect(reflect.ValueOf(c.parent)).FieldByName(c.name)
}

func (c *Cursor) Replace(n sqlast.Node) {
	v := c.field()
	if i := c.Index(); i >= 0 {
		v = v.Index(i)
	}
	v.Set(reflect.ValueOf(n))
}

func (c *Cursor) Delete() {
	i := c.Index()
	if i < 0 {
		log.Fatal("delete node not contained in slice")
	}
	v := c.field()
	l := v.Len()

	reflect.Copy(v.Slice(i, l), v.Slice(i+1, l))
	v.Index(l - 1).Set(reflect.Zero(v.Type().Elem()))
	v.SetLen(l - 1)
	c.iter.step--
}

func (c *Cursor) InsertAfter(n sqlast.Node) {
	i := c.Index()
	if i < 0 {
		log.Fatal("InsertAfter node not contained in slice")
	}
	v := c.field()
	v.Set(reflect.Append(v, reflect.Zero(v.Type().Elem())))
	l := v.Len()
	reflect.Copy(v.Slice(i+2, l), v.Slice(i+1, l))
	v.Index(i + 1).Set(reflect.ValueOf(n))
	c.iter.step++
}

func (c *Cursor) InsertBefore(n sqlast.Node) {
	i := c.Index()
	if i < 0 {
		log.Fatal("InsertBefore node not contained in slice")
	}
	v := c.field()
	v.Set(reflect.Append(v, reflect.Zero(v.Type().Elem())))
	l := v.Len()
	reflect.Copy(v.Slice(i+1, l), v.Slice(i, l))
	v.Index(i).Set(reflect.ValueOf(n))
	c.iter.index++
}

type iterator struct {
	index, step int
}

type application struct {
	pre, post ApplyFunc
	cursor    Cursor
	iter      iterator
}

func (a *application) apply(parent sqlast.Node, name string, iter *iterator, n sqlast.Node) {
	if v := reflect.ValueOf(n); v.Kind() == reflect.Ptr && v.IsNil() {
		n = nil
	}

	saved := a.cursor
	a.cursor.parent = parent
	a.cursor.name = name
	a.cursor.iter = iter
	a.cursor.node = n

	if a.pre != nil && !a.pre(&a.cursor) {
		a.cursor = saved
		return
	}

	switch n := n.(type) {
	case *sqlast.File:
		a.applyList(n, "Stmts")
	case *sqlast.Ident:
		// nothing to do
	case *sqlast.Wildcard:
		// nothing to do
	case *sqlast.QualifiedWildcard:
		a.applyList(n, "Idents")
	case *sqlast.CompoundIdent:
		a.applyList(n, "Idents")
	case *sqlast.IsNull:
		a.apply(n, "X", nil, n.X)
	case *sqlast.IsNotNull:
		a.apply(n, "X", nil, n.X)
	case *sqlast.InList:
		a.apply(n, "Expr", nil, n.Expr)
		a.applyList(n, "List")
	case *sqlast.InSubQuery:
		a.apply(n, "Expr", nil, n.Expr)
		a.apply(n, "SubQuery", nil, n.SubQuery)
	case *sqlast.Between:
		a.apply(n, "Expr", nil, n.Expr)
		a.apply(n, "Low", nil, n.Low)
		a.apply(n, "High", nil, n.High)
	case *sqlast.BinaryExpr:
		a.apply(n, "Left", nil, n.Left)
		a.apply(n, "Op", nil, n.Op)
		a.apply(n, "Right", nil, n.Right)
	case *sqlast.Cast:
		a.apply(n, "Expr", nil, n.Expr)
		a.apply(n, "DateType", nil, n.DateType)
	case *sqlast.Nested:
		a.apply(n, "AST", nil, n.AST)
	case *sqlast.UnaryExpr:
		a.apply(n, "Op", nil, n.Op)
		a.apply(n, "Expr", nil, n.Expr)
	case *sqlast.Function:
		a.apply(n, "Name", nil, n.Name)
		a.applyList(n, "Args")
		if n.Over != nil {
			a.apply(n, "Over", nil, n.Over)
		}
	case *sqlast.CaseExpr:
		a.apply(n, "Operand", nil, n.Operand)
	case *sqlast.Exists:
		a.apply(n, "Query", nil, n.Query)
	case *sqlast.SubQuery:
		a.apply(n, "Query", nil, n.Query)
	case *sqlast.ObjectName:
		a.applyList(n, "Idents")
	case *sqlast.WindowSpec:
		a.applyList(n, "PartitionBy")
		a.applyList(n, "OrderBy")
		if n.WindowsFrame != nil {
			a.apply(n, "WindowsFrame", nil, n.WindowsFrame)
		}
	case *sqlast.WindowFrame:
		a.apply(n, "Units", nil, n.Units)
		a.apply(n, "StartBound", nil, n.StartBound)
		if n.EndBound != nil {
			a.apply(n, "EndBound", nil, n.EndBound)
		}
	case *sqlast.WindowFrameUnit,
		*sqlast.CurrentRow,
		*sqlast.UnboundedPreceding,
		*sqlast.UnboundedFollowing,
		*sqlast.Preceding,
		*sqlast.Following:
		// nothing to do
	case *sqlast.Query:
		a.applyList(n, "CTEs")
		a.apply(n, "Body", nil, n.Body)
		a.applyList(n, "OrderBy")
		if n.Limit != nil {
			a.apply(n, "Limit", nil, n.Limit)
		}
	case *sqlast.CTE:
		a.apply(n, "Query", nil, n.Query)
		a.apply(n, "Alias", nil, n.Alias)
	case *sqlast.SelectExpr:
		a.apply(n, "Select", nil, n.Select)
	case *sqlast.QueryExpr:
		a.apply(n, "Query", nil, n.Query)
	case *sqlast.SetOperationExpr:
		a.apply(n, "Op", nil, n.Op)
		a.apply(n, "Left", nil, n.Left)
		a.apply(n, "Right", nil, n.Right)
	case *sqlast.UnionOperator:
		// nothing to do
	case *sqlast.ExceptOperator:
		// nothing to do
	case *sqlast.IntersectOperator:
		// nothing to do
	case *sqlast.SQLSelect:
		a.applyList(n, "Projection")
		a.applyList(n, "FromClause")
		if n.WhereClause != nil {
			a.apply(n, "WhereClause", nil, n.WhereClause)
		}
		a.applyList(n, "GroupByClause")
		if n.HavingClause != nil {
			a.apply(n, "HavingClause", nil, n.HavingClause)
		}
	case *sqlast.QualifiedJoin:
		a.apply(n, "LeftElement", nil, n.LeftElement)
		a.apply(n, "Type", nil, n.Type)
		a.apply(n, "RightElement", nil, n.RightElement)
		a.apply(n, "Spec", nil, n.Spec)
	case *sqlast.TableJoinElement:
		a.apply(n, "Ref", nil, n.Ref)
	case *sqlast.JoinType:
		// nothing to do
	case *sqlast.JoinCondition:
		a.apply(n, "SearchCondition", nil, n.SearchCondition)
	case *sqlast.NaturalJoin:
		a.apply(n, "LeftElement", nil, n.LeftElement)
		a.apply(n, "Type", nil, n.Type)
		a.apply(n, "RightElement", nil, n.RightElement)
	case *sqlast.CrossJoin:
		a.apply(n, "Factor", nil, n.Factor)
		a.apply(n, "Reference", nil, n.Reference)
	case *sqlast.Table:
		a.apply(n, "Name", nil, n.Name)
		if n.Alias != nil {
			a.apply(n, "Alias", nil, n.Alias)
		}
		a.applyList(n, "Args")
		a.applyList(n, "WithHints")
	case *sqlast.Derived:
		a.apply(n, "SubQuery", nil, n.SubQuery)
		if n.Alias != nil {
			a.apply(n, "Alias", nil, n.Alias)
		}
	case *sqlast.UnnamedSelectItem:
		a.apply(n, "Node", nil, n.Node)
	case *sqlast.AliasSelectItem:
		a.apply(n, "Expr", nil, n.Expr)
		a.apply(n, "Alias", nil, n.Alias)
	case *sqlast.QualifiedWildcardSelectItem:
		a.apply(n, "Prefix", nil, n.Prefix)
	case *sqlast.WildcardSelectItem:
		// nothing to do
	case *sqlast.OrderByExpr:
		a.apply(n, "Expr", nil, n.Expr)
	case *sqlast.LimitExpr:
		if !n.All {
			a.apply(n, "LimitValue", nil, n.LimitValue)
		}
		if n.OffsetValue != nil {
			a.apply(n, "OffsetValue", nil, n.OffsetValue)
		}
	case *sqlast.CharType:
		// nothing to do
	case *sqlast.VarcharType:
		// nothing to do
	case *sqlast.UUID:
		// nothing to do
	case *sqlast.Clob:
		// nothing to do
	case *sqlast.Binary:
		// nothing to do
	case *sqlast.Varbinary:
		// nothing to do
	case *sqlast.Blob:
		// nothing to do
	case *sqlast.Decimal:
		// nothing to do
	case *sqlast.Float:
		// nothing to do
	case *sqlast.SmallInt:
		// nothing to do
	case *sqlast.Int:
		// nothing to do
	case *sqlast.BigInt:
		// nothing to do
	case *sqlast.Real:
		// nothing to do
	case *sqlast.Double:
		// nothing to do
	case *sqlast.Boolean:
		// nothing to do
	case *sqlast.Date:
		// nothing to do
	case *sqlast.Time:
		// nothing to do
	case *sqlast.Timestamp:
		// nothing to do
	case *sqlast.Regclass:
		// nothing to do
	case *sqlast.Text:
		// nothing to do
	case *sqlast.Bytea:
		// nothing to do
	case *sqlast.Array:
		// nothing to do
	case *sqlast.Custom:
		// nothing to do
	case *sqlast.InsertStmt:
		a.apply(n, "TableName", nil, n.TableName)
		a.applyList(n, "Columns")
		a.apply(n, "Source", nil, n.Source)
		a.applyList(n, "UpdateAssignments")
	case *sqlast.ConstructorSource:
		a.applyList(n, "Rows")
	case *sqlast.RowValueExpr:
		a.applyList(n, "Values")
	case *sqlast.SubQuerySource:
		a.apply(n, "SubQuery", nil, n.SubQuery)
	case *sqlast.CopyStmt:
		a.apply(n, "TableName", nil, n.TableName)
		a.applyList(n, "Columns")
	case *sqlast.UpdateStmt:
		a.apply(n, "TableName", nil, n.TableName)
		a.applyList(n, "Assignments")
		a.apply(n, "Selection", nil, n.Selection)
	case *sqlast.DeleteStmt:
		a.apply(n, "TableName", nil, n.TableName)
		if n.Selection != nil {
			a.apply(n, "Selection", nil, n.Selection)
		}
	case *sqlast.CreateViewStmt:
		a.apply(n, "Name", nil, n.Name)
		a.apply(n, "Query", nil, n.Query)
	case *sqlast.CreateTableStmt:
		a.apply(n, "Name", nil, n.Name)
		a.applyList(n, "Elements")
	case *sqlast.Assignment:
		a.apply(n, "ID", nil, n.ID)
		a.apply(n, "Value", nil, n.Value)
	case *sqlast.TableConstraint:
		if n.Name != nil {
			a.apply(n, "Name", nil, n.Name)
		}
		a.apply(n, "Spec", nil, n.Spec)
	case *sqlast.UniqueTableConstraint:
		a.applyList(n, "Columns")
	case *sqlast.ReferentialTableConstraint:
		a.applyList(n, "Columns")
		a.apply(n, "KeyExpr", nil, n.KeyExpr)
	case *sqlast.ReferenceKeyExpr:
		a.apply(n, "TableName", nil, n.TableName)
		a.applyList(n, "Columns")
	case *sqlast.CheckTableConstraint:
		a.apply(n, "Expr", nil, n.Expr)
	case *sqlast.ColumnDef:
		a.apply(n, "Name", nil, n.Name)
		a.apply(n, "DataType", nil, n.DataType)
		if n.Default != nil {
			a.apply(n, "Default", nil, n.Default)
		}
		a.applyList(n, "Constraints")
	case *sqlast.ColumnConstraint:
		if n.Name != nil {
			a.apply(n, "Name", nil, n.Name)
		}
		a.apply(n, "Spec", nil, n.Spec)
	case *sqlast.NotNullColumnSpec:
		// nothing to do
	case *sqlast.UniqueColumnSpec:
		// nothing to do
	case *sqlast.ReferencesColumnSpec:
		a.apply(n, "TableName", nil, n.TableName)
		a.applyList(n, "Columns")
	case *sqlast.CheckColumnSpec:
		a.apply(n, "Expr", nil, n.Expr)
	case *sqlast.AlterTableStmt:
		a.apply(n, "TableName", nil, n.TableName)
		a.apply(n, "Action", nil, n.Action)
	case *sqlast.AddColumnTableAction:
		a.apply(n, "Column", nil, n.Column)
	case *sqlast.AlterColumnTableAction:
		a.apply(n, "ColumnName", nil, n.ColumnName)
		a.apply(n, "Action", nil, n.Action)
	case *sqlast.SetDefaultColumnAction:
		a.apply(n, "Default", nil, n.Default)
	case *sqlast.DropDefaultColumnAction:
		// nothing to do
	case *sqlast.PGAlterDataTypeColumnAction:
		a.apply(n, "DataType", nil, n.DataType)
	case *sqlast.PGSetNotNullColumnAction:
		// nothing to do
	case *sqlast.PGDropNotNullColumnAction:
		// nothing to do
	case *sqlast.RemoveColumnTableAction:
		a.apply(n, "Name", nil, n.Name)
	case *sqlast.AddConstraintTableAction:
		a.apply(n, "Constraint", nil, n.Constraint)
	case *sqlast.DropConstraintTableAction:
		a.apply(n, "Name", nil, n.Name)
	case *sqlast.DropTableStmt:
		a.applyList(n, "TableNames")
	case *sqlast.CreateIndexStmt:
		a.apply(n, "TableName", nil, n.TableName)
		if n.IndexName != nil {
			a.apply(n, "IndexName", nil, n.IndexName)
		}
		if n.MethodName != nil {
			a.apply(n, "MethodName", nil, n.MethodName)
		}
		a.applyList(n, "ColumnNames")
		if n.Selection != nil {
			a.apply(n, "Selection", nil, n.Selection)
		}
	case *sqlast.DropIndexStmt:
		a.applyList(n, "IndexNames")
	case *sqlast.ExplainStmt:
		a.apply(n, "Stmt", nil, n.Stmt)
	case *sqlast.Operator:
		// nothing to do
	case *sqlast.NullValue,
		*sqlast.LongValue,
		*sqlast.DoubleValue,
		*sqlast.SingleQuotedString,
		*sqlast.NationalStringLiteral,
		*sqlast.BooleanValue,
		*sqlast.DateValue,
		*sqlast.TimeValue,
		*sqlast.DateTimeValue,
		*sqlast.TimestampValue:
		// nothing to do
	default:
		log.Fatalf("not implemented type %T: %+v", n, n)
	}

	if a.post != nil && !a.post(&a.cursor) {
		panic(abort)
	}
	a.cursor = saved
}

func (a *application) applyList(parent sqlast.Node, name string) {
	saved := a.iter
	a.iter.index = 0
	for {
		v := reflect.Indirect(reflect.ValueOf(parent)).FieldByName(name)
		if a.iter.index >= v.Len() {
			break
		}

		var x sqlast.Node
		if e := v.Index(a.iter.index); e.IsValid() {
			x = e.Interface().(sqlast.Node)
		}

		a.iter.step = 1
		a.apply(parent, name, &a.iter, x)
		a.iter.index += a.iter.step
	}
	a.iter = saved
}
