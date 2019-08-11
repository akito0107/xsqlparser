package astutil

import (
	"github.com/akito0107/xsqlparser/sqlast"
	"log"
	"reflect"
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
	curosor   Cursor
	iter      iterator
}

func (a *application) apply(parent sqlast.Node, name string, iter *iterator, n sqlast.Node) {
	if v := reflect.ValueOf(n); v.Kind() == reflect.Ptr && v.IsNil() {
		n = nil
	}

	saved := a.curosor
	a.curosor.parent = parent
	a.curosor.name = name
	a.curosor.iter = iter
	a.curosor.node = n

	if a.pre != nil && !a.pre(&a.curosor) {
		a.curosor = saved
		return
	}

	switch n := n.(type) {
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
	case *sqlast.Unary:
		a.apply(n, "Operator", nil, n.Operator)
		a.apply(n, "Expr", nil, n.Expr)
	case *sqlast.Function:
		Walk(v, n.Name)
		walkASTNodeLists(v, n.Args)
		if n.Over != nil {
			Walk(v, n.Over)
		}
	case *sqlast.CaseExpr:
		Walk(v, n.Operand)
	case *sqlast.Exists:
		Walk(v, n.Query)
	case *sqlast.SubQuery:
		Walk(v, n.Query)
	case *sqlast.ObjectName:
		walkIdentLists(v, n.Idents)
	case *sqlast.WindowSpec:
		walkASTNodeLists(v, n.PartitionBy)
		for _, o := range n.OrderBy {
			Walk(v, o)
		}
		if n.WindowsFrame != nil {
			Walk(v, n.WindowsFrame)
		}
	case *sqlast.WindowFrame:
		Walk(v, n.Units)
		Walk(v, n.StartBound)
		if n.EndBound != nil {
			Walk(v, n.EndBound)
		}
	case sqlast.WindowFrameUnits:
		// nothing to do
	case *sqlast.CurrentRow:
		// nothing to do
	case *sqlast.UnboundedPreceding:
		// nothing to do
	case *sqlast.UnboundedFollowing:
		// nothing to do
	case *sqlast.Preceding:
		// nothing to do
	case *sqlast.Following:
		// nothing to do
	case *sqlast.Query:
		for _, c := range n.CTEs {
			Walk(v, c)
		}
		Walk(v, n.Body)
		for _, o := range n.OrderBy {
			Walk(v, o)
		}
		if n.Limit != nil {
			Walk(v, n.Limit)
		}
	case *sqlast.CTE:
		Walk(v, n.Query)
		Walk(v, n.Alias)
	case *sqlast.SelectExpr:
		Walk(v, n.Select)
	case *sqlast.QueryExpr:
		Walk(v, n.Query)
	case *sqlast.SetOperationExpr:
		Walk(v, n.Op)
		Walk(v, n.Left)
		Walk(v, n.Right)
	case *sqlast.UnionOperator:
		// nothing to do
	case *sqlast.ExceptOperator:
		// nothing to do
	case *sqlast.IntersectOperator:
		// nothing to do
	case *sqlast.SQLSelect:
		for _, p := range n.Projection {
			Walk(v, p)
		}
		if len(n.FromClause) != 0 {
			for _, f := range n.FromClause {
				Walk(v, f)
			}
		}
		if n.WhereClause != nil {
			Walk(v, n.WhereClause)
		}
		walkASTNodeLists(v, n.GroupByClause)
		if n.HavingClause != nil {
			Walk(v, n.HavingClause)
		}
	case *sqlast.QualifiedJoin:
		Walk(v, n.LeftElement)
		Walk(v, n.Type)
		Walk(v, n.RightElement)
		Walk(v, n.Spec)
	case *sqlast.TableJoinElement:
		Walk(v, n.Ref)
	case sqlast.JoinType:
		// nothing to do
	case *sqlast.JoinCondition:
		Walk(v, n.SearchCondition)
	case *sqlast.NaturalJoin:
		Walk(v, n.LeftElement)
		Walk(v, n.Type)
		Walk(v, n.RightElement)
	case *sqlast.CrossJoin:
		Walk(v, n.Factor)
		Walk(v, n.Reference)
	case *sqlast.Table:
		Walk(v, n.Name)
		if n.Alias != nil {
			Walk(v, n.Alias)
		}
		walkASTNodeLists(v, n.Args)
		walkASTNodeLists(v, n.WithHints)
	case *sqlast.Derived:
		Walk(v, n.SubQuery)
		if n.Alias != nil {
			Walk(v, n.Alias)
		}
	case *sqlast.UnnamedSelectItem:
		Walk(v, n.Node)
	case *sqlast.AliasSelectItem:
		Walk(v, n.Expr)
		Walk(v, n.Alias)
	case *sqlast.QualifiedWildcardSelectItem:
		Walk(v, n.Prefix)
	case *sqlast.WildcardSelectItem:
		// nothing to do
	case *sqlast.OrderByExpr:
		Walk(v, n.Expr)
	case *sqlast.LimitExpr:
		if !n.All {
			Walk(v, n.LimitValue)
		}
		if n.OffsetValue != nil {
			Walk(v, n.OffsetValue)
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
		Walk(v, n.TableName)
		walkIdentLists(v, n.Columns)
	case *sqlast.CopyStmt:
		Walk(v, n.TableName)
		walkIdentLists(v, n.Columns)
	case *sqlast.UpdateStmt:
		Walk(v, n.TableName)
		for _, a := range n.Assignments {
			Walk(v, a)
		}
		Walk(v, n.Selection)
	case *sqlast.DeleteStmt:
		Walk(v, n.TableName)
		Walk(v, n.Selection)
	case *sqlast.CreateViewStmt:
		Walk(v, n.Name)
		Walk(v, n.Query)
	case *sqlast.CreateTableStmt:
		Walk(v, n.Name)
		for _, e := range n.Elements {
			Walk(v, e)
		}
	case *sqlast.Assignment:
		Walk(v, n.ID)
		Walk(v, n.Value)
	case *sqlast.TableConstraint:
		if n.Name != nil {
			Walk(v, n.Name)
		}
		Walk(v, n.Spec)
	case *sqlast.UniqueTableConstraint:
		walkIdentLists(v, n.Columns)
	case *sqlast.ReferentialTableConstraint:
		walkIdentLists(v, n.Columns)
		Walk(v, n.KeyExpr)
	case *sqlast.ReferenceKeyExpr:
		Walk(v, n.TableName)
		walkIdentLists(v, n.Columns)
	case *sqlast.CheckTableConstraint:
		Walk(v, n.Expr)
	case *sqlast.ColumnDef:
		Walk(v, n.Name)
		Walk(v, n.DataType)
		if n.Default != nil {
			Walk(v, n.Default)
		}
		for _, c := range n.Constraints {
			Walk(v, c)
		}
	case *sqlast.ColumnConstraint:
		if n.Name != nil {
			Walk(v, n.Name)
		}
		Walk(v, n.Spec)
	case *sqlast.NotNullColumnSpec:
		// nothing to do
	case *sqlast.UniqueColumnSpec:
		// nothing to do
	case *sqlast.ReferencesColumnSpec:
		Walk(v, n.TableName)
		walkIdentLists(v, n.Columns)
	case *sqlast.CheckColumnSpec:
		Walk(v, n.Expr)
	case *sqlast.AlterTableStmt:
		Walk(v, n.TableName)
		Walk(v, n.Action)
	case *sqlast.AddColumnTableAction:
		Walk(v, n.Column)
	case *sqlast.AlterColumnTableAction:
		Walk(v, n.ColumnName)
		Walk(v, n.Action)
	case *sqlast.SetDefaultColumnAction:
		Walk(v, n.Default)
	case *sqlast.DropDefaultColumnAction:
		// nothing to do
	case *sqlast.PGAlterDataTypeColumnAction:
		Walk(v, n.DataType)
	case *sqlast.PGSetNotNullColumnAction:
		// nothing to do
	case *sqlast.PGDropNotNullColumnAction:
		// nothing to do
	case *sqlast.RemoveColumnTableAction:
		Walk(v, n.Name)
	case *sqlast.AddConstraintTableAction:
		Walk(v, n.Constraint)
	case *sqlast.DropConstraintTableAction:
		Walk(v, n.Name)
	case *sqlast.DropTableStmt:
		for _, t := range n.TableNames {
			Walk(v, t)
		}
	case *sqlast.CreateIndexStmt:
		Walk(v, n.TableName)
		if n.IndexName != nil {
			Walk(v, n.IndexName)
		}
		if n.MethodName != nil {
			Walk(v, n.MethodName)
		}
		walkIdentLists(v, n.ColumnNames)
		if n.Selection != nil {
			Walk(v, n.Selection)
		}
	case *sqlast.DropIndexStmt:
		walkIdentLists(v, n.IndexNames)
	case *sqlast.ExplainStmt:
		Walk(v, n.Stmt)
	case sqlast.Operator:
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
		// log.Fatalf("not implemented type %T: %+v", node, node)
	}

}

func (a *application) applyList(parent sqlast.Node, name string) {
	saved := a.iter
	a.iter.index = 0
	for {
		v := reflect.Indirect(reflect.ValueOf(parent))
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
