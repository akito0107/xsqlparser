package sqlast

import (
	"log"
)

type Visitor interface {
	Visit(node Node) Visitor
}

func walkIdentLists(v Visitor, list []*Ident) {
	for _, i := range list {
		Walk(v, i)
	}
}

func walkASTNodeLists(v Visitor, list []Node) {
	for _, l := range list {
		Walk(v, l)
	}
}

func Walk(v Visitor, node Node) {
	if v := v.Visit(node); v == nil {
		return
	}

	switch n := node.(type) {
	case *File:
		for _, stmt := range n.Stmts {
			Walk(v, stmt)
		}
	case *Ident:
		// nothing to do
	case *Wildcard:
		// nothing to do
	case *QualifiedWildcard:
		walkIdentLists(v, n.Idents)
	case *CompoundIdent:
		walkIdentLists(v, n.Idents)
	case *IsNull:
		Walk(v, n.X)
	case *IsNotNull:
		Walk(v, n.X)
	case *InList:
		Walk(v, n.Expr)
		walkASTNodeLists(v, n.List)
	case *InSubQuery:
		Walk(v, n.Expr)
		Walk(v, n.SubQuery)
	case *Between:
		Walk(v, n.Expr)
		Walk(v, n.Low)
		Walk(v, n.High)
	case *BinaryExpr:
		Walk(v, n.Left)
		Walk(v, n.Op)
		Walk(v, n.Right)
	case *Cast:
		Walk(v, n.Expr)
		Walk(v, n.DateType)
	case *Nested:
		Walk(v, n.AST)
	case *UnaryExpr:
		Walk(v, n.Op)
		Walk(v, n.Expr)
	case *Function:
		Walk(v, n.Name)
		walkASTNodeLists(v, n.Args)
		if n.Over != nil {
			Walk(v, n.Over)
		}
	case *CaseExpr:
		Walk(v, n.Operand)
	case *Exists:
		Walk(v, n.Query)
	case *SubQuery:
		Walk(v, n.Query)
	case *ObjectName:
		walkIdentLists(v, n.Idents)
	case *WindowSpec:
		walkASTNodeLists(v, n.PartitionBy)
		for _, o := range n.OrderBy {
			Walk(v, o)
		}
		if n.WindowsFrame != nil {
			Walk(v, n.WindowsFrame)
		}
	case *WindowFrame:
		Walk(v, n.Units)
		Walk(v, n.StartBound)
		if n.EndBound != nil {
			Walk(v, n.EndBound)
		}
	case *WindowFrameUnit:
		// nothing to do
	case *CurrentRow:
		// nothing to do
	case *UnboundedPreceding:
		// nothing to do
	case *UnboundedFollowing:
		// nothing to do
	case *Preceding:
		// nothing to do
	case *Following:
		// nothing to do
	case *QueryStmt:
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
	case *CTE:
		Walk(v, n.Query)
		Walk(v, n.Alias)
	case *SelectExpr:
		Walk(v, n.Select)
	case *QueryExpr:
		Walk(v, n.Query)
	case *SetOperationExpr:
		Walk(v, n.Op)
		Walk(v, n.Left)
		Walk(v, n.Right)
	case *UnionOperator:
		// nothing to do
	case *ExceptOperator:
		// nothing to do
	case *IntersectOperator:
		// nothing to do
	case *SQLSelect:
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
	case *QualifiedJoin:
		Walk(v, n.LeftElement)
		Walk(v, n.Type)
		Walk(v, n.RightElement)
		Walk(v, n.Spec)
	case *TableJoinElement:
		Walk(v, n.Ref)
	case *JoinType:
	// nothing to do
	case *JoinCondition:
		Walk(v, n.SearchCondition)
	case *NaturalJoin:
		Walk(v, n.LeftElement)
		Walk(v, n.Type)
		Walk(v, n.RightElement)
	case *CrossJoin:
		Walk(v, n.Factor)
		Walk(v, n.Reference)
	case *Table:
		Walk(v, n.Name)
		if n.Alias != nil {
			Walk(v, n.Alias)
		}
		walkASTNodeLists(v, n.Args)
		walkASTNodeLists(v, n.WithHints)
	case *Derived:
		Walk(v, n.SubQuery)
		if n.Alias != nil {
			Walk(v, n.Alias)
		}
	case *UnnamedSelectItem:
		Walk(v, n.Node)
	case *AliasSelectItem:
		Walk(v, n.Expr)
		Walk(v, n.Alias)
	case *QualifiedWildcardSelectItem:
		Walk(v, n.Prefix)
	case *WildcardSelectItem:
		// nothing to do
	case *OrderByExpr:
		Walk(v, n.Expr)
	case *LimitExpr:
		if !n.All {
			Walk(v, n.LimitValue)
		}
		if n.OffsetValue != nil {
			Walk(v, n.OffsetValue)
		}
	case *CharType:
		// nothing to do
	case *VarcharType:
		// nothing to do
	case *UUID:
		// nothing to do
	case *Clob:
		// nothing to do
	case *Binary:
		// nothing to do
	case *Varbinary:
		// nothing to do
	case *Blob:
		// nothing to do
	case *Decimal:
		// nothing to do
	case *Float:
		// nothing to do
	case *SmallInt:
		// nothing to do
	case *Int:
		// nothing to do
	case *BigInt:
		// nothing to do
	case *Real:
		// nothing to do
	case *Double:
		// nothing to do
	case *Boolean:
		// nothing to do
	case *Date:
		// nothing to do
	case *Time:
		// nothing to do
	case *Timestamp:
		// nothing to do
	case *Regclass:
		// nothing to do
	case *Text:
		// nothing to do
	case *Bytea:
		// nothing to do
	case *Array:
		// nothing to do
	case *Custom:
		// nothing to do
	case *InsertStmt:
		Walk(v, n.TableName)
		walkIdentLists(v, n.Columns)
		Walk(v, n.Source)

		for _, a := range n.UpdateAssignments {
			Walk(v, a)
		}

	case *ConstructorSource:
		for _, r := range n.Rows {
			Walk(v, r)
		}
	case *RowValueExpr:
		for _, r := range n.Values {
			Walk(v, r)
		}
	case *SubQuerySource:
		Walk(v, n.SubQuery)
	case *CopyStmt:
		Walk(v, n.TableName)
		walkIdentLists(v, n.Columns)
	case *UpdateStmt:
		Walk(v, n.TableName)
		for _, a := range n.Assignments {
			Walk(v, a)
		}
		Walk(v, n.Selection)
	case *DeleteStmt:
		Walk(v, n.TableName)
		if n.Selection != nil {
			Walk(v, n.Selection)
		}
	case *CreateViewStmt:
		Walk(v, n.Name)
		Walk(v, n.Query)
	case *CreateTableStmt:
		Walk(v, n.Name)
		for _, e := range n.Elements {
			Walk(v, e)
		}
	case *Assignment:
		Walk(v, n.ID)
		Walk(v, n.Value)
	case *TableConstraint:
		if n.Name != nil {
			Walk(v, n.Name)
		}
		Walk(v, n.Spec)
	case *UniqueTableConstraint:
		walkIdentLists(v, n.Columns)
	case *ReferentialTableConstraint:
		walkIdentLists(v, n.Columns)
		Walk(v, n.KeyExpr)
	case *ReferenceKeyExpr:
		Walk(v, n.TableName)
		walkIdentLists(v, n.Columns)
	case *CheckTableConstraint:
		Walk(v, n.Expr)
	case *ColumnDef:
		Walk(v, n.Name)
		Walk(v, n.DataType)
		if n.Default != nil {
			Walk(v, n.Default)
		}
		for _, c := range n.Constraints {
			Walk(v, c)
		}
	case *ColumnConstraint:
		if n.Name != nil {
			Walk(v, n.Name)
		}
		Walk(v, n.Spec)
	case *NotNullColumnSpec:
		// nothing to do
	case *UniqueColumnSpec:
		// nothing to do
	case *ReferencesColumnSpec:
		Walk(v, n.TableName)
		walkIdentLists(v, n.Columns)
	case *CheckColumnSpec:
		Walk(v, n.Expr)
	case *AlterTableStmt:
		Walk(v, n.TableName)
		Walk(v, n.Action)
	case *AddColumnTableAction:
		Walk(v, n.Column)
	case *AlterColumnTableAction:
		Walk(v, n.ColumnName)
		Walk(v, n.Action)
	case *SetDefaultColumnAction:
		Walk(v, n.Default)
	case *DropDefaultColumnAction:
		// nothing to do
	case *PGAlterDataTypeColumnAction:
		Walk(v, n.DataType)
	case *PGSetNotNullColumnAction:
		// nothing to do
	case *PGDropNotNullColumnAction:
		// nothing to do
	case *RemoveColumnTableAction:
		Walk(v, n.Name)
	case *AddConstraintTableAction:
		Walk(v, n.Constraint)
	case *DropConstraintTableAction:
		Walk(v, n.Name)
	case *DropTableStmt:
		for _, t := range n.TableNames {
			Walk(v, t)
		}
	case *CreateIndexStmt:
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
	case *DropIndexStmt:
		walkIdentLists(v, n.IndexNames)
	case *ExplainStmt:
		Walk(v, n.Stmt)
	case *Operator:
		// nothing to do
	case *NullValue,
		*LongValue,
		*DoubleValue,
		*SingleQuotedString,
		*NationalStringLiteral,
		*BooleanValue,
		*DateValue,
		*TimeValue,
		*DateTimeValue,
		*TimestampValue:
		// nothing to do
	default:
		log.Panicf("not implemented type %T: %+v", node, node)
	}

	v.Visit(nil)
}

type inspector func(node Node) bool

func (f inspector) Visit(node Node) Visitor {
	if f(node) {
		return f
	}
	return nil
}

func Inspect(node Node, f func(node Node) bool) {
	Walk(inspector(f), node)
}
