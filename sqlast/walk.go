package sqlast

import (
	"log"
)

type Visitor interface {
	Visit(node ASTNode) Visitor
}

func walkIdentLists(v Visitor, list []*SQLIdent) {
	for _, i := range list {
		Walk(v, i)
	}
}

func walkASTNodeLists(v Visitor, list []ASTNode) {
	for _, l := range list {
		Walk(v, l)
	}
}

func Walk(v Visitor, node ASTNode) {
	if v := v.Visit(node); v == nil {
		return
	}

	switch n := node.(type) {
	case *SQLIdentifier:
		Walk(v, n.Ident)
	case *SQLWildcard:
		// nothing to do
	case *SQLQualifiedWildcard:
		walkIdentLists(v, n.Idents)
	case *SQLCompoundIdentifier:
		walkIdentLists(v, n.Idents)
	case *SQLIsNull:
		Walk(v, n.X)
	case *SQLIsNotNull:
		Walk(v, n.X)
	case *SQLInList:
		Walk(v, n.Expr)
		walkASTNodeLists(v, n.List)
	case *SQLInSubQuery:
		Walk(v, n.Expr)
		Walk(v, n.SubQuery)
	case *SQLBetween:
		Walk(v, n.Expr)
		Walk(v, n.Low)
		Walk(v, n.High)
	case *SQLBinaryExpr:
		Walk(v, n.Left)
		Walk(v, n.Op)
		Walk(v, n.Right)
	case *SQLCast:
		Walk(v, n.Expr)
		Walk(v, n.DateType)
	case *SQLNested:
		Walk(v, n.AST)
	case *SQLUnary:
		Walk(v, n.Operator)
		Walk(v, n.Expr)
	case *SQLValue:
		Walk(v, n.Value)
	case *SQLFunction:
		Walk(v, n.Name)
		walkASTNodeLists(v, n.Args)
		if n.Over != nil {
			Walk(v, n.Over)
		}
	case *SQLCase:
		Walk(v, n.Operand)
	case *SQLExists:
		Walk(v, n.Query)
	case *SQLSubquery:
		Walk(v, n.Query)
	case *SQLObjectName:
		walkIdentLists(v, n.Idents)
	case *SQLWindowSpec:
		walkASTNodeLists(v, n.PartitionBy)
		for _, o := range n.OrderBy {
			Walk(v, o)
		}
		if n.WindowsFrame != nil {
			Walk(v, n.WindowsFrame)
		}
	case *SQLWindowFrame:
		Walk(v, n.Units)
		Walk(v, n.StartBound)
		if n.EndBound != nil {
			Walk(v, n.EndBound)
		}
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
	case *SQLQuery:
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
		if n.Relation != nil {
			Walk(v, n.Relation)
		}
		for _, j := range n.Joins {
			Walk(v, j)
		}
		if n.Selection != nil {
			Walk(v, n.Selection)
		}
		walkASTNodeLists(v, n.GroupBy)
		if n.Having != nil {
			Walk(v, n.Having)
		}
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
	case *UnnamedExpression:
		Walk(v, n.Node)
	case *ExpressionWithAlias:
		Walk(v, n.Expr)
		Walk(v, n.Alias)
	case *QualifiedWildcard:
		Walk(v, n.Prefix)
	case *Wildcard:
		// nothing to do
	case *Join:
		log.Println("JOIN is not implemented yet")
		// TODO
	// case *OnJoinConstant:
	// case *UsingConstant:
	// case *NaturalConstant:
	case *SQLOrderByExpr:
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
	case *SQLInsert:
		Walk(v, n.TableName)
		walkIdentLists(v, n.Columns)
	case *SQLCopy:
	case *SQLUpdate:
	case *SQLDelete:
	case *SQLCreateView:
	case *SQLCreateTable:
	case *SQLAssignment:
	case *TableConstraint:
	case *UniqueTableConstraint:
	case *ReferentialTableConstraint:
	case *ReferenceKeyExpr:
	case *CheckTableConstraint:
	case *SQLColumnDef:
	case *ColumnConstraint:
	case *NotNullColumnSpec:
	case *UniqueColumnSpec:
	case *ReferencesColumnSpec:
	case *CheckColumnSpec:
	case *SQLAlterTable:
	case *AddColumnTableAction:
	case *AlterColumnTableAction:
	case *SetDefaultColumnAction:
	case *DropDefaultColumnAction:
	case *PGAlterDataTypeColumnAction:
	case *PGSetNotNullColumnAction:
	case *PGDropNotNullColumnAction:
	case *RemoveColumnTableAction:
	case *AddConstraintTableAction:
	case *DropConstraintTableAction:
	case *SQLDropTable:
	case *SQLCreateIndex:
	case *SQLDropIndex:
	case *SQLExplain:
	case *NullValue:
	default:
		log.Fatalf("not implemented type %s", node.ToSQLString())
	}

	v.Visit(nil)
}
