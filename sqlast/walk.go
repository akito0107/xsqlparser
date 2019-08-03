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
		Walk(v, n.Over)
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
		Walk(v, n.WindowsFrame)
	case *SQLWindowFrame:
		Walk(v, n.Units)
		Walk(v, n.StartBound)
		Walk(v, n.EndBound)
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
		Walk(v, n.Limit)
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
		Walk(v, n.Relation)
		for _, j := range n.Joins {
			Walk(v, j)
		}
		Walk(v, n.Selection)
		walkASTNodeLists(v, n.GroupBy)
		Walk(v, n.Having)
	case *Table:
		Walk(v, n.Name)
		Walk(v, n.Alias)
		walkASTNodeLists(v, n.Args)
		walkASTNodeLists(v, n.WithHints)
	case *Derived:
	case *UnnamedExpression:
	case *ExpressionWithAlias:
	case *QualifiedWildcard:
	case *Wildcard:
	case *Join:
	// case *OnJoinConstant:
	// case *UsingConstant:
	// case *NaturalConstant:
	case *SQLOrderByExpr:
	case *LimitExpr:
	case *CharType:
	case *VarcharType:
	case *UUID:
	case *Clob:
	case *Binary:
	case *Varbinary:
	case *Blob:
	case *Decimal:
	case *Float:
	case *SmallInt:
	case *Int:
	case *BigInt:
	case *Real:
	case *Double:
	case *Boolean:
	case *Date:
	case *Time:
	case *Timestamp:
	case *Regclass:
	case *Text:
	case *Bytea:
	case *Array:
	case *Custom:
	case *SQLInsert:
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
