package xsqlparser

import (
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"

	"github.com/akito0107/xsqlparser/dialect"
	"github.com/akito0107/xsqlparser/sqlast"
	errors "golang.org/x/xerrors"
)

type Parser struct {
	tokens []*TokenSet
	index  uint
}

func NewParser(src io.Reader, dialect dialect.Dialect) (*Parser, error) {
	tokenizer := NewTokenizer(src, dialect)
	set, err := tokenizer.Tokenize()
	if err != nil {
		return nil, errors.Errorf("tokenize err failed: %w", err)
	}

	return &Parser{tokens: set, index: 0}, nil
}

func (p *Parser) ParseSQL() ([]sqlast.SQLStmt, error) {
	var stmts []sqlast.SQLStmt
	var expectingDelimiter bool

	for {
		for {
			ok, _ := p.consumeToken(Semicolon)
			expectingDelimiter = false
			if !ok {
				break
			}
		}

		t, _ := p.peekToken()
		if t == nil {
			break
		}
		if expectingDelimiter {
			return nil, errors.Errorf("unexpected token %+v", t)
		}

		stmt, err := p.ParseStatement()
		if err != nil {
			return nil, errors.Errorf("parseStatement failed: %w", err)
		}
		stmts = append(stmts, stmt)
		expectingDelimiter = true

	}

	return stmts, nil
}

func (p *Parser) ParseStatement() (sqlast.SQLStmt, error) {
	tok, err := p.nextToken()
	if err != nil {
		return nil, err
	}
	word, ok := tok.Value.(*SQLWord)
	if !ok {
		return nil, errors.Errorf("a keyword at the beginning of statement %s", tok.Value)
	}

	switch word.Keyword {
	case "SELECT", "WITH":
		p.prevToken()
		return p.parseQuery()
	case "CREATE":
		return p.parseCreate()
	case "DELETE":
		return p.parseDelete()
	case "INSERT":
		return p.parseInsert()
	case "ALTER":
		return p.parseAlter()
	case "UPDATE":
		return p.parseUpdate()
	default:
		return nil, errors.Errorf("unexpected (or unsupported) keyword %s", word.Keyword)
	}
}

func (p *Parser) ParseDataType() (sqlast.SQLType, error) {
	tok, err := p.nextToken()
	if err != nil {
		return nil, errors.Errorf("nextToken failed: %w", err)
	}
	word, ok := tok.Value.(*SQLWord)
	if !ok {
		return nil, errors.Errorf("must be datetype name but %v", tok)
	}

	switch word.Keyword {
	case "BOOLEAN":
		return &sqlast.Boolean{}, nil
	case "FLOAT":
		size, err := p.parseOptionalPrecision()
		if err != nil {
			return nil, errors.Errorf("parsePrecision failed: %w", err)
		}
		return &sqlast.Float{Size: size}, nil
	case "REAL":
		return &sqlast.Real{}, nil
	case "DOUBLE":
		p.expectKeyword("PRECISION")
		return &sqlast.Double{}, nil
	case "SMALLINT":
		return &sqlast.SmallInt{}, nil
	case "INTEGER", "INT":
		return &sqlast.Int{}, nil
	case "BIGINT":
		return &sqlast.BigInt{}, nil
	case "VARCHAR":
		p, err := p.parseOptionalPrecision()
		if err != nil {
			return nil, errors.Errorf("parsePrecision failed: %w", err)
		}
		return &sqlast.VarcharType{Size: p}, nil
	case "CHAR", "CHARACTER":
		if ok, _ := p.parseKeyword("VARYING"); ok {
			p, err := p.parseOptionalPrecision()
			if err != nil {
				return nil, errors.Errorf("parsePrecision failed: %w", err)
			}
			return &sqlast.VarcharType{Size: p}, nil
		}
		p, err := p.parseOptionalPrecision()
		if err != nil {
			return nil, errors.Errorf("parsePrecision failed: %w", err)
		}
		return &sqlast.CharType{Size: p}, nil
	case "UUID":
		return &sqlast.UUID{}, nil
	case "DATE":
		return &sqlast.Date{}, nil
	case "TIMESTAMP":
		wok, _ := p.parseKeyword("WITH")
		ook, _ := p.parseKeyword("WITHOUT")
		if wok || ook {
			p.expectKeyword("TIME")
			p.expectKeyword("ZONE")
		}
		return &sqlast.Timestamp{
			WithTimeZone: wok,
		}, nil
	case "TIME":
		wok, _ := p.parseKeyword("WITH")
		ook, _ := p.parseKeyword("WITHOUT")
		if wok || ook {
			p.expectKeyword("TIME")
			p.expectKeyword("ZONE")
		}
		return &sqlast.Time{}, nil
	case "REGCLASS":
		return &sqlast.Regclass{}, nil
	case "TEXT":
		if ok, _ := p.consumeToken(LBracket); ok {
			p.expectToken(RBracket)
			return &sqlast.Array{
				Ty: &sqlast.Text{},
			}, nil
		}
		return &sqlast.Text{}, nil
	case "BYTEA":
		return &sqlast.Bytea{}, nil
	case "NUMERIC":
		precision, scale, err := p.parseOptionalPrecisionScale()
		if err != nil {
			return nil, errors.Errorf("parseOptionalPrecisionScale failed: %w", err)
		}
		return &sqlast.Decimal{
			Precision: precision,
			Scale:     scale,
		}, nil

	default:
		p.prevToken()
		typeName, err := p.parseObjectName()
		if err != nil {
			return nil, errors.Errorf("parseObjectName failed: %w", err)
		}
		return &sqlast.Custom{
			Ty: typeName,
		}, nil
	}
}

func (p *Parser) ParseExpr() (sqlast.ASTNode, error) {
	return p.parseSubexpr(0)
}

func (p *Parser) parseQuery() (*sqlast.SQLQuery, error) {
	hasCTE, _ := p.parseKeyword("WITH")
	var ctes []*sqlast.CTE
	if hasCTE {
		cts, err := p.parseCTEList()
		if err != nil {
			return nil, errors.Errorf("parseCTEList failed: %w", err)
		}
		ctes = cts
	}

	body, err := p.parseQueryBody(0)
	if err != nil {
		return nil, errors.Errorf("parseQueryBody failed: %w", err)
	}

	var orderBy []*sqlast.SQLOrderByExpr
	if ok, _ := p.parseKeywords("ORDER", "BY"); ok {
		o, err := p.parseOrderByExprList()
		if err != nil {
			return nil, errors.Errorf("parseOrderByExprList failed: %w", err)
		}
		orderBy = o
	}

	var limit sqlast.ASTNode
	if ok, _ := p.parseKeyword("LIMIT"); ok {
		l, err := p.parseLimit()
		if err != nil {
			return nil, errors.Errorf("parseLimit failed: %w", err)
		}
		limit = l
	}

	return &sqlast.SQLQuery{
		CTEs:    ctes,
		Body:    body,
		Limit:   limit,
		OrderBy: orderBy,
	}, nil
}

func (p *Parser) parseQueryBody(precedence uint8) (sqlast.SQLSetExpr, error) {
	var expr sqlast.SQLSetExpr
	if ok, _ := p.parseKeyword("SELECT"); ok {
		s, err := p.parseSelect()
		if err != nil {
			return nil, errors.Errorf("parseSelect failed: %w", err)
		}
		expr = s
	} else if ok, _ := p.consumeToken(LParen); ok {
		subquery, err := p.parseQuery()
		if err != nil {
			return nil, errors.Errorf("parseQuery failed: %w", err)
		}
		p.expectToken(RParen)
		expr = &sqlast.QueryExpr{
			Query: subquery,
		}
	} else {
		log.Fatal("expect SELECT or subquery in the query body")
	}
BODY_LOOP:
	for {
		nextToken, _ := p.peekToken()
		op := p.parseSetOperator(nextToken)
		var nextPrecedence uint8
		switch op.(type) {
		case *sqlast.UnionOperator, *sqlast.ExceptOperator:
			nextPrecedence = 10
		case *sqlast.IntersectOperator:
			nextPrecedence = 20
		default:
			break BODY_LOOP
		}
		if precedence >= nextPrecedence {
			break
		}
		p.mustNextToken()
		all, _ := p.parseKeyword("ALL")
		right, err := p.parseQueryBody(nextPrecedence)
		if err != nil {
			return nil, errors.Errorf("parseQueryBody failed: %w", err)
		}

		expr = &sqlast.SetOperationExpr{
			Left:  expr,
			Right: right,
			Op:    op,
			All:   all,
		}
	}

	return expr, nil
}

func (p *Parser) parseSetOperator(token *TokenSet) sqlast.SQLSetOperator {
	if token == nil {
		return nil
	}
	if token.Tok != SQLKeyword {
		return nil
	}
	word := token.Value.(*SQLWord)
	switch word.Keyword {
	case "UNION":
		return &sqlast.UnionOperator{}
	case "EXCEPT":
		return &sqlast.ExceptOperator{}
	case "INTERSECT":
		return &sqlast.IntersectOperator{}
	}

	return nil

}

func (p *Parser) parseSelect() (*sqlast.SQLSelect, error) {
	distinct, err := p.parseKeyword("DISTINCT")
	if err != nil {
		return nil, errors.Errorf("parseKeyword failed: %w", err)
	}
	projection, err := p.parseSelectList()
	if err != nil {
		return nil, errors.Errorf("parseSelectList failed: %w", err)
	}
	var relation sqlast.TableFactor
	var joins []*sqlast.Join

	if ok, _ := p.parseKeyword("FROM"); ok {
		t, err := p.parseTableFactor()
		if err != nil {
			return nil, errors.Errorf("parseTableFactor failed: %w", err)
		}
		relation = t
		j, err := p.parseJoins()
		if err != nil {
			return nil, errors.Errorf("parseJoins failed: %w", err)
		}
		joins = j
	}

	var selection sqlast.ASTNode
	if ok, _ := p.parseKeyword("WHERE"); ok {
		s, err := p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}
		selection = s
	}

	var groupBy []sqlast.ASTNode
	if ok, _ := p.parseKeywords("GROUP", "BY"); ok {
		g, err := p.parseExprList()
		if err != nil {
			return nil, errors.Errorf("parseExprList failed: %w", err)
		}
		groupBy = g
	}

	var having sqlast.ASTNode
	if ok, _ := p.parseKeyword("HAVING"); ok {
		h, err := p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}
		having = h
	}

	return &sqlast.SQLSelect{
		Distinct:   distinct,
		Projection: projection,
		Selection:  selection,
		Relation:   relation,
		Joins:      joins,
		GroupBy:    groupBy,
		Having:     having,
	}, nil

}

func (p *Parser) parseSelectList() ([]sqlast.SQLSelectItem, error) {
	var projections []sqlast.SQLSelectItem

	for {
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}
		if w, ok := expr.(*sqlast.Wildcard); ok {
			projections = append(projections, w)
		} else if q, ok := expr.(*sqlast.SQLQualifiedWildcard); ok {
			projections = append(projections, &sqlast.QualifiedWildcard{
				Prefix: &sqlast.SQLObjectName{
					Idents: q.Idents,
				},
			})
		} else {
			alias := p.parseOptionalAlias(dialect.ReservedForColumnAlias)

			if alias != nil {
				projections = append(projections, &sqlast.ExpressionWithAlias{
					Expr:  expr,
					Alias: alias,
				})
			} else {
				projections = append(projections, &sqlast.UnnamedExpression{
					Node: expr,
				})
			}
		}

		if t, _ := p.peekToken(); t.Tok == Comma {
			p.mustNextToken()
		} else {
			break
		}
	}
	return projections, nil
}

func (p *Parser) parseCreate() (sqlast.SQLStmt, error) {

	if ok, _ := p.parseKeyword("TABLE"); ok {
		return p.parseCreateTable()
	}

	mok, _ := p.parseKeyword("MATERIALIZED")
	vok, _ := p.parseKeyword("VIEW")

	if mok || vok {
		p.prevToken()
		return p.parseCreateView()
	}

	log.Fatal("TABLE or VIEW after create")

	return nil, nil
}

func (p *Parser) parseCreateTable() (sqlast.SQLStmt, error) {
	name, err := p.parseObjectName()
	if err != nil {
		return nil, errors.Errorf("parseObjectName failed: %w", err)
	}

	elements, err := p.parseElements()
	if err != nil {
		return nil, errors.Errorf("parseElements failed: %w", err)
	}

	return &sqlast.SQLCreateTable{
		Name:     name,
		External: false,
		Elements: elements,
	}, nil
}

func (p *Parser) parseCreateView() (sqlast.SQLStmt, error) {
	materialized, _ := p.parseKeyword("MATERIALIZED")
	p.expectKeyword("VIEW")
	name, err := p.parseObjectName()
	if err != nil {
		return nil, errors.Errorf("parseObjectName failed: %w", err)
	}
	p.expectKeyword("AS")
	q, err := p.parseQuery()
	if err != nil {
		return nil, errors.Errorf("parseQuery failed: %w", err)
	}

	return &sqlast.SQLCreateView{
		Materialized: materialized,
		Name:         name,
		Query:        q,
	}, nil

}

func (p *Parser) parseElements() ([]sqlast.TableElement, error) {
	var elements []sqlast.TableElement
	if ok, _ := p.consumeToken(LParen); !ok {
		return elements, nil
	}

	for {
		tok, _ := p.nextToken()
		if tok == nil || tok.Tok != SQLKeyword {
			return nil, errors.Errorf("parse error after column def")
		}

		word := tok.Value.(*SQLWord)
		switch word.Keyword {
		case "CONSTRAINT", "PRIMARY", "CHECK", "FOREIGN", "UNIQUE":
			p.prevToken()
			constraints, err := p.parseTableConstraints()
			if err != nil {
				return nil, errors.Errorf("parseTableConstraints failed: %w", err)
			}
			elements = append(elements, constraints)

		default:
			p.prevToken()
			def, err := p.parseColumnDef()
			if err != nil {
				return nil, errors.Errorf("parseColumnDef failed: %w", err)
			}

			elements = append(elements, def)
		}

		t, _ := p.nextToken()
		if t == nil || (t.Tok != Comma && t.Tok != RParen) {
			log.Fatalf("Expected ',' or ')' after column definition but %v", t)
		} else if t.Tok == RParen {
			break
		}
	}

	return elements, nil
}

func (p *Parser) parseColumnDef() (*sqlast.SQLColumnDef, error) {
	tok := p.mustNextToken()
	columnName := tok.Value.(*SQLWord)

	dataType, err := p.ParseDataType()
	if err != nil {
		return nil, errors.Errorf("ParseDataType failed: %w", err)
	}

	def, specs, err := p.parseColumnDefinition()
	if err != nil {
		return nil, errors.Errorf("parseColumnDefinition: %w", err)
	}

	return &sqlast.SQLColumnDef{
		Constraints: specs,
		Name:        columnName.AsSQLIdent(),
		DataType:    dataType,
		Default:     def,
	}, nil
}

func (p *Parser) parseTableConstraints() (*sqlast.TableConstraint, error) {
	tok, _ := p.peekToken()
	if tok == nil || tok.Tok != SQLKeyword {
		return nil, errors.Errorf("parse error after column def")
	}

	word, ok := tok.Value.(*SQLWord)

	var name *sqlast.SQLIdentifier
	if ok && word.Keyword == "CONSTRAINT" {
		p.mustNextToken()
		i, err := p.parseIdentifier()
		if err != nil {
			return nil, errors.Errorf("parseIdentifier failed: %w", err)
		}
		name = &sqlast.SQLIdentifier{Ident: i}
	}

	tok, _ = p.peekToken()

	var spec sqlast.TableConstraintSpec
	word = tok.Value.(*SQLWord)
	switch word.Keyword {
	case "UNIQUE":
		p.mustNextToken()
		if _, err := p.parseKeyword("KEY"); err != nil {
			return nil, errors.Errorf("parseKeyword failed: %w", err)
		}
		p.expectToken(LParen)
		columns, err := p.parseColumnNames()
		if err != nil {
			return nil, errors.Errorf("parseColumnNames failed: %w", err)
		}
		p.expectToken(RParen)
		spec = &sqlast.UniqueTableConstraint{
			Columns: columns,
		}
	case "PRIMARY":
		p.mustNextToken()
		p.expectKeyword("KEY")
		p.expectToken(LParen)
		columns, err := p.parseColumnNames()
		if err != nil {
			return nil, errors.Errorf("parseColumnNames failed: %w", err)
		}
		p.expectToken(RParen)
		spec = &sqlast.UniqueTableConstraint{
			IsPrimary: true,
			Columns:   columns,
		}
	case "FOREIGN":
		p.mustNextToken()
		p.expectKeyword("KEY")
		p.expectToken(LParen)
		columns, err := p.parseColumnNames()
		if err != nil {
			return nil, errors.Errorf("parseColumnNames failed: %w", err)
		}
		p.expectToken(RParen)
		p.expectKeyword("REFERENCES")

		t, _ := p.nextToken()
		w := t.Value.(*SQLWord)
		p.expectToken(LParen)
		refcolumns, err := p.parseColumnNames()
		p.expectToken(RParen)

		keys := &sqlast.ReferenceKeyExpr{
			TableName: sqlast.NewSQLIdentifier(w.AsSQLIdent()),
			Columns:   refcolumns,
		}

		spec = &sqlast.ReferentialTableConstraint{
			Columns: columns,
			KeyExpr: keys,
		}
	case "CHECK":
		p.mustNextToken()
		p.expectToken(LParen)
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}

		spec = &sqlast.CheckTableConstraint{
			Expr: expr,
		}
		p.expectToken(RParen)
	default:
		return nil, errors.Errorf("unknown table constraint: %v", word)
	}

	return &sqlast.TableConstraint{
		Name: name,
		Spec: spec,
	}, nil
}

func (p *Parser) parseColumnDefinition() (sqlast.ASTNode, []*sqlast.ColumnConstraint, error) {
	var specs []*sqlast.ColumnConstraint
	var def sqlast.ASTNode

COLUMN_DEF_LOOP:
	for {
		t, _ := p.peekToken()
		if t == nil || t.Tok != SQLKeyword {
			break
		}

		word := t.Value.(*SQLWord)

		switch word.Keyword {
		case "DEFAULT":
			if ok, _ := p.parseKeyword("DEFAULT"); ok {
				d, err := p.parseDefaultExpr(0)
				if err != nil {
					return nil, nil, errors.Errorf("parseDefaultExpr failed: %w", err)
				}
				def = d
				continue
			}
		case "CONSTRAINT", "NOT", "UNIQUE", "PRIMARY", "REFERENCES", "CHECK":
			s, err := p.parseColumnConstraints()
			if err != nil {
				return nil, nil, errors.Errorf("parseColumnConstrains failed: %w", err)
			}
			specs = s
		default:
			break COLUMN_DEF_LOOP
		}
	}
	return def, specs, nil
}

func (p *Parser) parseColumnConstraints() ([]*sqlast.ColumnConstraint, error) {
	var constraints []*sqlast.ColumnConstraint

CONSTRAINT_LOOP:
	for {
		tok, err := p.peekToken()
		if tok == nil {
			break CONSTRAINT_LOOP
		}
		if err != nil {
			return nil, errors.Errorf("peekToken failed: %w", err)
		}
		word, ok := tok.Value.(*SQLWord)

		var name *sqlast.SQLIdentifier
		if ok && word.Keyword == "CONSTRAINT" {
			p.mustNextToken()
			i, err := p.parseIdentifier()
			if err != nil {
				return nil, errors.Errorf("parseIdentifier failed: %w", err)
			}
			name = &sqlast.SQLIdentifier{Ident: i}
		}

		tok, _ = p.peekToken()
		if tok.Tok != SQLKeyword {
			break
		}

		var spec sqlast.ColumnConstraintSpec

		word = tok.Value.(*SQLWord)
		switch word.Keyword {
		case "NOT":
			p.mustNextToken()
			p.expectKeyword("NULL")
			spec = &sqlast.NotNullColumnSpec{}
		case "UNIQUE":
			p.mustNextToken()
			spec = &sqlast.UniqueColumnSpec{}
		case "PRIMARY":
			p.mustNextToken()
			p.expectKeyword("KEY")
			spec = &sqlast.UniqueColumnSpec{IsPrimaryKey: true}
		case "REFERENCES":
			p.mustNextToken()
			tname, err := p.parseObjectName()
			if err != nil {
				return nil, errors.Errorf("parseObjectName failed: %w", err)
			}
			p.expectToken(LParen)
			columns, err := p.parseColumnNames()
			if err != nil {
				return nil, errors.Errorf("parseColumnNames failed: %w", err)
			}
			p.expectToken(RParen)
			spec = &sqlast.ReferencesColumnSpec{
				TableName: tname,
				Columns:   columns,
			}
		case "CHECK":
			p.mustNextToken()
			p.expectToken(LParen)
			expr, err := p.ParseExpr()
			if err != nil {
				return nil, errors.Errorf("ParseExpr failed: %w", err)
			}

			spec = &sqlast.CheckColumnSpec{
				Expr: expr,
			}
			p.expectToken(RParen)
		default:
			break CONSTRAINT_LOOP
		}

		constraints = append(constraints, &sqlast.ColumnConstraint{
			Name: name,
			Spec: spec,
		})

	}
	return constraints, nil
}

func (p *Parser) parseDelete() (sqlast.SQLStmt, error) {
	p.expectKeyword("FROM")
	tableName, err := p.parseObjectName()
	if err != nil {
		return nil, errors.Errorf("parseObjectName failed: %w", err)
	}

	var selection sqlast.ASTNode
	if ok, _ := p.parseKeyword("WHERE"); ok {
		selection, err = p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}
	}

	return &sqlast.SQLDelete{
		TableName: tableName,
		Selection: selection,
	}, nil
}

func (p *Parser) parseUpdate() (sqlast.SQLStmt, error) {
	tableName, err := p.parseObjectName()
	if err != nil {
		return nil, errors.Errorf("parseObjectName failed: %w", err)
	}
	p.expectKeyword("SET")

	assignments, err := p.parseAssignments()
	if err != nil {
		return nil, errors.Errorf("parseAssignments failed: %w", err)
	}

	var selection sqlast.ASTNode
	if ok, _ := p.parseKeyword("WHERE"); ok {
		selection, err = p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}
	}

	return &sqlast.SQLUpdate{
		TableName:   tableName,
		Assignments: assignments,
		Selection:   selection,
	}, nil

}

func (p *Parser) parseAssignments() ([]*sqlast.SQLAssignment, error) {
	var assignments []*sqlast.SQLAssignment

	for {
		tok, _ := p.nextToken()
		if tok.Tok != SQLKeyword {
			return nil, errors.Errorf("should be sqlkeyword but %v", tok)
		}

		word := tok.Value.(*SQLWord)

		p.expectToken(Eq)

		val, err := p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}

		assignments = append(assignments, &sqlast.SQLAssignment{
			ID:    word.AsSQLIdent(),
			Value: val,
		})

		if ok, _ := p.consumeToken(Comma); !ok {
			break
		}
	}

	return assignments, nil
}

func (p *Parser) parseInsert() (sqlast.SQLStmt, error) {
	p.expectKeyword("INTO")
	tableName, err := p.parseObjectName()

	if err != nil {
		return nil, errors.Errorf("parseObjectName failed: %w", err)
	}
	var columns []*sqlast.SQLIdent

	if ok, _ := p.consumeToken(LParen); ok {
		columns, err = p.parseColumnNames()
		if err != nil {
			return nil, errors.Errorf("parseColumnNames failed: %w", err)
		}
		p.expectToken(RParen)
	}

	p.expectKeyword("VALUES")
	var values [][]sqlast.ASTNode

	for {
		p.expectToken(LParen)
		v, err := p.parseExprList()
		if err != nil {
			return nil, errors.Errorf("parseExprList failed: %w", err)
		}
		values = append(values, v)
		p.expectToken(RParen)
		if ok, _ := p.consumeToken(Comma); !ok {
			break
		}
	}

	return &sqlast.SQLInsert{
		TableName: tableName,
		Columns:   columns,
		Values:    values,
	}, nil
}

func (p *Parser) parseAlter() (sqlast.SQLStmt, error) {
	p.expectKeyword("TABLE")

	tableName, err := p.parseObjectName()
	if err != nil {
		return nil, errors.Errorf("parseObjectName failed: %w", err)
	}

	if ok, _ := p.parseKeywords("ADD", "COLUMN"); ok {
		columnDef, err := p.parseColumnDef()
		if err != nil {
			return nil, errors.Errorf("parseColumnDef failed: %w", err)
		}

		return &sqlast.SQLAlterTable{
			TableName: tableName,
			Action: &sqlast.AddColumnTableAction{
				Column: columnDef,
			},
		}, nil
	}

	if ok, _ := p.parseKeyword("ADD"); ok {
		constraint, err := p.parseTableConstraints()
		if err != nil {
			return nil, errors.Errorf("parseTableConstraints failed: %w", err)
		}

		return &sqlast.SQLAlterTable{
			TableName: tableName,
			Action: &sqlast.AddConstraintTableAction{
				Constraint: constraint,
			},
		}, nil
	}

	if ok, _ := p.parseKeywords("DROP", "CONSTRAINT"); ok {
		constraintName, err := p.parseIdentifier()
		if err != nil {
			return nil, errors.Errorf("parseIdentifier failed: %w", err)
		}
		cascade, _ := p.parseKeyword("CASCADE")

		return &sqlast.SQLAlterTable{
			TableName: tableName,
			Action: &sqlast.DropConstraintTableAction{
				Name:    constraintName,
				Cascade: cascade,
			},
		}, nil
	}

	if ok, _ := p.parseKeywords("DROP", "COLUMN"); ok {
		constraintName, err := p.parseIdentifier()
		if err != nil {
			return nil, errors.Errorf("parseIdentifier failed: %w", err)
		}
		cascade, _ := p.parseKeyword("CASCADE")

		return &sqlast.SQLAlterTable{
			TableName: tableName,
			Action: &sqlast.RemoveColumnTableAction{
				Name:    constraintName,
				Cascade: cascade,
			},
		}, nil
	}

	if ok, _ := p.parseKeywords("ALTER", "COLUMN"); ok {
		action, err := p.parseAlterColumn()
		if err != nil {
			return nil, errors.Errorf("parseAlterColumn failed: %w", err)
		}

		return &sqlast.SQLAlterTable{
			TableName: tableName,
			Action:    action,
		}, nil

	}

	t, _ := p.peekToken()
	return nil, errors.Errorf("unknown alter operation %v", t)
}

func (p *Parser) parseAlterColumn() (*sqlast.AlterColumnTableAction, error) {
	columnName, err := p.parseIdentifier()
	if err != nil {
		return nil, errors.Errorf("parseIdentifier failed: %w", err)
	}

	tok := p.mustNextToken()
	if tok.Tok != SQLKeyword {
		return nil, errors.Errorf("must be SQLKeyword but: %v", tok)
	}

	word := tok.Value.(*SQLWord)

	switch word.Keyword {
	case "SET":
		if ok, _ := p.parseKeyword("DEFAULT"); ok {
			def, err := p.parseDefaultExpr(0)
			if err != nil {
				return nil, errors.Errorf("parseDefaultExpr failed: %w", err)
			}
			return &sqlast.AlterColumnTableAction{
				ColumnName: columnName,
				Action: &sqlast.SetDefaultColumnAction{
					Default: def,
				},
			}, nil
		}
		if ok, _ := p.parseKeywords("NOT", "NULL"); ok {
			return &sqlast.AlterColumnTableAction{
				ColumnName: columnName,
				Action:     &sqlast.PGSetNotNullColumnAction{},
			}, nil
		}

		return nil, errors.Errorf("unknown SET action")
	case "DROP":
		if ok, _ := p.parseKeyword("DEFAULT"); ok {
			return &sqlast.AlterColumnTableAction{
				ColumnName: columnName,
				Action:     &sqlast.DropDefaultColumnAction{},
			}, nil
		}
		if ok, _ := p.parseKeywords("NOT", "NULL"); ok {
			return &sqlast.AlterColumnTableAction{
				ColumnName: columnName,
				Action:     &sqlast.PGDropNotNullColumnAction{},
			}, nil
		}
		return nil, errors.Errorf("unknown DROP action")
	case "TYPE":
		tp, err := p.ParseDataType()
		if err != nil {
			return nil, errors.Errorf("ParseDataType failed: %w", err)
		}

		return &sqlast.AlterColumnTableAction{
			ColumnName: columnName,
			Action: &sqlast.PGAlterDataTypeColumnAction{
				DataType: tp,
			},
		}, nil
	default:
		return nil, errors.Errorf("unknown alter column action %v", word)
	}
}

func (p *Parser) parseDefaultExpr(precedence uint) (sqlast.ASTNode, error) {
	expr, err := p.parsePrefix()
	if err != nil {
		return nil, errors.Errorf("parsePrefix failed: %w", err)
	}
	for {
		tok, _ := p.peekToken()
		if tok != nil && tok.Tok == SQLKeyword {
			w := tok.Value.(*SQLWord)
			if w.Keyword == "NOT" || w.Keyword == "NULL" {
				break
			}
		}

		nextPrecedence, err := p.getNextPrecedence()
		if err != nil {
			return nil, errors.Errorf("getNextPrecedence failed: %w", err)
		}
		if precedence >= nextPrecedence {
			break
		}
		expr, err = p.parseInfix(expr, nextPrecedence)
		if err != nil {
			return nil, errors.Errorf("parseInfix failed: %w")
		}
	}
	return expr, nil
}

// TODO add tests
func (p *Parser) parseOptionalAlias(reservedKeywords map[string]struct{}) *sqlast.SQLIdent {
	afterAs, _ := p.parseKeyword("AS")
	maybeAlias, _ := p.nextToken()

	if maybeAlias == nil {
		return nil
	}

	if maybeAlias.Tok == SQLKeyword {

		word := maybeAlias.Value.(*SQLWord)
		if afterAs || !containsStr(reservedKeywords, word.Keyword) {
			return word.AsSQLIdent()
		}
	}
	if afterAs {
		log.Fatalf("expected an identifier after AS")
	}
	p.prevToken()
	return nil
}

func (p *Parser) parseJoins() ([]*sqlast.Join, error) {
	var joins []*sqlast.Join
	var natural bool

JOIN_LOOP:
	for {
		tok, _ := p.peekToken()

		if tok == nil {
			return joins, nil
		}

		switch tok.Tok {
		case Comma:
			p.mustNextToken()
			relation, err := p.parseTableFactor()
			if err != nil {
				return nil, errors.Errorf("parseTableFactor failed: %w", err)
			}
			join := &sqlast.Join{
				Relation: relation,
				Op:       sqlast.Implicit,
			}
			joins = append(joins, join)
			continue
		case SQLKeyword:
			word := tok.Value.(*SQLWord)

			switch word.Keyword {
			case "CROSS":
				p.mustNextToken()
				p.expectKeyword("JOIN")
				relation, err := p.parseTableFactor()
				if err != nil {
					return nil, errors.Errorf("parseTableFactor failed: %w", err)
				}
				join := &sqlast.Join{
					Relation: relation,
					Op:       sqlast.Cross,
				}
				joins = append(joins, join)
				continue
			case "NATURAL":
				p.mustNextToken()
				natural = true
			}
		default:
			natural = false
		}

		t, _ := p.peekToken()
		if t.Tok != SQLKeyword {
			break
		}

		word := t.Value.(*SQLWord)

		var join *sqlast.Join
		switch word.Keyword {
		case "INNER":
			p.mustNextToken()
			p.expectKeyword("JOIN")
			relation, err := p.parseTableFactor()
			if err != nil {
				return nil, errors.Errorf("parseTableFactor failed: %w", err)
			}
			constraint, err := p.parseJoinConstraint(natural)
			if err != nil {
				return nil, errors.Errorf("parseJoinConstraint failed: %w", err)
			}
			join = &sqlast.Join{
				Op:       sqlast.Inner,
				Relation: relation,
				Constant: constraint,
			}
		case "JOIN":
			p.mustNextToken()
			relation, err := p.parseTableFactor()
			if err != nil {
				return nil, errors.Errorf("parseTableFactor failed: %w", err)
			}
			constraint, err := p.parseJoinConstraint(natural)
			if err != nil {
				return nil, errors.Errorf("parseJoinConstraint failed: %w", err)
			}
			join = &sqlast.Join{
				Op:       sqlast.Inner,
				Relation: relation,
				Constant: constraint,
			}
		case "LEFT":
			p.mustNextToken()
			if _, err := p.parseKeyword("OUTER"); err != nil {
				return nil, errors.Errorf("parseKeyword failed: %w", err)
			}
			p.expectKeyword("JOIN")
			relation, err := p.parseTableFactor()
			if err != nil {
				return nil, errors.Errorf("parseTableFactor failed: %w", err)
			}
			constraint, err := p.parseJoinConstraint(natural)
			if err != nil {
				return nil, errors.Errorf("parseJoinConstraint failed: %w", err)
			}
			join = &sqlast.Join{
				Relation: relation,
				Op:       sqlast.LeftOuter,
				Constant: constraint,
			}
		case "RIGHT":
			p.mustNextToken()
			if _, err := p.parseKeyword("OUTER"); err != nil {
				return nil, errors.Errorf("parseKeyword failed: %w", err)
			}
			p.expectKeyword("JOIN")
			relation, err := p.parseTableFactor()
			if err != nil {
				return nil, errors.Errorf("parseTableFactor failed: %w", err)
			}
			constraint, err := p.parseJoinConstraint(natural)
			if err != nil {
				return nil, errors.Errorf("parseJoinConstraint failed: %w", err)
			}
			join = &sqlast.Join{
				Relation: relation,
				Op:       sqlast.RightOuter,
				Constant: constraint,
			}
		case "FULL":
			p.mustNextToken()
			if _, err := p.parseKeyword("OUTER"); err != nil {
				return nil, errors.Errorf("parseKeyword failed: %w", err)
			}
			p.expectKeyword("JOIN")
			relation, err := p.parseTableFactor()
			if err != nil {
				return nil, errors.Errorf("parseTableFactor failed: %w", err)
			}
			constraint, err := p.parseJoinConstraint(natural)
			if err != nil {
				return nil, errors.Errorf("parseJoinConstraint failed: %w", err)
			}
			join = &sqlast.Join{
				Relation: relation,
				Op:       sqlast.FullOuter,
				Constant: constraint,
			}
		default:
			break JOIN_LOOP
		}
		joins = append(joins, join)
	}

	return joins, nil
}

func (p *Parser) parseJoinConstraint(natural bool) (sqlast.JoinConstant, error) {
	if natural {
		return &sqlast.NaturalConstant{}, nil
	} else if ok, _ := p.parseKeyword("ON"); ok {
		constraint, err := p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}
		return &sqlast.OnJoinConstant{
			Node: constraint,
		}, nil
	} else if ok, _ := p.parseKeyword("USING"); ok {
		p.expectToken(LParen)
		attrs, err := p.parseColumnNames()
		if err != nil {
			return nil, errors.Errorf("parseColumnNames failed: %w", err)
		}
		p.expectToken(RParen)
		return &sqlast.UsingConstant{
			Idents: attrs,
		}, nil
	}

	log.Fatal("OR, or USING after JOIN")
	return nil, nil
}

func (p *Parser) parseCTEList() ([]*sqlast.CTE, error) {
	var ctes []*sqlast.CTE

	for {
		alias, err := p.parseIdentifier()
		if err != nil {
			return nil, errors.Errorf("parseIdentifier failed: %w", err)
		}
		p.expectKeyword("AS")
		p.expectToken(LParen)
		q, err := p.parseQuery()
		if err != nil {
			return nil, errors.Errorf("parseQuery failed: %w", err)
		}
		ctes = append(ctes, &sqlast.CTE{
			Alias: alias,
			Query: q,
		})
		p.expectToken(RParen)
		if ok, _ := p.consumeToken(Comma); !ok {
			break
		}
	}
	return ctes, nil
}

func (p *Parser) parseTableFactor() (sqlast.TableFactor, error) {
	isLateral, _ := p.parseKeyword("LATERAL")
	if ok, _ := p.consumeToken(LParen); ok {
		subquery, err := p.parseQuery()
		if err != nil {
			return nil, errors.Errorf("parseQuery failed: %w", err)
		}
		p.expectToken(RParen)
		alias := p.parseOptionalAlias(dialect.ReservedForTableAlias)
		return &sqlast.Derived{
			Lateral:  isLateral,
			SubQuery: subquery,
			Alias:    alias,
		}, nil
	} else if isLateral && !ok {
		t, _ := p.nextToken()
		return nil, errors.Errorf("after lateral expected %s but %+v", LParen, t)
	}

	name, err := p.parseObjectName()
	if err != nil {
		return nil, errors.Errorf("parseObjectName failed: %w", err)
	}
	var args []sqlast.ASTNode
	if ok, _ := p.consumeToken(LParen); ok {
		a, err := p.parseOptionalArgs()
		if err != nil {
			return nil, errors.Errorf("parseOptionalArgs failed: %w", err)
		}
		args = a
	}
	alias := p.parseOptionalAlias(dialect.ReservedForTableAlias)

	var withHints []sqlast.ASTNode
	if ok, _ := p.parseKeyword("WITH"); ok {
		if ok, _ := p.consumeToken(LParen); ok {
			h, err := p.parseExprList()
			if err != nil {
				return nil, errors.Errorf("parseExprList failed: %w", err)
			}
			withHints = h
			p.expectToken(RParen)
		} else {
			p.prevToken()
		}
	}

	return &sqlast.Table{
		Name:      name,
		Args:      args,
		Alias:     alias,
		WithHints: withHints,
	}, nil

}

func (p *Parser) parseLimit() (sqlast.ASTNode, error) {
	if ok, _ := p.parseKeyword("ALL"); ok {
		return nil, nil
	}

	i, err := p.parseLiteralInt()
	if err != nil {
		return nil, errors.Errorf("parseLiteralInt failed: %w", err)
	}

	return sqlast.NewLongValue(int64(i)), nil
}

func (p *Parser) parseIdentifier() (*sqlast.SQLIdent, error) {
	tok, err := p.nextToken()
	if err != nil {
		return nil, errors.Errorf("nextToken failed: %w", err)
	}
	word, ok := tok.Value.(*SQLWord)
	if !ok {
		return nil, errors.Errorf("expected identifier but %+v", tok)
	}

	return sqlast.NewSQLIdent(word.Value), nil
}

func (p *Parser) parseExprList() ([]sqlast.ASTNode, error) {
	var exprList []sqlast.ASTNode

	for {
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}
		exprList = append(exprList, expr)
		if tok, _ := p.peekToken(); tok != nil && tok.Tok == Comma {
			p.mustNextToken()
		} else {
			break
		}
	}

	return exprList, nil
}

func (p *Parser) parseColumnNames() ([]*sqlast.SQLIdent, error) {
	return p.parseListOfIds(Comma)
}

func (p *Parser) parseSubexpr(precedence uint) (sqlast.ASTNode, error) {
	expr, err := p.parsePrefix()
	if err != nil {
		return nil, errors.Errorf("parsePrefix failed: %w", err)
	}

	for {
		nextPrecedence, err := p.getNextPrecedence()
		if err != nil {
			return nil, errors.Errorf("getNextPrecedence failed: %w", err)
		}
		if precedence >= nextPrecedence {
			break
		}
		ex, err := p.parseInfix(expr, nextPrecedence)
		if err != nil {
			return nil, errors.Errorf("parseInfix failed: %w", err)
		}
		expr = ex
	}

	return expr, nil
}

func (p *Parser) parseInfix(expr sqlast.ASTNode, precedence uint) (sqlast.ASTNode, error) {
	operator := sqlast.None
	tok, err := p.nextToken()
	if err != nil {
		return nil, errors.Errorf("nextToken failed: %w", err)
	}

	switch tok.Tok {
	case Eq:
		operator = sqlast.Eq
	case Neq:
		operator = sqlast.NotEq
	case Gt:
		operator = sqlast.Gt
	case GtEq:
		operator = sqlast.GtEq
	case Lt:
		operator = sqlast.Lt
	case LtEq:
		operator = sqlast.LtEq
	case Plus:
		operator = sqlast.Plus
	case Minus:
		operator = sqlast.Minus
	case Mult:
		operator = sqlast.Multiply
	case Mod:
		operator = sqlast.Modulus
	case Div:
		operator = sqlast.Divide
	case SQLKeyword:
		word := tok.Value.(*SQLWord)
		switch word.Keyword {
		case "AND":
			operator = sqlast.And
		case "OR":
			operator = sqlast.Or
		case "LIKE":
			operator = sqlast.Like
		case "NOT":
			ok, _ := p.parseKeyword("LIKE")
			if ok {
				operator = sqlast.NotLike
			}
		}
	}

	if operator != sqlast.None {
		right, err := p.parseSubexpr(precedence)
		if err != nil {
			return nil, errors.Errorf("parseSubexpr failed: %w", err)
		}

		return &sqlast.SQLBinaryExpr{
			Left:  expr,
			Op:    operator,
			Right: right,
		}, nil
	}

	if tok.Tok == SQLKeyword {
		word := tok.Value.(*SQLWord)

		switch word.Keyword {
		case "IS":
			if ok, _ := p.parseKeyword("NULL"); ok {
				return &sqlast.SQLIsNull{
					X: expr,
				}, nil
			}
			if ok, _ := p.parseKeywords("NOT", "NULL"); ok {
				return &sqlast.SQLIsNotNull{
					X: expr,
				}, nil
			}
			return nil, errors.Errorf("NULL or NOT NULL after IS")
		case "NOT", "IN", "BETWEEN":
			p.prevToken()
			negated, _ := p.parseKeyword("NOT")
			if ok, _ := p.parseKeyword("IN"); ok {
				return p.parseIn(expr, negated)
			}
			if ok, _ := p.parseKeyword("BETWEEN"); ok {
				return p.parseBetween(expr, negated)
			}
		}
	}

	if tok.Tok == DoubleColon {
		return p.parsePGCast(expr)
	}

	log.Fatalf("no infix parser for token %+v", tok)
	return nil, nil
}

func (p *Parser) parsePGCast(expr sqlast.ASTNode) (sqlast.ASTNode, error) {
	tp, err := p.ParseDataType()
	if err != nil {
		return nil, errors.Errorf("ParseDataType failed: %w", err)
	}
	return &sqlast.SQLCast{
		Expr:     expr,
		DateType: tp,
	}, nil
}

func (p *Parser) parseIn(expr sqlast.ASTNode, negated bool) (sqlast.ASTNode, error) {
	p.expectToken(LParen)
	sok, _ := p.parseKeyword("SELECT")
	wok, _ := p.parseKeyword("WITH")
	var inop sqlast.ASTNode
	if sok || wok {
		p.prevToken()
		q, err := p.parseQuery()
		if err != nil {
			return nil, errors.Errorf("parseQuery failed: %w", err)
		}
		inop = &sqlast.SQLInSubQuery{
			Negated:  negated,
			Expr:     expr,
			SubQuery: q,
		}
	} else {
		list, err := p.parseExprList()
		if err != nil {
			return nil, errors.Errorf("parseExprList failed: %w", err)
		}
		inop = &sqlast.SQLInList{
			Expr:    expr,
			Negated: negated,
			List:    list,
		}
	}

	p.expectToken(RParen)

	return inop, nil
}

func (p *Parser) parseBetween(expr sqlast.ASTNode, negated bool) (sqlast.ASTNode, error) {
	low, err := p.parsePrefix()
	if err != nil {
		return nil, errors.Errorf("parsePrefix: %w", err)
	}
	p.expectKeyword("AND")
	high, err := p.parsePrefix()
	if err != nil {
		return nil, errors.Errorf("parsePrefix: %w", err)
	}

	return &sqlast.SQLBetween{
		Expr:    expr,
		Negated: negated,
		High:    high,
		Low:     low,
	}, nil

}

func (p *Parser) getNextPrecedence() (uint, error) {
	tok, _ := p.peekToken()
	if tok == nil {
		return 0, nil
	}
	return p.getPrecedence(tok), nil
}

func (p *Parser) getPrecedence(ts *TokenSet) uint {
	switch ts.Tok {
	case SQLKeyword:
		word := ts.Value.(*SQLWord)
		switch word.Keyword {
		case "OR":
			return 5
		case "AND":
			return 10
		case "NOT":
			return 15
		case "IS":
			return 17
		case "IN":
			return 20
		case "BETWEEN":
			return 20
		case "LIKE":
			return 20
		default:
			return 0
		}
	case Eq, Lt, LtEq, Neq, Gt, GtEq:
		return 20
	case Plus, Minus:
		return 30
	case Mult, Div, Mod:
		return 40
	case DoubleColon:
		return 50
	default:
		return 0
	}
}

func (p *Parser) parsePrefix() (sqlast.ASTNode, error) {
	tok, err := p.nextToken()
	if err != nil {
		return nil, errors.Errorf("nextToken error: %w", err)
	}

	switch tok.Tok {
	case SQLKeyword:
		word := tok.Value.(*SQLWord)
		switch word.Keyword {
		case "TRUE", "FALSE", "NULL":
			p.prevToken()
			t, err := p.parseSQLValue()
			if err != nil {
				return nil, errors.Errorf("parseSQLValue failed: %w", err)
			}
			return t, nil
		case "CASE":
			ast, err := p.parseCaseExpression()
			if err != nil {
				return nil, errors.Errorf("parseCaseExpression failed: %w", err)
			}
			return ast, nil
		case "CAST":
			ast, err := p.parseCastExpression()
			if err != nil {
				return nil, errors.Errorf("parseCastExpression failed: %w", err)
			}
			return ast, nil
		case "EXISTS":
			ast, err := p.parseExistsExpression(false)
			if err != nil {
				return nil, errors.Errorf("parseExistsExpression: %w", err)
			}
			return ast, nil
		case "NOT":
			if ok, _ := p.parseKeyword("EXISTS"); ok {
				ast, err := p.parseExistsExpression(true)
				if err != nil {
					return nil, errors.Errorf("parseExistsExpression: %w", err)
				}

				return ast, nil
			}

			ts := &TokenSet{
				Tok:   SQLKeyword,
				Value: MakeKeyword("NOT", 0),
			}
			precedence := p.getPrecedence(ts)
			expr, err := p.parseSubexpr(precedence)
			if err != nil {
				return nil, errors.Errorf("parseSubexpr failed: %w", err)
			}
			return &sqlast.SQLUnary{
				Operator: sqlast.Not,
				Expr:     expr,
			}, nil
		default:
			t, _ := p.peekToken()
			if t == nil || (t.Tok != LParen && t.Tok != Period) {
				return &sqlast.SQLIdentifier{
					Ident: word.AsSQLIdent(),
				}, nil
			}
			idParts := []*sqlast.SQLIdent{word.AsSQLIdent()}
			endWithWildcard := false

			for {
				if ok, _ := p.consumeToken(Period); !ok {
					break
				}
				n, err := p.nextToken()
				if err != nil {
					return nil, errors.Errorf("nextToken failed: %w", err)
				}

				if n.Tok == SQLKeyword {
					w := n.Value.(*SQLWord)
					idParts = append(idParts, w.AsSQLIdent())
					continue
				}
				if n.Tok == Mult {
					endWithWildcard = true
					break
				}

				return nil, errors.Errorf("an identifier or '*' after '.'")
			}

			if endWithWildcard {
				return &sqlast.SQLQualifiedWildcard{
					Idents: idParts,
				}, nil
			}

			if ok, _ := p.consumeToken(LParen); ok {
				p.prevToken()
				name := &sqlast.SQLObjectName{
					Idents: idParts,
				}
				f, err := p.parseFunction(name)
				if err != nil {
					return nil, errors.Errorf("parseFunction failed: %w", err)
				}
				return f, nil
			}

			return &sqlast.SQLCompoundIdentifier{
				Idents: idParts,
			}, nil
		}
	case Mult:
		return &sqlast.SQLWildcard{}, nil
	case Plus:
		precedence := p.getPrecedence(tok)
		expr, err := p.parseSubexpr(precedence)
		if err != nil {
			return nil, errors.Errorf("parseSubexpr failed: %w", err)
		}
		return &sqlast.SQLUnary{
			Operator: sqlast.Plus,
			Expr:     expr,
		}, nil
	case Minus:
		precedence := p.getPrecedence(tok)
		expr, err := p.parseSubexpr(precedence)
		if err != nil {
			return nil, errors.Errorf("parseSubexpr failed: %w", err)
		}
		return &sqlast.SQLUnary{
			Operator: sqlast.Minus,
			Expr:     expr,
		}, nil
	case Number, SingleQuotedString, NationalStringLiteral:
		p.prevToken()
		v, err := p.parseSQLValue()
		if err != nil {
			return nil, errors.Errorf("parseSQLValue failed", err)
		}
		return v, nil
	case LParen:
		sok, _ := p.parseKeyword("SELECT")
		wok, _ := p.parseKeyword("WITH")

		var ast sqlast.ASTNode

		if sok || wok {
			p.prevToken()
			expr, err := p.parseQuery()
			if err != nil {
				return nil, errors.Errorf("parseQuery failed: %w", err)
			}
			ast = &sqlast.SQLSubquery{
				Query: expr,
			}
		} else {
			expr, err := p.ParseExpr()
			if err != nil {
				return nil, errors.Errorf("parseQuery failed: %w", err)
			}
			ast = &sqlast.SQLNested{
				AST: expr,
			}
		}
		p.expectToken(RParen)
		return ast, nil
	}
	log.Fatal("prefix parser expected a keyword but hit EOF")
	return nil, nil
}

func (p *Parser) parseFunction(name *sqlast.SQLObjectName) (sqlast.ASTNode, error) {
	p.expectToken(LParen)
	args, err := p.parseOptionalArgs()
	if err != nil {
		return nil, errors.Errorf("parseOptionalArgs failed: %w", err)
	}
	var over *sqlast.SQLWindowSpec
	if ok, _ := p.parseKeyword("OVER"); ok {
		p.expectToken(LParen)

		var partitionBy []sqlast.ASTNode
		if ok, _ := p.parseKeywords("PARTITION", "BY"); ok {
			el, err := p.parseExprList()
			if err != nil {
				return nil, errors.Errorf("parseExprList failed: %w", err)
			}
			partitionBy = el
		}

		var orderBy []*sqlast.SQLOrderByExpr
		if ok, _ := p.parseKeywords("ORDER", "BY"); ok {
			el, err := p.parseOrderByExprList()
			if err != nil {
				return nil, errors.Errorf("parseOrderByExprList failed: %w", err)
			}
			orderBy = el
		}

		windowFrame, err := p.parseWindowFrame()
		if err != nil {
			return nil, errors.Errorf("parseWindowFrame failed: %w", err)
		}

		over = &sqlast.SQLWindowSpec{
			PartitionBy:  partitionBy,
			OrderBy:      orderBy,
			WindowsFrame: windowFrame,
		}
	}

	return &sqlast.SQLFunction{
		Name: name,
		Args: args,
		Over: over,
	}, nil
}

func (p *Parser) parseOptionalArgs() ([]sqlast.ASTNode, error) {
	if ok, _ := p.consumeToken(RParen); ok {
		var args []sqlast.ASTNode
		return args, nil
	} else {
		as, err := p.parseExprList()
		if err != nil {
			return nil, errors.Errorf("parseExprList failed: %w", err)
		}
		p.expectToken(RParen)
		return as, nil
	}
}

func (p *Parser) parseOrderByExprList() ([]*sqlast.SQLOrderByExpr, error) {
	var exprList []*sqlast.SQLOrderByExpr

	for {
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}
		var asc *bool

		if ok, _ := p.parseKeyword("ASC"); ok {
			b := true
			asc = &b
		} else if ok, _ := p.parseKeyword("DESC"); ok {
			b := false
			asc = &b
		}

		exprList = append(exprList, &sqlast.SQLOrderByExpr{
			Expr: expr,
			ASC:  asc,
		})

		if t, _ := p.peekToken(); t != nil && t.Tok == Comma {
			p.mustNextToken()
		} else {
			break
		}
	}

	return exprList, nil
}

func (p *Parser) parseWindowFrame() (*sqlast.SQLWindowFrame, error) {
	var windowFrame *sqlast.SQLWindowFrame
	t, _ := p.peekToken()
	if t.Tok == SQLKeyword {
		w := t.Value.(*SQLWord)
		var units sqlast.SQLWindowFrameUnits
		units = units.FromStr(w.Keyword)
		p.mustNextToken()

		if ok, _ := p.parseKeyword("BETWEEN"); ok {
			startBound, err := p.parseWindowFrameBound()
			if err != nil {
				return nil, errors.Errorf("parseWindowFrameBound: %w", err)
			}
			p.expectKeyword("AND")
			endBound, err := p.parseWindowFrameBound()
			if err != nil {
				return nil, errors.Errorf("parseWindowFrameBound: %w", err)
			}

			windowFrame = &sqlast.SQLWindowFrame{
				StartBound: startBound,
				EndBound:   endBound,
				Units:      units,
			}
		} else {
			startBound, err := p.parseWindowFrameBound()
			if err != nil {
				return nil, errors.Errorf("parseWindowFrameBound: %w", err)
			}
			windowFrame = &sqlast.SQLWindowFrame{
				StartBound: startBound,
				Units:      units,
			}
		}
	}

	p.expectToken(RParen)
	return windowFrame, nil
}

func (p *Parser) parseWindowFrameBound() (sqlast.SQLWindowFrameBound, error) {
	if ok, _ := p.parseKeywords("CURRENT", "ROW"); ok {
		return &sqlast.CurrentRow{}, nil
	}

	var rows *uint64
	if ok, _ := p.parseKeyword("UNBOUNDED"); ok {
		if ok, _ := p.parseKeyword("PRECEDING"); ok {
			return &sqlast.UnboundedPreceding{}, nil
		}
		if ok, _ := p.parseKeyword("FOLLOWING"); ok {
			return &sqlast.UnboundedFollowing{}, nil
		}
	} else {
		i, err := p.parseLiteralInt()
		if err != nil {
			return nil, errors.Errorf("parseLiteralInt failed: %w", err)
		}
		if i < 0 {
			return nil, errors.Errorf("the number of rows must ne non-negative, got %d", i)
		}
		ui := uint64(i)
		rows = &ui
	}

	if ok, _ := p.parseKeyword("PRECEDING"); ok {
		return &sqlast.Preceding{Bound: rows}, nil
	}
	if ok, _ := p.parseKeyword("FOLLOWING"); ok {
		return &sqlast.Following{Bound: rows}, nil
	}
	log.Fatal("expected PRECEDING or FOLLOWING")
	return nil, nil
}

func (p *Parser) parseObjectName() (*sqlast.SQLObjectName, error) {
	idents, err := p.parseListOfIds(Period)
	if err != nil {
		return nil, errors.Errorf("parseListOfId: %w", err)
	}
	return &sqlast.SQLObjectName{
		Idents: idents,
	}, nil
}

func (p *Parser) parseSQLValue() (sqlast.ASTNode, error) {
	return p.parseValue()
}

func (p *Parser) parseValue() (sqlast.ASTNode, error) {
	tok, err := p.nextToken()
	if err != nil {
		return nil, errors.Errorf("nextToken failed: %w", err)
	}

	switch tok.Tok {
	case SQLKeyword:
		word := tok.Value.(*SQLWord)

		switch word.Keyword {
		case "TRUE":
			return sqlast.NewBooleanValue(true), nil
		case "FALSE":
			return sqlast.NewBooleanValue(false), nil
		case "NULL":
			return sqlast.NewNullValue(), nil
		default:
			return nil, errors.Errorf("unexpected token %v", word)
		}
	case Number:
		num := tok.Value.(string)
		if strings.Contains(num, ".") {
			f, err := strconv.ParseFloat(num, 64)
			if err != nil {
				return nil, errors.Errorf("parseFloat failed %s", num)
			}
			return sqlast.NewDoubleValue(f), nil
		} else {
			i, _ := strconv.Atoi(num)
			return sqlast.NewLongValue(int64(i)), nil
		}
	case SingleQuotedString:
		str := tok.Value.(string)
		return sqlast.NewSingleQuotedString(str), nil
	case NationalStringLiteral:
		str := tok.Value.(string)
		return sqlast.NewNationalStringLiteral(str), nil
	default:
		return nil, errors.Errorf("unexpected token %v", tok)
	}

}

func (p *Parser) parseOptionalPrecision() (*uint, error) {
	if ok, _ := p.consumeToken(LParen); ok {
		n, err := p.parseLiteralInt()
		if err != nil {
			return nil, errors.Errorf("parseLiteralInt failed: %w", err)
		}
		p.expectToken(RParen)
		i := uint(n)
		return &i, nil
	} else {
		return nil, nil
	}
}

func (p *Parser) parseOptionalPrecisionScale() (*uint, *uint, error) {
	if ok, _ := p.consumeToken(LParen); !ok {
		return nil, nil, nil
	}
	n, err := p.parseLiteralInt()
	if err != nil {
		return nil, nil, errors.Errorf("parseLiteralInt failed: %w", err)
	}
	var scale *uint
	if ok, _ := p.consumeToken(Comma); ok {
		s, err := p.parseLiteralInt()
		if err != nil {
			return nil, nil, errors.Errorf("parseLiteralInt failed: %w", err)
		}
		us := uint(s)
		scale = &us
	}
	p.expectToken(RParen)
	i := uint(n)
	return &i, scale, nil
}

func (p *Parser) parseLiteralInt() (int, error) {
	tok, _ := p.nextToken()
	if tok.Tok != Number {
		return 0, errors.Errorf("expect literal int but %v", tok.Tok)
	}
	istr := tok.Value.(string)
	i, err := strconv.Atoi(istr)
	if err != nil {
		return 0, errors.Errorf("strconv.Atoi failed: %w", err)
	}

	return i, nil
}

func (p *Parser) parseListOfIds(separator Token) ([]*sqlast.SQLIdent, error) {
	var idents []*sqlast.SQLIdent
	expectIdentifier := true

	for {
		tok, _ := p.nextToken()
		if tok == nil {
			break
		}
		if tok.Tok == SQLKeyword && expectIdentifier {
			expectIdentifier = false
			word := tok.Value.(*SQLWord)
			idents = append(idents, word.AsSQLIdent())
			continue
		} else if tok.Tok == separator && !expectIdentifier {
			expectIdentifier = true
			continue
		}
		p.prevToken()
		break
	}

	if expectIdentifier {
		return nil, errors.Errorf("expect identifier")
	}

	return idents, nil
}

func (p *Parser) parseCaseExpression() (sqlast.ASTNode, error) {
	var operand sqlast.ASTNode

	if ok, _ := p.parseKeyword("WHEN"); !ok {
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}
		operand = expr
		p.expectKeyword("WHEN")
	}

	var conditions []sqlast.ASTNode
	var results []sqlast.ASTNode

	for {
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}
		conditions = append(conditions, expr)
		p.expectKeyword("THEN")
		result, err := p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}
		results = append(results, result)
		if ok, _ := p.parseKeyword("WHEN"); !ok {
			break
		}
	}
	var elseResult sqlast.ASTNode

	if ok, _ := p.parseKeyword("ELSE"); ok {
		result, err := p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}
		elseResult = result
	}
	p.expectKeyword("END")

	return &sqlast.SQLCase{
		Operand:    operand,
		Conditions: conditions,
		Results:    results,
		ElseResult: elseResult,
	}, nil

}

func (p *Parser) parseCastExpression() (sqlast.ASTNode, error) {
	p.expectToken(LParen)
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, errors.Errorf("ParseExpr failed: %w", err)
	}
	p.expectKeyword("AS")
	dataType, err := p.ParseDataType()
	if err != nil {
		return nil, errors.Errorf("ParseDataType")
	}
	p.expectToken(RParen)

	return &sqlast.SQLCast{
		Expr:     expr,
		DateType: dataType,
	}, nil
}

func (p *Parser) parseExistsExpression(negated bool) (sqlast.ASTNode, error) {
	p.expectToken(LParen)
	expr, err := p.parseQuery()
	if err != nil {
		return nil, errors.Errorf("parseQuery failed: %w", err)
	}
	p.expectToken(RParen)

	return &sqlast.SQLExists{
		Negated: negated,
		Query:   expr,
	}, nil
}

func (p *Parser) expectKeyword(expected string) {
	ok, err := p.parseKeyword(expected)
	if err != nil || !ok {
		for i := 0; i < int(p.index); i++ {
			fmt.Printf("%v", p.tokens[i].Value)
		}
		fmt.Println()
		log.Fatalf("should be expected keyword: %s err: %v", expected, err)
	}
}

func (p *Parser) expectToken(expected Token) {
	ok, err := p.consumeToken(expected)
	if err != nil || !ok {
		tok, _ := p.peekToken()

		for i := 0; i < int(p.index); i++ {
			fmt.Printf("%v", p.tokens[i].Value)
		}
		fmt.Println()
		log.Fatalf("should be %s token, but %+v,  err: %+v", expected, tok, err)
	}
}

func (p *Parser) consumeToken(expected Token) (bool, error) {
	tok, err := p.peekToken()
	if err != nil {
		return false, err
	}

	if tok.Tok == expected {
		if _, err := p.nextToken(); err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}

func (p *Parser) mustNextToken() *TokenSet {
	tok, err := p.nextToken()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	return tok
}

func (p *Parser) nextToken() (*TokenSet, error) {
	for {
		tok, err := p.nextTokenNoSkip()
		if err != nil {
			return nil, err
		}
		if tok.Tok == Whitespace {
			continue
		}
		return tok, nil
	}
}

var TokenAlreadyConsumed = errors.New("tokens are already consumed")

func (p *Parser) nextTokenNoSkip() (*TokenSet, error) {
	if p.index < uint(len(p.tokens)) {
		p.index += 1
		return p.tokens[p.index-1], nil
	}
	return nil, TokenAlreadyConsumed
}

func (p *Parser) prevToken() *TokenSet {
	for {
		tok := p.prevTokenNoSkip()
		if tok.Tok == Whitespace {
			continue
		}
		return tok
	}
}

func (p *Parser) prevTokenNoSkip() *TokenSet {
	if p.index > 0 {
		p.index -= 1
		return p.tokens[p.index]
	}
	return nil
}

func (p *Parser) peekToken() (*TokenSet, error) {
	u, err := p.tilNonWhitespace()
	if err != nil {
		return nil, err
	}
	return p.tokens[u], nil
}

func (p *Parser) tokenAt(n uint) *TokenSet {
	return p.tokens[n]
}

func (p *Parser) tilNonWhitespace() (uint, error) {
	idx := p.index
	for {
		if idx >= uint(len(p.tokens)) {
			return 0, TokenAlreadyConsumed
		}
		tok := p.tokens[idx]
		if tok.Tok == Whitespace {
			idx += 1
			continue
		}
		return idx, nil
	}
}

func (p *Parser) parseKeywords(keywords ...string) (bool, error) {
	idx := p.index

	for _, k := range keywords {
		if ok, _ := p.parseKeyword(k); !ok {
			p.index = idx
			return false, nil
		}
	}

	return true, nil
}

func (p *Parser) parseKeyword(expected string) (bool, error) {
	tok, err := p.peekToken()
	if err != nil {
		return false, errors.Errorf("parseKeyword %s failed: %w", expected, err)
	}

	word, ok := tok.Value.(*SQLWord)
	if !ok {
		return false, nil
	}

	if strings.EqualFold(word.Value, expected) {
		p.mustNextToken()
		return true, nil
	}
	return false, nil
}

func containsStr(strmap map[string]struct{}, t string) bool {
	_, ok := strmap[t]
	return ok
}
