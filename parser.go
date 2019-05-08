package xsqlparser

import (
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
		return nil, errors.Errorf("tokenize err %w", err)
	}

	return &Parser{tokens: set, index: 0}, nil
}

func (p *Parser) ParseSQL() ([]sqlast.SQLStmt, error) {
	var stmts []sqlast.SQLStmt
	var expectingDelimiter bool

	for {
		for {
			ok, err := p.consumeToken(Semicolon)
			if err != nil {
				return nil, err
			}
			expectingDelimiter = false
			if !ok {
				break
			}
		}

		t, err := p.peekToken()
		if err == TokenAlreadyConsumed {
			break
		}
		if expectingDelimiter {
			return nil, errors.Errorf("unexpected token %+v", t)
		}

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
	default:
		return nil, errors.Errorf("unexpected (or unsupported) keyword %s", word.Keyword)
	}
	return nil, errors.New("unreachable")
}

func (p *Parser) parseQuery() (*sqlast.SQLQuery, error) {
	hasCTE, _ := p.parseKeyword("WITH")
	var ctes []*sqlast.CTE
	if hasCTE {
		cts, err := p.parseCTEList()
		if err != nil {
			return nil, errors.Errorf("parseCTEList failed %w", err)
		}
		ctes = cts
	}

	body, err := p.parseQueryBody(0)
	if err != nil {
		return nil, errors.Errorf("parseQueryBody failed %w", err)
	}

	var orderBy []*sqlast.SQLOrderByExpr
	if ok, _ := p.parseKeywords("ORDER", "BY"); ok {
		o, err := p.parseOrderByExprList()
		if err != nil {
			return nil, errors.Errorf("parseOrderByExprList failed %w", err)
		}
		orderBy = o
	}

	var limit sqlast.ASTNode
	if ok, _ := p.parseKeyword("LIMIT"); ok {
		l, err := p.parseLimit()
		if err != nil {
			return nil, errors.Errorf("parseLimit failed %w", err)
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
	var expr sqlast.ASTNode
	if ok, _ := p.parseKeyword("SELECT"); ok {
		s, err := p.parseSelect()
		if err != nil {
			return nil, errors.Errorf("parseSelect failed %w", err)
		}
		expr = s
	} else if ok, _ := p.consumeToken(LParen); ok {
		subquery, err := p.parseQuery()
		if err != nil {
			return nil, errors.Errorf("parseQuery failed %w", err)
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
		p.nextToken()
		all, _ := p.parseKeyword("ALL")
		right, err := p.parseQueryBody(nextPrecedence)
		if err != nil {
			return nil, errors.Errorf("parseQueryBody failed %w", err)
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
		return nil, errors.Errorf("parseKeyword failed %w", err)
	}
	projection, err := p.parseSelectList()
	if err != nil {
		return nil, errors.Errorf("parseSelectList failed %w", err)
	}
	var relation sqlast.TableFactor
	var joins []*sqlast.Join

	if ok, _ := p.parseKeyword("FROM"); ok {
		t, err := p.parseTableFactor()
		if err != nil {
			return nil, errors.Errorf("parseTableFactor failed %w", err)
		}
		relation = t
		j, err := p.parseJoins()
		if err != nil {
			return nil, errors.Errorf("parseJoins failed %w", err)
		}
		joins = j
	}

	var selection sqlast.ASTNode
	if ok, _ := p.parseKeyword("WHERE"); ok {
		s, err := p.parseExpr()
		if err != nil {
			return nil, errors.Errorf("parseExpr failed %w", err)
		}
		selection = s
	}

	var groupBy []sqlast.ASTNode
	if ok, _ := p.parseKeywords("GROUP", "BY"); ok {
		g, err := p.parseExprList()
		if err != nil {
			return nil, errors.Errorf("parseExprList failed %w", err)
		}
		groupBy = g
	}

	var having sqlast.ASTNode
	if ok, _ := p.parseKeyword("HAVING"); ok {
		h, err := p.parseExpr()
		if err != nil {
			return nil, errors.Errorf("parseExpr failed %w", err)
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
		expr, err := p.parseExpr()
		if err != nil {
			return nil, errors.Errorf("parseExpr failed %w", err)
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
			p.nextToken()
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
		return nil, errors.Errorf("parseObjectName failed %w", err)
	}

	columns, err := p.parseColumns()
	if err != nil {
		return nil, errors.Errorf("parseColumns failed %w", err)
	}

	return &sqlast.SQLCreateTable{
		Name:     name,
		Columns:  columns,
		External: false,
	}, nil
}

func (p *Parser) parseCreateView() (sqlast.SQLStmt, error) {
	materialized, _ := p.parseKeyword("MATERIALIZED")
	p.expectKeyword("VIEW")
	name, err := p.parseObjectName()
	if err != nil {
		return nil, errors.Errorf("parseObjectName failed %w", err)
	}
	p.expectKeyword("AS")
	q, err := p.parseQuery()
	if err != nil {
		return nil, errors.Errorf("parseQuery failed %w", err)
	}

	return &sqlast.SQLCreateView{
		Materialized: materialized,
		Name:         name,
		Query:        q,
	}, nil

}

func (p *Parser) parseColumns() ([]*sqlast.SQLColumnDef, error) {
	var columns []*sqlast.SQLColumnDef
	if ok, _ := p.consumeToken(LParen); !ok {
		return columns, nil
	}

	for {
		tok, _ := p.nextToken()
		if tok == nil || tok.Tok != SQLKeyword {
			return nil, errors.Errorf("parse error after column def")
		}

		columnName := tok.Value.(*SQLWord)

		dataType, err := p.parseDataType()
		if err != nil {
			return nil, errors.Errorf("parseDataType failed %w", err)
		}
		isPrimary, _ := p.parseKeywords("PRIMARY", "KEY")
		isUnique, _ := p.parseKeyword("UNIQUE")

		var def sqlast.ASTNode
		if ok, _ := p.parseKeyword("DEFAULT"); ok {
			def, err = p.parseDefaultExpr(0)
			if err != nil {
				return nil, errors.Errorf("parseDefaultExpr failed %w", err)
			}
		}

		var allowNull bool
		if ok, _ := p.parseKeywords("NOT", "NULL"); ok {
			allowNull = false
		} else {
			p.parseKeyword("NULL")
			allowNull = true
		}

		columns = append(columns, &sqlast.SQLColumnDef{
			Name:      columnName.AsSQLIdent(),
			DateType:  dataType,
			IsPrimary: isPrimary,
			IsUnique:  isUnique,
			Default:   def,
			AllowNull: allowNull,
		})

		t, _ := p.nextToken()
		if t == nil || (t.Tok != Comma && t.Tok != RParen) {
			log.Fatal("Expected ',' or ')' after column definition")
		} else if t.Tok == RParen {
			break
		}
	}

	return columns, nil
}

func (p *Parser) parseDelete() (sqlast.SQLStmt, error) {
	p.expectKeyword("FROM")
	tableName, err := p.parseObjectName()
	if err != nil {
		return nil, errors.Errorf("parseObjectName failed %w", err)
	}

	var selection sqlast.ASTNode
	if ok, _ := p.parseKeyword("WHERE"); ok {
		selection, err = p.parseExpr()
		if err != nil {
			return nil, errors.Errorf("parseExpr failed %w", err)
		}
	}

	return &sqlast.SQLDelete{
		TableName: tableName,
		Selection: selection,
	}, nil
}

func (p *Parser) parseInsert() (sqlast.SQLStmt, error) {
	p.expectKeyword("INTO")
	tableName, err := p.parseObjectName()

	if err != nil {
		return nil, errors.Errorf("parseObjectName failed %w", err)
	}
	var columns []*sqlast.SQLIdent

	if ok, _ := p.consumeToken(LParen); ok {
		columns, err = p.parseColumnNames()
		if err != nil {
			return nil, errors.Errorf("parseColumnNames failed %w", err)
		}
		p.expectToken(RParen)
	}

	p.expectKeyword("VALUES")
	var values [][]sqlast.ASTNode

	for {
		p.expectToken(LParen)
		v, err := p.parseExprList()
		if err != nil {
			return nil, errors.Errorf("parseExprList failed %w", err)
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
	p.parseKeyword("ONLY")
	tableName, err := p.parseObjectName()
	if err != nil {
		return nil, errors.Errorf("parseObjectName failed %w", err)
	}

	var operaton sqlast.AlterOperation

	if ok, _ := p.parseKeyword("ADD"); ok {
		if ok, _ := p.parseKeyword("CONSTRAINT"); ok {
			constraintName, err := p.parseIdentifier()
			if err != nil {
				return nil, errors.Errorf("parseIdentifier failed %w", err)
			}
			tableKey, err := p.parseTableKey(constraintName)
			if err != nil {
				return nil, errors.Errorf("parseTableKey failed %w", err)
			}
			operaton = &sqlast.AddConstraint{
				TableKey: tableKey,
			}

		} else {
			return nil, errors.Errorf("CONSTRAINT after ADD")
		}
	} else {
		return nil, errors.Errorf("ADD after ALTER TABLE")
	}

	return &sqlast.SQLAlterTable{
		TableName: tableName,
		Operation: operaton,
	}, nil

}

func (p *Parser) parseDefaultExpr(precedence uint) (sqlast.ASTNode, error) {
	expr, err := p.parsePrefix()
	if err != nil {
		return nil, errors.Errorf("parsePrefix failed %w", err)
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
			return nil, errors.Errorf("getNextPrecedence failed %w", err)
		}
		if precedence >= nextPrecedence {
			break
		}
		expr, err = p.parseInfix(expr, nextPrecedence)
		if err != nil {
			return nil, errors.Errorf("parseInfix failed %w")
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
			p.nextToken()
			relation, err := p.parseTableFactor()
			if err != nil {
				return nil, errors.Errorf("parseTableFactor failed %w", err)
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
				p.nextToken()
				p.expectKeyword("JOIN")
				relation, err := p.parseTableFactor()
				if err != nil {
					return nil, errors.Errorf("parseTableFactor failed %w", err)
				}
				join := &sqlast.Join{
					Relation: relation,
					Op:       sqlast.Cross,
				}
				joins = append(joins, join)
				continue
			case "NATURAL":
				p.nextToken()
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
			p.nextToken()
			p.expectKeyword("JOIN")
			relation, err := p.parseTableFactor()
			if err != nil {
				return nil, errors.Errorf("parseTableFactor failed %w", err)
			}
			constraint, err := p.parseJoinConstraint(natural)
			if err != nil {
				return nil, errors.Errorf("parseJoinConstraint failed %w", err)
			}
			join = &sqlast.Join{
				Op:       sqlast.Inner,
				Relation: relation,
				Constant: constraint,
			}
		case "JOIN":
			p.nextToken()
			relation, err := p.parseTableFactor()
			if err != nil {
				return nil, errors.Errorf("parseTableFactor failed %w", err)
			}
			constraint, err := p.parseJoinConstraint(natural)
			if err != nil {
				return nil, errors.Errorf("parseJoinConstraint failed %w", err)
			}
			join = &sqlast.Join{
				Op:       sqlast.Inner,
				Relation: relation,
				Constant: constraint,
			}
		case "LEFT":
			p.nextToken()
			p.parseKeyword("OUTER")
			p.expectKeyword("JOIN")
			relation, err := p.parseTableFactor()
			if err != nil {
				return nil, errors.Errorf("parseTableFactor failed %w", err)
			}
			constraint, err := p.parseJoinConstraint(natural)
			if err != nil {
				return nil, errors.Errorf("parseJoinConstraint failed %w", err)
			}
			join = &sqlast.Join{
				Relation: relation,
				Op:       sqlast.LeftOuter,
				Constant: constraint,
			}
		case "RIGHT":
			p.nextToken()
			p.parseKeyword("OUTER")
			p.expectKeyword("JOIN")
			relation, err := p.parseTableFactor()
			if err != nil {
				return nil, errors.Errorf("parseTableFactor failed %w", err)
			}
			constraint, err := p.parseJoinConstraint(natural)
			if err != nil {
				return nil, errors.Errorf("parseJoinConstraint failed %w", err)
			}
			join = &sqlast.Join{
				Relation: relation,
				Op:       sqlast.RightOuter,
				Constant: constraint,
			}
		case "FULL":
			p.nextToken()
			p.parseKeyword("OUTER")
			p.expectKeyword("JOIN")
			relation, err := p.parseTableFactor()
			if err != nil {
				return nil, errors.Errorf("parseTableFactor failed %w", err)
			}
			constraint, err := p.parseJoinConstraint(natural)
			if err != nil {
				return nil, errors.Errorf("parseJoinConstraint failed %w", err)
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
		constraint, err := p.parseExpr()
		if err != nil {
			return nil, errors.Errorf("parseExpr failed %w", err)
		}
		return &sqlast.OnJoinConstant{
			Node: constraint,
		}, nil
	} else if ok, _ := p.parseKeyword("USING"); ok {
		p.expectToken(LParen)
		attrs, err := p.parseColumnNames()
		if err != nil {
			return nil, errors.Errorf("parseColumnNames failed %w", err)
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
			return nil, errors.Errorf("parseIdentifier failed %w", err)
		}
		p.expectKeyword("AS")
		p.expectToken(LParen)
		q, err := p.parseQuery()
		if err != nil {
			return nil, errors.Errorf("parseQuery failed %w", err)
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
	if ok, _ := p.consumeToken(LParen); ok {
		subquery, err := p.parseQuery()
		if err != nil {
			return nil, errors.Errorf("parseQuery failed %w", err)
		}
		p.expectToken(RParen)
		alias := p.parseOptionalAlias(dialect.ReservedForTableAlias)
		return &sqlast.Derived{
			SubQuery: subquery,
			Alias:    alias,
		}, nil
	}

	name, err := p.parseObjectName()
	if err != nil {
		return nil, errors.Errorf("parseObjectName failed %w", err)
	}
	var args []sqlast.ASTNode
	if ok, _ := p.consumeToken(LParen); ok {
		a, err := p.parseOptionalArgs()
		if err != nil {
			return nil, errors.Errorf("parseOptionalArgs failed %w", err)
		}
		args = a
	}
	alias := p.parseOptionalAlias(dialect.ReservedForTableAlias)

	var withHints []sqlast.ASTNode
	if ok, _ := p.parseKeyword("WITH"); ok {
		if ok, _ := p.consumeToken(LParen); ok {
			h, err := p.parseExprList()
			if err != nil {
				return nil, errors.Errorf("parseExprList failed %w", err)
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

func (p *Parser) parseTableKey(constraintName *sqlast.SQLIdent) (sqlast.TableKey, error) {
	isPrimaryKey, _ := p.parseKeywords("PRIMARY", "KEY")
	isUniqueKey, _ := p.parseKeyword("UNIQUE")
	isForeignKey, _ := p.parseKeywords("FOREIGN", "KEY")

	p.expectToken(LParen)

	columns, err := p.parseColumnNames()
	if err != nil {
		return nil, errors.Errorf("parseColumnNames %w", err)
	}
	p.expectToken(RParen)
	k := &sqlast.Key{
		Name:    constraintName,
		Columns: columns,
	}

	if isPrimaryKey {
		return &sqlast.PrimaryKey{
			Key: k,
		}, nil
	}

	if isUniqueKey {
		p.parseKeyword("KEY")
		return &sqlast.UniqueKey{
			Key: k,
		}, nil
	}

	if isForeignKey {
		p.expectKeyword("REFERENCES")
		foreignTable, err := p.parseObjectName()
		if err != nil {
			return nil, errors.Errorf("parseObjectName %w", err)
		}
		p.expectToken(LParen)
		referredColumns, err := p.parseColumnNames()
		if err != nil {
			return nil, errors.Errorf("parseColumnNames %w", err)
		}
		p.expectToken(RParen)

		return &sqlast.ForeignKey{
			Key:             k,
			ForeignTable:    foreignTable,
			ReferredColumns: referredColumns,
		}, nil
	}

	return nil, errors.Errorf("expecting primary key, unique key, or foreign key")
}

func (p *Parser) parseLimit() (sqlast.ASTNode, error) {
	if ok, _ := p.parseKeyword("ALL"); ok {
		return nil, nil
	}

	i, err := p.parseLiteralInt()
	if err != nil {
		return nil, errors.Errorf("parseLiteralInt failed %w", err)
	}

	return sqlast.NewLongValue(int64(i)), nil
}

func (p *Parser) expectToken(expected Token) {
	ok, err := p.consumeToken(expected)
	if err != nil || !ok {
		tok, _ := p.peekToken()
		log.Fatalf("should be %s token, but %v,  err: %+v", expected, tok, err)
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

func (p *Parser) parseIdentifier() (*sqlast.SQLIdent, error) {
	tok, err := p.nextToken()
	if err != nil {
		return nil, errors.Errorf("nextToken failed %w", err)
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
		expr, err := p.parseExpr()
		if err != nil {
			return nil, errors.Errorf("parseExpr failed %w", err)
		}
		exprList = append(exprList, expr)
		if tok, _ := p.peekToken(); tok != nil && tok.Tok == Comma {
			p.nextToken()
		} else {
			break
		}
	}

	return exprList, nil
}

func (p *Parser) parseColumnNames() ([]*sqlast.SQLIdent, error) {
	return p.parseListOfIds(Comma)
}

func (p *Parser) parseExpr() (sqlast.ASTNode, error) {
	return p.parseSubexpr(0)
}

func (p *Parser) parseSubexpr(precedence uint) (sqlast.ASTNode, error) {
	expr, err := p.parsePrefix()
	if err != nil {
		return nil, errors.Errorf("parsePrefix failed %w", err)
	}

	for {
		nextPrecedence, err := p.getNextPrecedence()
		if err != nil {
			return nil, errors.Errorf("getNextPrecedence failed %w", err)
		}
		if precedence >= nextPrecedence {
			break
		}
		ex, err := p.parseInfix(expr, nextPrecedence)
		if err != nil {
			return nil, errors.Errorf("parseInfix failed %w", err)
		}
		expr = ex
	}

	return expr, nil
}

func (p *Parser) parseInfix(expr sqlast.ASTNode, precedence uint) (sqlast.ASTNode, error) {
	operator := sqlast.None
	tok, err := p.nextToken()
	if err != nil {
		return nil, errors.Errorf("nextToken failed %w", err)
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
			return nil, errors.Errorf("parseSubexpr failed %w", err)
		}

		return &sqlast.SQLBinaryExpr{
			Left:  expr,
			Op:    operator,
			Right: right,
		}, nil
	}

	if tok.Tok == SQLKeyword {
		word := tok.Value.(*SQLWord)

		switch word.Value {
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
	tp, err := p.parseDataType()
	if err != nil {
		return nil, errors.Errorf("parseDataType failed %w", err)
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
			return nil, errors.Errorf("parseQuery failed %w", err)
		}
		inop = &sqlast.SQLInSubQuery{
			Negated:  negated,
			Expr:     expr,
			SubQuery: q,
		}
	} else {
		list, err := p.parseExprList()
		if err != nil {
			return nil, errors.Errorf("parseExprList failed %w", err)
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
		return nil, errors.Errorf("parsePrefix %w", err)
	}
	p.expectKeyword("BETWEEN")
	high, err := p.parsePrefix()
	if err != nil {
		return nil, errors.Errorf("parsePrefix %w", err)
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
		return nil, errors.Errorf("nextToken error %w", err)
	}

	switch tok.Tok {
	case SQLKeyword:
		word := tok.Value.(*SQLWord)
		switch word.Keyword {
		case "TRUE", "FALSE", "NULL":
			p.prevToken()
			t, err := p.parseSQLValue()
			if err != nil {
				return nil, errors.Errorf("parseSQLValue failed %w", err)
			}
			return t, nil
		case "CASE":
			ast, err := p.parseCaseExpression()
			if err != nil {
				return nil, errors.Errorf("parseCaseExpression failed %w", err)
			}
			return ast, nil
		case "CAST":
			ast, err := p.parseCastExpression()
			if err != nil {
				return nil, errors.Errorf("parseCastExpression failed %w", err)
			}
			return ast, nil
		case "NOT":
			ts := &TokenSet{
				Tok:   SQLKeyword,
				Value: MakeKeyword("NOT", 0),
			}
			precedence := p.getPrecedence(ts)
			expr, err := p.parseSubexpr(precedence)
			if err != nil {
				return nil, errors.Errorf("parseSubexpr failed %w", err)
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
					return nil, errors.Errorf("nextToken failed %w", err)
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
					return nil, errors.Errorf("parseFuncton failed %w", err)
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
			return nil, errors.Errorf("parseSubexpr failed %w", err)
		}
		return &sqlast.SQLUnary{
			Operator: sqlast.Plus,
			Expr:     expr,
		}, nil
	case Minus:
		precedence := p.getPrecedence(tok)
		expr, err := p.parseSubexpr(precedence)
		if err != nil {
			return nil, errors.Errorf("parseSubexpr failed %w", err)
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
				return nil, errors.Errorf("parseQuery failed %w", err)
			}
			ast = &sqlast.SQLSubquery{
				Query: expr,
			}
		} else {
			expr, err := p.parseExpr()
			if err != nil {
				return nil, errors.Errorf("parseQuery failed %w", err)
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
		return nil, errors.Errorf("parseOptionalArgs %w", err)
	}
	var over *sqlast.SQLWindowSpec
	if ok, _ := p.parseKeyword("OVER"); ok {
		p.expectToken(LParen)

		var partitionBy []sqlast.ASTNode
		if ok, _ := p.parseKeywords("PARTITION", "BY"); ok {
			el, err := p.parseExprList()
			if err != nil {
				return nil, errors.Errorf("parseExprList failed %w", err)
			}
			partitionBy = el
		}

		var orderBy []*sqlast.SQLOrderByExpr
		if ok, _ := p.parseKeywords("ORDER", "BY"); ok {
			el, err := p.parseOrderByExprList()
			if err != nil {
				return nil, errors.Errorf("parseOrderByExprList %w", err)
			}
			orderBy = el
		}

		windowFrame, err := p.parseWindowFrame()
		if err != nil {
			return nil, errors.Errorf("parseWindowFrame failed %w", err)
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
			return nil, errors.Errorf("parseExprList %w", err)
		}
		p.expectToken(RParen)
		return as, nil
	}
}

func (p *Parser) parseOrderByExprList() ([]*sqlast.SQLOrderByExpr, error) {
	var exprList []*sqlast.SQLOrderByExpr

	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, errors.Errorf("parseExpr failed %w", err)
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
			p.nextToken()
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
		p.nextToken()

		if ok, _ := p.parseKeyword("BETWEEN"); ok {
			startBound, err := p.parseWindowFrameBound()
			if err != nil {
				return nil, errors.Errorf("parseWindowFrameBound %w", err)
			}
			p.expectKeyword("AND")
			endBound, err := p.parseWindowFrameBound()
			if err != nil {
				return nil, errors.Errorf("parseWindowFrameBound %w", err)
			}

			windowFrame = &sqlast.SQLWindowFrame{
				StartBound: startBound,
				EndBound:   endBound,
				Units:      units,
			}
		} else {
			startBound, err := p.parseWindowFrameBound()
			if err != nil {
				return nil, errors.Errorf("parseWindowFrameBound %w", err)
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
	if ok, _ := p.parseKeyword("UNBOUNDED"); !ok {
		i, err := p.parseLiteralInt()
		if err != nil {
			return nil, errors.Errorf("parseLiteralInt failed %w", err)
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
		return nil, errors.Errorf("parseListOfId %w", err)
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
		return nil, errors.Errorf("nextToken failed %w", err)
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

func (p *Parser) parseDataType() (sqlast.SQLType, error) {
	tok, err := p.nextToken()
	if err != nil {
		return nil, errors.Errorf("nextToken failed %w", err)
	}
	word, ok := tok.Value.(*SQLWord)
	if !ok {
		return nil, errors.Errorf("must be datetype name but %v", tok)
	}

	switch word.Keyword {
	case "BOOLEAN":
		return &sqlast.Boolean{}, nil
	case "FLOAT":
		p, err := p.parseOptionalPrecision()
		if err != nil {
			return nil, errors.Errorf("parsePrecision failed %w", err)
		}
		return &sqlast.Float{Size: p}, nil
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
			return nil, errors.Errorf("parsePrecision failed %w", err)
		}
		return &sqlast.VarcharType{Size: p}, nil
	case "CHAR", "CHARACTER":
		if ok, _ := p.parseKeyword("VARYING"); ok {
			p, err := p.parseOptionalPrecision()
			if err != nil {
				return nil, errors.Errorf("parsePrecision failed %w", err)
			}
			return &sqlast.VarcharType{Size: p}, nil
		}
		p, err := p.parseOptionalPrecision()
		if err != nil {
			return nil, errors.Errorf("parsePrecision failed %w", err)
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
		return &sqlast.Timestamp{}, nil
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
			return nil, errors.Errorf("parseOptionalPrecisionScale failed %w", err)
		}
		return &sqlast.Decimal{
			Precision: precision,
			Scale:     scale,
		}, nil

	default:
		p.prevToken()
		typeName, err := p.parseObjectName()
		if err != nil {
			return nil, errors.Errorf("parseObjectName %w", err)
		}
		return &sqlast.Custom{
			Ty: typeName,
		}, nil
	}
}

func (p *Parser) parseOptionalPrecision() (*uint8, error) {
	if ok, _ := p.consumeToken(LParen); ok {
		n, err := p.parseLiteralInt()
		if err != nil {
			return nil, errors.Errorf("parseLiteralInt failed %w", err)
		}
		p.expectToken(RParen)
		i := uint8(n)
		return &i, nil
	} else {
		return nil, nil
	}
}

func (p *Parser) parseOptionalPrecisionScale() (*uint8, *uint8, error) {
	if ok, _ := p.consumeToken(LParen); !ok {
		return nil, nil, nil
	}
	n, err := p.parseLiteralInt()
	if err != nil {
		return nil, nil, errors.Errorf("parseLiteralInt failed %w", err)
	}
	var scale *uint8
	if ok, _ := p.consumeToken(Comma); ok {
		s, err := p.parseLiteralInt()
		if err != nil {
			return nil, nil, errors.Errorf("parseLiteralInt failed %w", err)
		}
		us := uint8(s)
		scale = &us
	}
	p.expectToken(RParen)
	i := uint8(n)
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
		return 0, errors.Errorf("strconv.Atoi failed %w", err)
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
		expr, err := p.parseExpr()
		if err != nil {
			return nil, errors.Errorf("parseExpr %w", err)
		}
		operand = expr
		p.expectKeyword("WHEN")
	}

	var conditions []sqlast.ASTNode
	var results []sqlast.ASTNode

	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, errors.Errorf("parseExpr %w", err)
		}
		conditions = append(conditions, expr)
		p.expectKeyword("THEN")
		result, err := p.parseExpr()
		if err != nil {
			return nil, errors.Errorf("parseExpr %w", err)
		}
		results = append(results, result)
		if ok, _ := p.parseKeyword("WHEN"); !ok {
			break
		}
	}
	var elseResult sqlast.ASTNode

	if ok, _ := p.parseKeyword("ELSE"); ok {
		result, err := p.parseExpr()
		if err != nil {
			return nil, errors.Errorf("parseExpr %w", err)
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
	expr, err := p.parseExpr()
	if err != nil {
		return nil, errors.Errorf("parseExpr failed", err)
	}
	p.expectKeyword("AS")
	dataType, err := p.parseDataType()
	if err != nil {
		return nil, errors.Errorf("parseDataType")
	}
	p.expectToken(RParen)

	return &sqlast.SQLCast{
		Expr:     expr,
		DateType: dataType,
	}, nil
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

// TODO Must~
func (p *Parser) expectKeyword(expected string) {
	ok, err := p.parseKeyword(expected)
	if err != nil || !ok {
		log.Fatalf("should be expected keyword: %s err: %v", expected, err)
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
		p.nextToken()
		return true, nil
	}
	return false, nil
}

func containsStr(strmap map[string]struct{}, t string) bool {
	_, ok := strmap[t]
	return ok
}
