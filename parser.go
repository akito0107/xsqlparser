package xsqlparser

import (
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"

	errors "golang.org/x/xerrors"

	"github.com/akito0107/xsqlparser/dialect"
	"github.com/akito0107/xsqlparser/sqlast"
	"github.com/akito0107/xsqlparser/sqltoken"
)

type Parser struct {
	tokens []*sqltoken.Token
	index  uint
}

func NewParser(src io.Reader, dialect dialect.Dialect) (*Parser, error) {
	tokenizer := sqltoken.NewTokenizer(src, dialect)
	set, err := tokenizer.Tokenize()
	if err != nil {
		return nil, errors.Errorf("tokenize err failed: %w", err)
	}

	return &Parser{tokens: set, index: 0}, nil
}

func (p *Parser) ParseSQL() ([]sqlast.Stmt, error) {
	var stmts []sqlast.Stmt
	var expectingDelimiter bool

	for {
		for {
			ok, _ := p.consumeToken(sqltoken.Semicolon)
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
			return nil, errors.Errorf("unexpected sqltoken %+v", t)
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

func (p *Parser) ParseStatement() (sqlast.Stmt, error) {
	tok, err := p.nextToken()
	if err != nil {
		return nil, err
	}
	word, ok := tok.Value.(*sqltoken.SQLWord)
	if !ok {
		return nil, errors.Errorf("a keyword at the beginning of statement %s", tok.Value)
	}

	switch word.Keyword {
	case "SELECT", "WITH":
		p.prevToken()
		return p.parseQuery()
	case "CREATE":
		p.prevToken()
		return p.parseCreate()
	case "DELETE":
		p.prevToken()
		return p.parseDelete()
	case "INSERT":
		return p.parseInsert()
	case "ALTER":
		return p.parseAlter()
	case "UPDATE":
		return p.parseUpdate()
	case "DROP":
		return p.parseDrop()
	case "EXPLAIN":
		stmt, err := p.ParseStatement()
		if err != nil {
			return nil, err
		}
		return &sqlast.ExplainStmt{Stmt: stmt}, nil
	default:
		return nil, errors.Errorf("unexpected (or unsupported) keyword %s", word.Keyword)
	}
}

func (p *Parser) ParseDataType() (sqlast.Type, error) {
	tok, err := p.nextToken()
	if err != nil {
		return nil, errors.Errorf("nextToken failed: %w", err)
	}
	word, ok := tok.Value.(*sqltoken.SQLWord)
	if !ok {
		return nil, errors.Errorf("must be datetype name but %v", tok)
	}

	switch word.Keyword {
	case "BOOLEAN":
		return &sqlast.Boolean{
			From: tok.From,
			To:   tok.To,
		}, nil
	case "FLOAT":
		size, r, err := p.parseOptionalPrecision()
		if err != nil {
			return nil, errors.Errorf("parsePrecision failed: %w", err)
		}
		return &sqlast.Float{Size: size, From: tok.From, To: tok.To, RParen: r}, nil
	case "REAL":
		return &sqlast.Real{From: tok.From, To: tok.To}, nil
	case "DOUBLE":
		p := p.expectKeyword("PRECISION")
		return &sqlast.Double{From: tok.From, To: p.To}, nil
	case "SMALLINT":
		return &sqlast.SmallInt{From: tok.From, To: tok.To}, nil
	case "INTEGER", "INT":
		return &sqlast.Int{From: tok.From, To: tok.To}, nil
	case "BIGINT":
		return &sqlast.BigInt{From: tok.From, To: tok.To}, nil
	case "VARCHAR":
		p, r, err := p.parseOptionalPrecision()
		if err != nil {
			return nil, errors.Errorf("parsePrecision failed: %w", err)

		}
		// FIXME Character
		return &sqlast.VarcharType{Size: p, RParen: r, Character: tok.From}, nil
	case "CHAR", "CHARACTER":
		if ok, v, _ := p.parseKeyword("VARYING"); ok {
			p, r, err := p.parseOptionalPrecision()
			if err != nil {
				return nil, errors.Errorf("parsePrecision failed: %w", err)
			}
			return &sqlast.VarcharType{Size: p, Character: tok.From, Varying: v.To, RParen: r}, nil
		}
		p, r, err := p.parseOptionalPrecision()
		if err != nil {
			return nil, errors.Errorf("parsePrecision failed: %w", err)
		}
		return &sqlast.CharType{Size: p, From: tok.From, To: tok.To, RParen: r}, nil
	case "UUID":
		return &sqlast.UUID{From: tok.From, To: tok.To}, nil
	case "DATE":
		return &sqlast.Date{}, nil
	case "TIMESTAMP":
		wok, _, _ := p.parseKeyword("WITH")
		ook, _, _ := p.parseKeyword("WITHOUT")
		if wok || ook {
			p.expectKeyword("TIME")
			p.expectKeyword("ZONE")
		}
		return &sqlast.Timestamp{
			Timestamp:    tok.From,
			WithTimeZone: wok,
		}, nil
	case "TIME":
		wok, _, _ := p.parseKeyword("WITH")
		ook, _, _ := p.parseKeyword("WITHOUT")
		if wok || ook {
			p.expectKeyword("TIME")
			p.expectKeyword("ZONE")
		}
		return &sqlast.Time{}, nil
	case "REGCLASS":
		return &sqlast.Regclass{}, nil
	case "TEXT":
		if ok, _ := p.consumeToken(sqltoken.LBracket); ok {
			p.expectToken(sqltoken.RBracket)
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

func (p *Parser) ParseExpr() (sqlast.Node, error) {
	return p.parseSubexpr(0)
}

func (p *Parser) parseQuery() (*sqlast.Query, error) {
	hasCTE, _, _ := p.parseKeyword("WITH")
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

	var orderBy []*sqlast.OrderByExpr
	if ok, _ := p.parseKeywords("ORDER", "BY"); ok {
		o, err := p.parseOrderByExprList()
		if err != nil {
			return nil, errors.Errorf("parseOrderByExprList failed: %w", err)
		}
		orderBy = o
	}

	var limit *sqlast.LimitExpr
	if ok, _, _ := p.parseKeyword("LIMIT"); ok {
		l, err := p.parseLimit()
		if err != nil {
			return nil, errors.Errorf("invalid limit expression: %w", err)
		}
		limit = l
	}

	return &sqlast.Query{
		CTEs:    ctes,
		Body:    body,
		Limit:   limit,
		OrderBy: orderBy,
	}, nil
}

func (p *Parser) parseQueryBody(precedence uint8) (sqlast.SQLSetExpr, error) {
	var expr sqlast.SQLSetExpr
	if ok, tok, _ := p.parseKeyword("SELECT"); ok {
		s, err := p.parseSelect()
		if err != nil {
			return nil, errors.Errorf("parseSelect failed: %w", err)
		}
		s.Select = tok.From
		expr = s
	} else if ok, _ := p.consumeToken(sqltoken.LParen); ok {
		subquery, err := p.parseQuery()
		if err != nil {
			return nil, errors.Errorf("parseQuery failed: %w", err)
		}
		p.expectToken(sqltoken.RParen)
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
		all, _, _ := p.parseKeyword("ALL")
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

func (p *Parser) parseSetOperator(token *sqltoken.Token) sqlast.SQLSetOperator {
	if token == nil {
		return nil
	}
	if token.Kind != sqltoken.SQLKeyword {
		return nil
	}
	word := token.Value.(*sqltoken.SQLWord)
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
	distinct, _, err := p.parseKeyword("DISTINCT")
	if err != nil {
		return nil, errors.Errorf("parseKeyword failed: %w", err)
	}
	projection, err := p.parseSelectList()
	if err != nil {
		return nil, errors.Errorf("parseSelectList failed: %w", err)
	}
	var tableRefs []sqlast.TableReference

	if ok, _, _ := p.parseKeyword("FROM"); ok {
		tableRefs, err = p.parseFromClause()
		if err != nil {
			return nil, errors.Errorf("parse from clause failed: %w", err)
		}
	}

	var selection sqlast.Node
	if ok, _, _ := p.parseKeyword("WHERE"); ok {
		s, err := p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}
		selection = s
	}

	var groupBy []sqlast.Node
	if ok, _ := p.parseKeywords("GROUP", "BY"); ok {
		g, err := p.parseExprList()
		if err != nil {
			return nil, errors.Errorf("parseExprList failed: %w", err)
		}
		groupBy = g
	}

	var having sqlast.Node
	if ok, _, _ := p.parseKeyword("HAVING"); ok {
		h, err := p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}
		having = h
	}

	return &sqlast.SQLSelect{
		Distinct:      distinct,
		Projection:    projection,
		WhereClause:   selection,
		FromClause:    tableRefs,
		GroupByClause: groupBy,
		HavingClause:  having,
	}, nil

}

func (p *Parser) parseSelectList() ([]sqlast.SQLSelectItem, error) {
	var projections []sqlast.SQLSelectItem

	for {
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}
		if w, ok := expr.(*sqlast.WildcardSelectItem); ok {
			projections = append(projections, w)
		} else if q, ok := expr.(*sqlast.QualifiedWildcard); ok {
			projections = append(projections, &sqlast.QualifiedWildcardSelectItem{
				Prefix: &sqlast.ObjectName{
					Idents: q.Idents,
				},
			})
		} else {
			alias := p.parseOptionalAlias(dialect.ReservedForColumnAlias)

			if alias != nil {
				projections = append(projections, &sqlast.AliasSelectItem{
					Expr:  expr,
					Alias: alias,
				})
			} else {
				projections = append(projections, &sqlast.UnnamedSelectItem{
					Node: expr,
				})
			}
		}

		if t, _ := p.peekToken(); t.Kind == sqltoken.Comma {
			p.mustNextToken()
		} else {
			break
		}
	}
	return projections, nil
}

func (p *Parser) parseCreate() (sqlast.Stmt, error) {
	ok, t, _ := p.parseKeyword("CREATE")
	if !ok {
		return nil, errors.Errorf("expect CREATE but %+v", t)
	}
	if ok, _, _ := p.parseKeyword("TABLE"); ok {
		return p.parseCreateTable(t)
	}

	mok, _, _ := p.parseKeyword("MATERIALIZED")
	vok, _, _ := p.parseKeyword("VIEW")

	if mok || vok {
		p.prevToken()
		return p.parseCreateView(t)
	}

	iok, _, _ := p.parseKeyword("INDEX")
	uiok, _ := p.parseKeywords("UNIQUE", "INDEX")

	if iok || uiok {
		return p.parseCreateIndex(uiok)
	}

	log.Fatal("TABLE or VIEW or UNIQUE INDEX or INDEX after create")

	return nil, nil
}

func (p *Parser) parseCreateTable(create *sqltoken.Token) (sqlast.Stmt, error) {
	name, err := p.parseObjectName()
	if err != nil {
		return nil, errors.Errorf("parseObjectName failed: %w", err)
	}

	elements, err := p.parseElements()
	if err != nil {
		return nil, errors.Errorf("parseElements failed: %w", err)
	}

	return &sqlast.CreateTableStmt{
		Create:   create.From,
		Name:     name,
		Elements: elements,
	}, nil
}

func (p *Parser) parseCreateView(create *sqltoken.Token) (sqlast.Stmt, error) {
	materialized, _, _ := p.parseKeyword("MATERIALIZED")
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

	return &sqlast.CreateViewStmt{
		Create:       create.From,
		Materialized: materialized,
		Name:         name,
		Query:        q,
	}, nil

}

func (p *Parser) parseCreateIndex(unique bool) (sqlast.Stmt, error) {
	var indexName *sqlast.Ident
	ok, _, _ := p.parseKeyword("ON")
	if !ok {
		if n, err := p.parseIdentifier(); err != nil {
			return nil, errors.Errorf("parseIdentifier failed: %w", err)
		} else {
			indexName = n
		}
		p.expectKeyword("ON")
	}

	tableName, err := p.parseObjectName()
	if err != nil {
		return nil, errors.Errorf("parseObjectName failed: %w", err)
	}
	var methodName *sqlast.Ident

	if ok, _, _ := p.parseKeyword("USING"); ok {
		m, err := p.parseIdentifier()
		if err != nil {
			return nil, errors.Errorf("parseIdentifier failed: %w", err)
		}
		methodName = m
	}

	var columns []*sqlast.Ident
	if ok, _ := p.consumeToken(sqltoken.LParen); ok {
		columns, err = p.parseColumnNames()
		if err != nil {
			return nil, errors.Errorf("parseColumnNames failed: %w", err)
		}
		p.expectToken(sqltoken.RParen)
	}

	var selection sqlast.Node
	if ok, _, _ := p.parseKeyword("WHERE"); ok {
		s, err := p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}
		selection = s
	}

	return &sqlast.CreateIndexStmt{
		IsUnique:    unique,
		IndexName:   indexName,
		TableName:   tableName,
		MethodName:  methodName,
		ColumnNames: columns,
		Selection:   selection,
	}, nil
}

func (p *Parser) parseElements() ([]sqlast.TableElement, error) {
	var elements []sqlast.TableElement
	if ok, _ := p.consumeToken(sqltoken.LParen); !ok {
		return elements, nil
	}

	for {
		tok, _ := p.nextToken()
		if tok == nil || tok.Kind != sqltoken.SQLKeyword {
			return nil, errors.Errorf("parse error after column def")
		}

		word := tok.Value.(*sqltoken.SQLWord)
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
		if t == nil || (t.Kind != sqltoken.Comma && t.Kind != sqltoken.RParen) {
			log.Fatalf("Expected ',' or ')' after column definition but %v", t)
		} else if t.Kind == sqltoken.RParen {
			break
		}
	}

	return elements, nil
}

func (p *Parser) parseColumnDef() (*sqlast.ColumnDef, error) {
	tok := p.mustNextToken()
	columnName := tok.Value.(*sqltoken.SQLWord)

	dataType, err := p.ParseDataType()
	if err != nil {
		return nil, errors.Errorf("ParseDataType failed: %w", err)
	}

	def, specs, err := p.parseColumnDefinition()
	if err != nil {
		return nil, errors.Errorf("parseColumnDefinition: %w", err)
	}

	return &sqlast.ColumnDef{
		Constraints: specs,
		Name: &sqlast.Ident{
			From:  tok.From,
			To:    tok.To,
			Value: columnName.String(),
		},
		DataType: dataType,
		Default:  def,
	}, nil
}

func (p *Parser) parseTableConstraints() (*sqlast.TableConstraint, error) {
	tok, _ := p.peekToken()
	if tok == nil || tok.Kind != sqltoken.SQLKeyword {
		return nil, errors.Errorf("parse error after column def")
	}

	word, ok := tok.Value.(*sqltoken.SQLWord)

	var constraintPos sqltoken.Pos
	var name *sqlast.Ident
	if ok && word.Keyword == "CONSTRAINT" {
		constraintPos = tok.From
		p.mustNextToken()
		i, err := p.parseIdentifier()
		if err != nil {
			return nil, errors.Errorf("parseIdentifier failed: %w", err)
		}
		name = i
	}

	tok, _ = p.peekToken()

	var spec sqlast.TableConstraintSpec
	word = tok.Value.(*sqltoken.SQLWord)
	switch word.Keyword {
	case "UNIQUE":
		p.mustNextToken()
		if _, _, err := p.parseKeyword("KEY"); err != nil {
			return nil, errors.Errorf("parseKeyword failed: %w", err)
		}
		p.expectToken(sqltoken.LParen)
		columns, err := p.parseColumnNames()
		if err != nil {
			return nil, errors.Errorf("parseColumnNames failed: %w", err)
		}
		r, _ := p.nextToken()
		if r.Kind != sqltoken.RParen {
			return nil, errors.Errorf("expected RParen but %+v", r)
		}
		spec = &sqlast.UniqueTableConstraint{
			Unique:  tok.From,
			RParen:  r.To,
			Columns: columns,
		}
	case "PRIMARY":
		p.mustNextToken()
		p.expectKeyword("KEY")
		p.expectToken(sqltoken.LParen)
		columns, err := p.parseColumnNames()
		if err != nil {
			return nil, errors.Errorf("parseColumnNames failed: %w", err)
		}
		r, _ := p.nextToken()
		if r.Kind != sqltoken.RParen {
			return nil, errors.Errorf("expected RParen but %+v", r)
		}
		spec = &sqlast.UniqueTableConstraint{
			Primary:   tok.From,
			RParen:    r.To,
			IsPrimary: true,
			Columns:   columns,
		}
	case "FOREIGN":
		p.mustNextToken()
		p.expectKeyword("KEY")
		p.expectToken(sqltoken.LParen)
		columns, err := p.parseColumnNames()
		if err != nil {
			return nil, errors.Errorf("parseColumnNames failed: %w", err)
		}
		p.expectToken(sqltoken.RParen)
		p.expectKeyword("REFERENCES")

		t, _ := p.nextToken()
		w := t.Value.(*sqltoken.SQLWord)
		p.expectToken(sqltoken.LParen)
		refcolumns, err := p.parseColumnNames()
		r, _ := p.nextToken()
		if r.Kind != sqltoken.RParen {
			return nil, errors.Errorf("expected RParen but %+v", r)
		}
		keys := &sqlast.ReferenceKeyExpr{
			TableName: &sqlast.Ident{
				From:  t.From,
				To:    t.To,
				Value: w.String(),
			},
			Columns: refcolumns,
			RParen:  r.To,
		}

		spec = &sqlast.ReferentialTableConstraint{
			Foreign: tok.From,
			Columns: columns,
			KeyExpr: keys,
		}
	case "CHECK":
		p.mustNextToken()
		p.expectToken(sqltoken.LParen)
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}
		r, _ := p.nextToken()
		if r.Kind != sqltoken.RParen {
			return nil, errors.Errorf("expected RParen but %+v", r)
		}
		spec = &sqlast.CheckTableConstraint{
			Expr:   expr,
			Check:  tok.From,
			RParen: r.To,
		}
	default:
		return nil, errors.Errorf("unknown table constraint: %v", word)
	}

	return &sqlast.TableConstraint{
		Name:       name,
		Spec:       spec,
		Constraint: constraintPos,
	}, nil
}

func (p *Parser) parseColumnDefinition() (sqlast.Node, []*sqlast.ColumnConstraint, error) {
	var specs []*sqlast.ColumnConstraint
	var def sqlast.Node

COLUMN_DEF_LOOP:
	for {
		t, _ := p.peekToken()
		if t == nil || t.Kind != sqltoken.SQLKeyword {
			break
		}

		word := t.Value.(*sqltoken.SQLWord)

		switch word.Keyword {
		case "DEFAULT":
			if ok, _, _ := p.parseKeyword("DEFAULT"); ok {
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
				return nil, nil, errors.Errorf("parseColumnConstraints failed: %w", err)
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
		word, ok := tok.Value.(*sqltoken.SQLWord)

		var name *sqlast.Ident
		if ok && word.Keyword == "CONSTRAINT" {
			p.mustNextToken()
			i, err := p.parseIdentifier()
			if err != nil {
				return nil, errors.Errorf("parseIdentifier failed: %w", err)
			}
			name = i
		}

		tok, _ = p.peekToken()
		if tok.Kind != sqltoken.SQLKeyword {
			break
		}

		var spec sqlast.ColumnConstraintSpec

		word = tok.Value.(*sqltoken.SQLWord)
		switch word.Keyword {
		case "NOT":
			p.mustNextToken()
			ok, ntok, _ := p.parseKeyword("NULL")
			if !ok {
				return nil, errors.Errorf("expected NULL but +%v", ntok)
			}
			spec = &sqlast.NotNullColumnSpec{
				Not:  tok.From,
				Null: ntok.To,
			}
		case "UNIQUE":
			p.mustNextToken()
			spec = &sqlast.UniqueColumnSpec{
				Unique: tok.From,
			}
		case "PRIMARY":
			p.mustNextToken()
			ok, ktok, _ := p.parseKeyword("KEY")
			if !ok {
				return nil, errors.Errorf("expected KEY but +%v", ktok)
			}
			spec = &sqlast.UniqueColumnSpec{IsPrimaryKey: true, Primary: tok.From, Key: ktok.To}
		case "REFERENCES":
			p.mustNextToken()
			tname, err := p.parseObjectName()
			if err != nil {
				return nil, errors.Errorf("parseObjectName failed: %w", err)
			}
			p.expectToken(sqltoken.LParen)
			columns, err := p.parseColumnNames()
			if err != nil {
				return nil, errors.Errorf("parseColumnNames failed: %w", err)
			}
			r, _ := p.nextToken()
			if r.Kind != sqltoken.RParen {
				return nil, errors.Errorf("expected RParen but %+v", r)
			}
			spec = &sqlast.ReferencesColumnSpec{
				TableName:  tname,
				Columns:    columns,
				References: tok.From,
				RParen:     r.To,
			}
		case "CHECK":
			p.mustNextToken()
			p.expectToken(sqltoken.LParen)
			expr, err := p.ParseExpr()
			if err != nil {
				return nil, errors.Errorf("ParseExpr failed: %w", err)
			}
			r, _ := p.nextToken()
			if r.Kind != sqltoken.RParen {
				return nil, errors.Errorf("expected RParen but %+v", r)
			}
			spec = &sqlast.CheckColumnSpec{
				Check:  tok.From,
				Expr:   expr,
				RParen: r.To,
			}
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

func (p *Parser) parseDelete() (sqlast.Stmt, error) {
	ok, d, _ := p.parseKeyword("DELETE")
	if !ok {
		return nil, errors.Errorf("expect DELETE but %+v", d)
	}

	p.expectKeyword("FROM")
	tableName, err := p.parseObjectName()
	if err != nil {
		return nil, errors.Errorf("parseObjectName failed: %w", err)
	}

	var selection sqlast.Node
	if ok, _, _ := p.parseKeyword("WHERE"); ok {
		selection, err = p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}
	}

	return &sqlast.DeleteStmt{
		Delete:    d.From,
		TableName: tableName,
		Selection: selection,
	}, nil
}

func (p *Parser) parseUpdate() (sqlast.Stmt, error) {
	tableName, err := p.parseObjectName()
	if err != nil {
		return nil, errors.Errorf("parseObjectName failed: %w", err)
	}
	p.expectKeyword("SET")

	assignments, err := p.parseAssignments()
	if err != nil {
		return nil, errors.Errorf("parseAssignments failed: %w", err)
	}

	var selection sqlast.Node
	if ok, _, _ := p.parseKeyword("WHERE"); ok {
		selection, err = p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}
	}

	return &sqlast.UpdateStmt{
		TableName:   tableName,
		Assignments: assignments,
		Selection:   selection,
	}, nil

}

func (p *Parser) parseAssignments() ([]*sqlast.Assignment, error) {
	var assignments []*sqlast.Assignment

	for {
		tok, _ := p.nextToken()
		if tok.Kind != sqltoken.SQLKeyword {
			return nil, errors.Errorf("should be sqlkeyword but %v", tok)
		}

		word := tok.Value.(*sqltoken.SQLWord)

		p.expectToken(sqltoken.Eq)

		val, err := p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}

		assignments = append(assignments, &sqlast.Assignment{
			ID:    sqlast.NewIdentFromWord(word),
			Value: val,
		})

		if ok, _ := p.consumeToken(sqltoken.Comma); !ok {
			break
		}
	}

	return assignments, nil
}

func (p *Parser) parseInsert() (sqlast.Stmt, error) {
	p.expectKeyword("INTO")
	tableName, err := p.parseObjectName()

	if err != nil {
		return nil, errors.Errorf("invalid table name: %w", err)
	}
	var columns []*sqlast.Ident

	if ok, _ := p.consumeToken(sqltoken.LParen); ok {
		columns, err = p.parseColumnNames()
		if err != nil {
			return nil, errors.Errorf("invalid column names: %w", err)
		}
		p.expectToken(sqltoken.RParen)
	}

	var insertSrc sqlast.InsertSource
	if ok, _, _ := p.parseKeyword("VALUES"); !ok {
		q, err := p.parseQuery()
		if err != nil {
			return nil, errors.Errorf("invalid select source: expected query: %w", err)
		}
		insertSrc = &sqlast.SubQuerySource{
			SubQuery: q,
		}
	} else {
		var constSrc sqlast.ConstructorSource
		for {
			p.expectToken(sqltoken.LParen)
			v, err := p.parseExprList()
			if err != nil {
				return nil, errors.Errorf("invalid insert value assign: %w", err)
			}
			constSrc.Rows = append(constSrc.Rows, &sqlast.RowValueExpr{
				Values: v,
			})
			p.expectToken(sqltoken.RParen)
			if ok, _ := p.consumeToken(sqltoken.Comma); !ok {
				break
			}
		}

		insertSrc = &constSrc
	}

	var assigns []*sqlast.Assignment
	if ok, _ := p.parseKeywords("ON", "DUPLICATE", "KEY", "UPDATE"); ok {
		assignments, err := p.parseAssignments()
		if err != nil {
			return nil, errors.Errorf("invalid DUPLICATE KEY UPDATE assignments: %w", err)
		}
		assigns = assignments
	}

	return &sqlast.InsertStmt{
		TableName:         tableName,
		Columns:           columns,
		Source:            insertSrc,
		UpdateAssignments: assigns,
	}, nil
}

func (p *Parser) parseAlter() (sqlast.Stmt, error) {
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

		return &sqlast.AlterTableStmt{
			TableName: tableName,
			Action: &sqlast.AddColumnTableAction{
				Column: columnDef,
			},
		}, nil
	}

	if ok, _, _ := p.parseKeyword("ADD"); ok {
		constraint, err := p.parseTableConstraints()
		if err != nil {
			return nil, errors.Errorf("parseTableConstraints failed: %w", err)
		}

		return &sqlast.AlterTableStmt{
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
		cascade, _, _ := p.parseKeyword("CASCADE")

		return &sqlast.AlterTableStmt{
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
		cascade, _, _ := p.parseKeyword("CASCADE")

		return &sqlast.AlterTableStmt{
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

		return &sqlast.AlterTableStmt{
			TableName: tableName,
			Action:    action,
		}, nil

	}

	t, _ := p.peekToken()
	return nil, errors.Errorf("unknown alter operation %v", t)
}

func (p *Parser) parseDrop() (sqlast.Stmt, error) {
	ok, _, _ := p.parseKeyword("TABLE")

	if !ok {
		p.expectKeyword("INDEX")
		idents, err := p.parseColumnNames()
		if err != nil {
			return nil, errors.Errorf("parseColumnNames of DROP INDEX failed: %w", err)
		}

		return &sqlast.DropIndexStmt{
			IndexNames: idents,
		}, nil
	}
	exists, _ := p.parseKeywords("IF", "EXISTS")
	tableName, err := p.parseObjectName()
	if err != nil {
		return nil, errors.Errorf("parseObjectName failed: %w", err)
	}
	cascade, _, _ := p.parseKeyword("CASCADE")

	return &sqlast.DropTableStmt{
		TableNames: []*sqlast.ObjectName{tableName},
		Cascade:    cascade,
		IfExists:   exists,
	}, nil
}

func (p *Parser) parseAlterColumn() (*sqlast.AlterColumnTableAction, error) {
	columnName, err := p.parseIdentifier()
	if err != nil {
		return nil, errors.Errorf("parseIdentifier failed: %w", err)
	}

	tok := p.mustNextToken()
	if tok.Kind != sqltoken.SQLKeyword {
		return nil, errors.Errorf("must be SQLKeyword but: %v", tok)
	}

	word := tok.Value.(*sqltoken.SQLWord)

	switch word.Keyword {
	case "SET":
		if ok, _, _ := p.parseKeyword("DEFAULT"); ok {
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
		if ok, _, _ := p.parseKeyword("DEFAULT"); ok {
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

func (p *Parser) parseDefaultExpr(precedence uint) (sqlast.Node, error) {
	expr, err := p.parsePrefix()
	if err != nil {
		return nil, errors.Errorf("parsePrefix failed: %w", err)
	}
	for {
		tok, _ := p.peekToken()
		if tok != nil && tok.Kind == sqltoken.SQLKeyword {
			w := tok.Value.(*sqltoken.SQLWord)
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

func (p *Parser) parseOptionalAlias(reservedKeywords map[string]struct{}) *sqlast.Ident {
	afterAs, _, _ := p.parseKeyword("AS")
	maybeAlias, _ := p.nextToken()

	if maybeAlias == nil {
		return nil
	}

	if maybeAlias.Kind == sqltoken.SQLKeyword {

		word := maybeAlias.Value.(*sqltoken.SQLWord)
		if afterAs || !containsStr(reservedKeywords, word.Keyword) {
			return &sqlast.Ident{
				Value: word.String(),
				From:  maybeAlias.From,
				To:    maybeAlias.To,
			}
		}
	}
	if afterAs {
		log.Fatalf("expected an identifier after AS")
	}
	p.prevToken()
	return nil
}

func (p *Parser) parseCTEList() ([]*sqlast.CTE, error) {
	var ctes []*sqlast.CTE

	for {
		alias, err := p.parseIdentifier()
		if err != nil {
			return nil, errors.Errorf("parseIdentifier failed: %w", err)
		}
		p.expectKeyword("AS")
		p.expectToken(sqltoken.LParen)
		q, err := p.parseQuery()
		if err != nil {
			return nil, errors.Errorf("parseQuery failed: %w", err)
		}
		ctes = append(ctes, &sqlast.CTE{
			Alias: alias,
			Query: q,
		})
		p.expectToken(sqltoken.RParen)
		if ok, _ := p.consumeToken(sqltoken.Comma); !ok {
			break
		}
	}
	return ctes, nil
}

func (p *Parser) parseFromClause() ([]sqlast.TableReference, error) {
	var res []sqlast.TableReference

	table, err := p.parseTableReference()
	if err != nil {
		return nil, errors.Errorf("parseTable failed: %w", err)
	}

	res = append(res, table)

	for {
		ok, _ := p.consumeToken(sqltoken.Comma)
		if !ok {
			break
		}
		table, err := p.parseTableReference()
		if err != nil {
			return nil, errors.Errorf("parseTable failed: %w", err)
		}
		res = append(res, table)
	}

	return res, nil
}

func (p *Parser) parseTableReference() (sqlast.TableReference, error) {
	leftElem, err := p.parseTableFactor()
	if err != nil {
		return nil, errors.Errorf("parse joined table left element failed: %w", err)
	}

	e := sqlast.TableReference(leftElem)

	for {
		right, err := p.parseTableReferenceRight()
		if err != nil {
			return nil, errors.Errorf("parse table reference right failed: %w", err)
		}

		if right == nil {
			break
		}

		switch rtp := right.(type) {
		case *sqlast.NaturalJoin:
			rtp.LeftElement = &sqlast.TableJoinElement{
				Ref: e,
			}
			e = rtp
		case *sqlast.CrossJoin:
			rtp.Reference = leftElem
			e = rtp
		case *sqlast.QualifiedJoin:
			rtp.LeftElement = &sqlast.TableJoinElement{
				Ref: e,
			}
			e = rtp
		default:
			return nil, errors.Errorf("unknown join")
		}
	}

	return e, nil
}

func (p *Parser) parseTableReferenceRight() (sqlast.TableReference, error) {
	if ok, _ := p.consumeToken(sqltoken.Comma); ok {
		p.prevToken()
		return nil, nil
	}

	tok, err := p.nextToken()
	if err != nil {
		return nil, nil
	}
	word, ok := tok.Value.(*sqltoken.SQLWord)
	if !ok {
		p.prevToken()
		return nil, nil
	}

	switch word.Keyword {
	case "NATURAL":
		tp, err := p.parseJoinType()
		if err != nil {
			return nil, errors.Errorf("parse natural join type failed: %w", err)
		}
		p.expectKeyword("JOIN")
		rightElem, err := p.parseTableReference()
		if err != nil {
			return nil, errors.Errorf("parse natural join right element failed: %w", err)
		}

		return &sqlast.NaturalJoin{
			Type: tp,
			RightElement: &sqlast.TableJoinElement{
				Ref: rightElem,
			},
		}, nil
	case "CROSS":
		p.expectKeyword("JOIN")
		rightElem, err := p.parseTableFactor()
		if err != nil {
			return nil, errors.Errorf("parse cross join right element failed: %w", err)
		}

		return &sqlast.CrossJoin{
			Factor: rightElem,
		}, nil
	case "INNER":
		p.expectKeyword("JOIN")
		ref, err := p.parseTableReference()
		if err != nil {
			return nil, errors.Errorf("parse inner join right elem filed: %w", err)
		}
		spec, err := p.parseJoinSpec()
		if err != nil {
			return nil, errors.Errorf("parse inner join spec filed: %w", err)
		}

		return &sqlast.QualifiedJoin{
			RightElement: &sqlast.TableJoinElement{
				Ref: ref,
			},
			Spec: spec,
			Type: &sqlast.JoinType{Condition: sqlast.INNER},
		}, nil
	case "LEFT", "RIGHT", "FULL", "JOIN":
		p.prevToken()
		tp, err := p.parseJoinType()
		if err != nil {
			return nil, errors.Errorf("parse qualified join type failed: %w", err)
		}
		p.expectKeyword("JOIN")
		ref, err := p.parseTableReference()
		if err != nil {
			return nil, errors.Errorf("parse qualified join right elem failed: %w", err)
		}

		spec, err := p.parseJoinSpec()
		if err != nil {
			return nil, errors.Errorf("parse qualified join spec failed: %w", err)
		}

		return &sqlast.QualifiedJoin{
			RightElement: &sqlast.TableJoinElement{
				Ref: ref,
			},
			Type: tp,
			Spec: spec,
		}, nil

	default:
		p.prevToken()
		return nil, nil
	}
}

func (p *Parser) parseJoinType() (*sqlast.JoinType, error) {
	tok, _ := p.nextToken()
	word, ok := tok.Value.(*sqltoken.SQLWord)
	if !ok {
		return nil, errors.Errorf("unknown join type %v", tok)
	}

	switch word.Keyword {
	case "INNER":
		return &sqlast.JoinType{
			Condition: sqlast.INNER,
			From:      tok.From,
			To:        tok.To,
		}, nil
	case "LEFT":
		outer, _, _ := p.parseKeyword("OUTER")
		if outer {
			return &sqlast.JoinType{
				Condition: sqlast.LEFTOUTER,
				From:      tok.From,
				To:        tok.To,
			}, nil
		}
		return &sqlast.JoinType{
			Condition: sqlast.LEFT,
			From:      tok.From,
			To:        tok.To,
		}, nil
	case "RIGHT":
		outer, _, _ := p.parseKeyword("OUTER")
		if outer {
			return &sqlast.JoinType{Condition: sqlast.RIGHTOUTER}, nil
		}
		return &sqlast.JoinType{Condition: sqlast.RIGHT}, nil
	case "FULL":
		outer, _, _ := p.parseKeyword("OUTER")
		if outer {
			return &sqlast.JoinType{Condition: sqlast.FULLOUTER}, nil
		}
		return &sqlast.JoinType{Condition: sqlast.FULL}, nil
	case "JOIN":
		p.prevToken()
		return &sqlast.JoinType{Condition: sqlast.IMPLICIT}, nil
	default:
		return nil, errors.Errorf("unknown join type: %v", word)
	}
}

func (p *Parser) parseJoinSpec() (sqlast.JoinSpec, error) {
	if ok, tok, _ := p.parseKeyword("ON"); ok {
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("parse join condition failed: %w", err)
		}
		return &sqlast.JoinCondition{
			SearchCondition: expr,
			On:              tok.From,
		}, nil
	}

	ok, _, _ := p.parseKeyword("USING")
	if !ok {
		tok, _ := p.nextToken()
		return nil, errors.Errorf("unknown join spec need USING or ON but: %v", tok)
	}

	p.expectToken(sqltoken.LParen)
	idents, err := p.parseListOfIds(sqltoken.Comma)
	if err != nil {
		return nil, errors.Errorf("parse named columns join list failed: %w", err)
	}
	p.expectToken(sqltoken.RParen)

	return &sqlast.NamedColumnsJoin{
		ColumnList: idents,
	}, nil
}

func (p *Parser) parseTableFactor() (sqlast.TableFactor, error) {
	isLateral, _, _ := p.parseKeyword("LATERAL")
	if ok, _ := p.consumeToken(sqltoken.LParen); ok {
		subquery, err := p.parseQuery()
		if err != nil {
			return nil, errors.Errorf("parseQuery failed: %w", err)
		}
		p.expectToken(sqltoken.RParen)
		alias := p.parseOptionalAlias(dialect.ReservedForTableAlias)
		return &sqlast.Derived{
			Lateral:  isLateral,
			SubQuery: subquery,
			Alias:    alias,
		}, nil
	} else if isLateral && !ok {
		t, _ := p.nextToken()
		return nil, errors.Errorf("after lateral expected %s but %+v", sqltoken.LParen, t)
	}

	name, err := p.parseObjectName()
	if err != nil {
		return nil, errors.Errorf("parseObjectName failed: %w", err)
	}
	var args []sqlast.Node
	if ok, _ := p.consumeToken(sqltoken.LParen); ok {
		a, err := p.parseOptionalArgs()
		if err != nil {
			return nil, errors.Errorf("parseOptionalArgs failed: %w", err)
		}
		args = a
	}
	alias := p.parseOptionalAlias(dialect.ReservedForTableAlias)

	var withHints []sqlast.Node
	if ok, _, _ := p.parseKeyword("WITH"); ok {
		if ok, _ := p.consumeToken(sqltoken.LParen); ok {
			h, err := p.parseExprList()
			if err != nil {
				return nil, errors.Errorf("parseExprList failed: %w", err)
			}
			withHints = h
			p.expectToken(sqltoken.RParen)
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

func (p *Parser) parseLimit() (*sqlast.LimitExpr, error) {
	if ok, _, _ := p.parseKeyword("ALL"); ok {
		return &sqlast.LimitExpr{All: true}, nil
	}

	i, tok, err := p.parseLiteralInt()
	if err != nil {
		return nil, errors.Errorf("invalid limit value: %w", err)
	}

	var offset *sqlast.LongValue
	if ok, tok, _ := p.parseKeyword("OFFSET"); ok {
		o, _, err := p.parseLiteralInt()
		if err != nil {
			return nil, errors.Errorf("invalid offset value: %w", err)
		}
		offset = &sqlast.LongValue{
			Long: int64(o),
			From: tok.From,
			To:   tok.To,
		}
	}

	return &sqlast.LimitExpr{
		LimitValue: &sqlast.LongValue{
			Long: int64(i),
			From: tok.From,
			To:   tok.To,
		},
		OffsetValue: offset,
	}, nil
}

func (p *Parser) parseIdentifier() (*sqlast.Ident, error) {
	tok, err := p.nextToken()
	if err != nil {
		return nil, errors.Errorf("nextToken failed: %w", err)
	}
	word, ok := tok.Value.(*sqltoken.SQLWord)
	if !ok {
		return nil, errors.Errorf("expected identifier but %+v", tok)
	}

	return &sqlast.Ident{
		From:  tok.From,
		To:    tok.To,
		Value: word.String(),
	}, nil
}

func (p *Parser) parseExprList() ([]sqlast.Node, error) {
	var exprList []sqlast.Node

	for {
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}
		exprList = append(exprList, expr)
		if tok, _ := p.peekToken(); tok != nil && tok.Kind == sqltoken.Comma {
			p.mustNextToken()
		} else {
			break
		}
	}

	return exprList, nil
}

func (p *Parser) parseColumnNames() ([]*sqlast.Ident, error) {
	return p.parseListOfIds(sqltoken.Comma)
}

func (p *Parser) parseSubexpr(precedence uint) (sqlast.Node, error) {
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

func (p *Parser) parseInfix(expr sqlast.Node, precedence uint) (sqlast.Node, error) {
	operator := sqlast.None
	tok, err := p.nextToken()
	if err != nil {
		return nil, errors.Errorf("nextToken failed: %w", err)
	}

	switch tok.Kind {
	case sqltoken.Eq:
		operator = sqlast.Eq
	case sqltoken.Neq:
		operator = sqlast.NotEq
	case sqltoken.Gt:
		operator = sqlast.Gt
	case sqltoken.GtEq:
		operator = sqlast.GtEq
	case sqltoken.Lt:
		operator = sqlast.Lt
	case sqltoken.LtEq:
		operator = sqlast.LtEq
	case sqltoken.Plus:
		operator = sqlast.Plus
	case sqltoken.Minus:
		operator = sqlast.Minus
	case sqltoken.Mult:
		operator = sqlast.Multiply
	case sqltoken.Mod:
		operator = sqlast.Modulus
	case sqltoken.Div:
		operator = sqlast.Divide
	case sqltoken.SQLKeyword:
		word := tok.Value.(*sqltoken.SQLWord)
		switch word.Keyword {
		case "AND":
			operator = sqlast.And
		case "OR":
			operator = sqlast.Or
		case "LIKE":
			operator = sqlast.Like
		case "NOT":
			ok, _, _ := p.parseKeyword("LIKE")
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

		return &sqlast.BinaryExpr{
			Left:  expr,
			Op:    &sqlast.Operator{Type: operator, From: tok.From, To: tok.To},
			Right: right,
		}, nil
	}

	if tok.Kind == sqltoken.SQLKeyword {
		word := tok.Value.(*sqltoken.SQLWord)

		switch word.Keyword {
		case "IS":
			if ok, _, _ := p.parseKeyword("NULL"); ok {
				return &sqlast.IsNull{
					X: expr,
				}, nil
			}
			if ok, _ := p.parseKeywords("NOT", "NULL"); ok {
				return &sqlast.IsNotNull{
					X: expr,
				}, nil
			}
			return nil, errors.Errorf("NULL or NOT NULL after IS")
		case "NOT", "IN", "BETWEEN":
			p.prevToken()
			negated, _, _ := p.parseKeyword("NOT")
			if ok, _, _ := p.parseKeyword("IN"); ok {
				return p.parseIn(expr, negated)
			}
			if ok, _, _ := p.parseKeyword("BETWEEN"); ok {
				return p.parseBetween(expr, negated)
			}
		}
	}

	if tok.Kind == sqltoken.DoubleColon {
		return p.parsePGCast(expr)
	}

	log.Fatalf("no infix parser for sqltoken %+v", tok)
	return nil, nil
}

// TODO position
func (p *Parser) parsePGCast(expr sqlast.Node) (sqlast.Node, error) {
	tp, err := p.ParseDataType()
	if err != nil {
		return nil, errors.Errorf("ParseDataType failed: %w", err)
	}
	return &sqlast.Cast{
		Expr:     expr,
		DateType: tp,
	}, nil
}

func (p *Parser) parseIn(expr sqlast.Node, negated bool) (sqlast.Node, error) {
	p.expectToken(sqltoken.LParen)
	sok, _, _ := p.parseKeyword("SELECT")
	wok, _, _ := p.parseKeyword("WITH")
	var inop sqlast.Node
	if sok || wok {
		p.prevToken()
		q, err := p.parseQuery()
		if err != nil {
			return nil, errors.Errorf("parseQuery failed: %w", err)
		}
		r, _ := p.nextToken()
		if r.Kind != sqltoken.RParen {
			return nil, errors.Errorf("expected RParen but %+v", r)
		}
		inop = &sqlast.InSubQuery{
			RParen:   r.To,
			Negated:  negated,
			Expr:     expr,
			SubQuery: q,
		}
	} else {
		list, err := p.parseExprList()
		if err != nil {
			return nil, errors.Errorf("parseExprList failed: %w", err)
		}
		r, _ := p.nextToken()
		if r.Kind != sqltoken.RParen {
			return nil, errors.Errorf("expected RParen but %+v", r)
		}
		inop = &sqlast.InList{
			RParen:  r.To,
			Expr:    expr,
			Negated: negated,
			List:    list,
		}
	}
	return inop, nil
}

func (p *Parser) parseBetween(expr sqlast.Node, negated bool) (sqlast.Node, error) {
	low, err := p.parsePrefix()
	if err != nil {
		return nil, errors.Errorf("parsePrefix: %w", err)
	}
	p.expectKeyword("AND")
	high, err := p.parsePrefix()
	if err != nil {
		return nil, errors.Errorf("parsePrefix: %w", err)
	}

	return &sqlast.Between{
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

func (p *Parser) getPrecedence(ts *sqltoken.Token) uint {
	switch ts.Kind {
	case sqltoken.SQLKeyword:
		word := ts.Value.(*sqltoken.SQLWord)
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
	case sqltoken.Eq, sqltoken.Lt, sqltoken.LtEq, sqltoken.Neq, sqltoken.Gt, sqltoken.GtEq:
		return 20
	case sqltoken.Plus, sqltoken.Minus:
		return 30
	case sqltoken.Mult, sqltoken.Div, sqltoken.Mod:
		return 40
	case sqltoken.DoubleColon:
		return 50
	default:
		return 0
	}
}

func (p *Parser) parsePrefix() (sqlast.Node, error) {
	tok, err := p.nextToken()
	if err != nil {
		return nil, errors.Errorf("nextToken error: %w", err)
	}

	switch tok.Kind {
	case sqltoken.SQLKeyword:
		word := tok.Value.(*sqltoken.SQLWord)
		switch word.Keyword {
		case "TRUE", "FALSE", "NULL":
			p.prevToken()
			t, err := p.parseSQLValue()
			if err != nil {
				return nil, errors.Errorf("parseSQLValue failed: %w", err)
			}
			return t, nil
		case "CASE":
			p.prevToken()
			ast, err := p.parseCaseExpression()
			if err != nil {
				return nil, errors.Errorf("parseCaseExpression failed: %w", err)
			}
			return ast, nil
		case "CAST":
			p.prevToken()
			ast, err := p.parseCastExpression()
			if err != nil {
				return nil, errors.Errorf("parseCastExpression failed: %w", err)
			}
			return ast, nil
		case "EXISTS":
			p.prevToken()
			ast, err := p.parseExistsExpression(nil)
			if err != nil {
				return nil, errors.Errorf("parseExistsExpression: %w", err)
			}
			return ast, nil
		case "NOT":
			if ok, _, _ := p.parseKeyword("EXISTS"); ok {
				p.prevToken()
				ast, err := p.parseExistsExpression(tok)
				if err != nil {
					return nil, errors.Errorf("parseExistsExpression: %w", err)
				}

				return ast, nil
			}

			ts := &sqltoken.Token{
				Kind:  sqltoken.SQLKeyword,
				Value: sqltoken.MakeKeyword("NOT", 0),
			}
			precedence := p.getPrecedence(ts)
			expr, err := p.parseSubexpr(precedence)
			if err != nil {
				return nil, errors.Errorf("parseSubexpr failed: %w", err)
			}
			return &sqlast.UnaryExpr{
				From: tok.From,
				Op:   &sqlast.Operator{Type: sqlast.Not},
				Expr: expr,
			}, nil
		default:
			t, _ := p.peekToken()
			if t == nil || (t.Kind != sqltoken.LParen && t.Kind != sqltoken.Period) {
				return &sqlast.Ident{Value: word.String(),
					From: tok.From,
					To:   tok.To,
				}, nil
			}
			idParts := []*sqlast.Ident{
				{Value: word.String(), From: tok.From, To: tok.To},
			}
			endWithWildcard := false

			for {
				if ok, _ := p.consumeToken(sqltoken.Period); !ok {
					break
				}
				n, err := p.nextToken()
				if err != nil {
					return nil, errors.Errorf("nextToken failed: %w", err)
				}

				if n.Kind == sqltoken.SQLKeyword {
					w := n.Value.(*sqltoken.SQLWord)
					idParts = append(idParts, &sqlast.Ident{Value: w.String(),
						From: n.From,
						To:   n.To,
					})
					continue
				}
				if n.Kind == sqltoken.Mult {
					endWithWildcard = true
					break
				}

				return nil, errors.Errorf("an identifier or '*' after '.'")
			}

			if endWithWildcard {
				return &sqlast.QualifiedWildcard{
					Idents: idParts,
				}, nil
			}

			if ok, _ := p.consumeToken(sqltoken.LParen); ok {
				p.prevToken()
				name := &sqlast.ObjectName{
					Idents: idParts,
				}
				f, err := p.parseFunction(name)
				if err != nil {
					return nil, errors.Errorf("parseFunction failed: %w", err)
				}
				return f, nil
			}

			return &sqlast.CompoundIdent{
				Idents: idParts,
			}, nil
		}
	case sqltoken.Mult:
		return &sqlast.Wildcard{
			Wildcard: tok.From,
		}, nil
	case sqltoken.Plus:
		precedence := p.getPrecedence(tok)
		expr, err := p.parseSubexpr(precedence)
		if err != nil {
			return nil, errors.Errorf("parseSubexpr failed: %w", err)
		}
		return &sqlast.UnaryExpr{
			From: tok.From,
			Op:   &sqlast.Operator{Type: sqlast.Plus, From: tok.From, To: tok.To},
			Expr: expr,
		}, nil
	case sqltoken.Minus:
		precedence := p.getPrecedence(tok)
		expr, err := p.parseSubexpr(precedence)
		if err != nil {
			return nil, errors.Errorf("parseSubexpr failed: %w", err)
		}
		return &sqlast.UnaryExpr{
			From: tok.From,
			Op:   &sqlast.Operator{Type: sqlast.Minus, From: tok.From, To: tok.To},
			Expr: expr,
		}, nil
	case sqltoken.Number, sqltoken.SingleQuotedString, sqltoken.NationalStringLiteral:
		p.prevToken()
		v, err := p.parseSQLValue()
		if err != nil {
			return nil, errors.Errorf("parseSQLValue failed", err)
		}
		return v, nil
	case sqltoken.LParen:
		sok, _, _ := p.parseKeyword("SELECT")
		wok, _, _ := p.parseKeyword("WITH")

		var ast sqlast.Node

		if sok || wok {
			p.prevToken()
			expr, err := p.parseQuery()
			if err != nil {
				return nil, errors.Errorf("parseQuery failed: %w", err)
			}
			r, _ := p.nextToken()
			if r.Kind != sqltoken.RParen {
				return nil, errors.Errorf("expected RParen but %+v", r)
			}
			ast = &sqlast.SubQuery{
				LParen: tok.From,
				RParen: r.To,
				Query:  expr,
			}
		} else {
			expr, err := p.ParseExpr()
			if err != nil {
				return nil, errors.Errorf("parseQuery failed: %w", err)
			}
			r, _ := p.nextToken()
			if r.Kind != sqltoken.RParen {
				return nil, errors.Errorf("expected RParen but %+v", r)
			}
			ast = &sqlast.Nested{
				LParen: tok.From,
				RParen: r.To,
				AST:    expr,
			}
		}
		return ast, nil
	}
	log.Fatal("prefix parser expected a keyword but hit EOF")
	return nil, nil
}

func (p *Parser) parseFunction(name *sqlast.ObjectName) (sqlast.Node, error) {
	p.expectToken(sqltoken.LParen)
	args, err := p.parseOptionalArgs()
	if err != nil {
		return nil, errors.Errorf("parseOptionalArgs failed: %w", err)
	}

	r, _ := p.nextToken()
	if r.Kind != sqltoken.RParen {
		return nil, errors.Errorf("expected RParen but %+v", r)
	}

	var over *sqlast.WindowSpec
	if ok, _, _ := p.parseKeyword("OVER"); ok {
		p.expectToken(sqltoken.LParen)

		var partitionBy []sqlast.Node
		var partition sqltoken.Pos

		ok, ptok, _ := p.parseKeyword("PARTITION")
		if ok {
			p.expectKeyword("BY")

			el, err := p.parseExprList()
			if err != nil {
				return nil, errors.Errorf("parseExprList failed: %w", err)
			}
			partitionBy = el
			partition = ptok.From
		}

		var orderBy []*sqlast.OrderByExpr
		var order sqltoken.Pos
		ok, otok, _ := p.parseKeyword("PARTITION")
		if ok {
			p.expectKeyword("BY")
			el, err := p.parseOrderByExprList()
			if err != nil {
				return nil, errors.Errorf("parseOrderByExprList failed: %w", err)
			}
			orderBy = el
			order = otok.From
		}

		windowFrame, err := p.parseWindowFrame()
		if err != nil {
			return nil, errors.Errorf("parseWindowFrame failed: %w", err)
		}

		over = &sqlast.WindowSpec{
			PartitionBy:  partitionBy,
			OrderBy:      orderBy,
			WindowsFrame: windowFrame,
			Partition:    partition,
			Order:        order,
		}
	}

	return &sqlast.Function{
		Name:       name,
		Args:       args,
		Over:       over,
		ArgsRParen: r.To,
	}, nil
}

func (p *Parser) parseOptionalArgs() ([]sqlast.Node, error) {
	if ok, _ := p.consumeToken(sqltoken.RParen); ok {
		p.prevToken()
		return nil, nil
	} else {
		as, err := p.parseExprList()
		if err != nil {
			return nil, errors.Errorf("parseExprList failed: %w", err)
		}
		return as, nil
	}
}

func (p *Parser) parseOrderByExprList() ([]*sqlast.OrderByExpr, error) {
	var exprList []*sqlast.OrderByExpr

	for {
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}
		var asc *bool

		if ok, _, _ := p.parseKeyword("ASC"); ok {
			b := true
			asc = &b
		} else if ok, _, _ := p.parseKeyword("DESC"); ok {
			b := false
			asc = &b
		}

		exprList = append(exprList, &sqlast.OrderByExpr{
			Expr: expr,
			ASC:  asc,
		})

		if t, _ := p.peekToken(); t != nil && t.Kind == sqltoken.Comma {
			p.mustNextToken()
		} else {
			break
		}
	}

	return exprList, nil
}

func (p *Parser) parseWindowFrame() (*sqlast.WindowFrame, error) {
	var windowFrame *sqlast.WindowFrame
	t, _ := p.peekToken()
	if t.Kind == sqltoken.SQLKeyword {
		w := t.Value.(*sqltoken.SQLWord)
		var u sqlast.WindowFrameUnit
		// FIXME
		units := u.FromStr(w.Keyword)
		p.mustNextToken()

		if ok, _, _ := p.parseKeyword("BETWEEN"); ok {
			startBound, err := p.parseWindowFrameBound()
			if err != nil {
				return nil, errors.Errorf("parseWindowFrameBound: %w", err)
			}
			p.expectKeyword("AND")
			endBound, err := p.parseWindowFrameBound()
			if err != nil {
				return nil, errors.Errorf("parseWindowFrameBound: %w", err)
			}

			windowFrame = &sqlast.WindowFrame{
				StartBound: startBound,
				EndBound:   endBound,
				Units:      units,
			}
		} else {
			startBound, err := p.parseWindowFrameBound()
			if err != nil {
				return nil, errors.Errorf("parseWindowFrameBound: %w", err)
			}
			windowFrame = &sqlast.WindowFrame{
				StartBound: startBound,
				Units:      units,
			}
		}
	}

	p.expectToken(sqltoken.RParen)
	return windowFrame, nil
}

func (p *Parser) parseWindowFrameBound() (sqlast.SQLWindowFrameBound, error) {
	if ok, _ := p.parseKeywords("CURRENT", "ROW"); ok {
		return &sqlast.CurrentRow{}, nil
	}

	var rows *uint64
	if ok, _, _ := p.parseKeyword("UNBOUNDED"); ok {
		if ok, _, _ := p.parseKeyword("PRECEDING"); ok {
			return &sqlast.UnboundedPreceding{}, nil
		}
		if ok, _, _ := p.parseKeyword("FOLLOWING"); ok {
			return &sqlast.UnboundedFollowing{}, nil
		}
	} else {
		i, _, err := p.parseLiteralInt()
		if err != nil {
			return nil, errors.Errorf("parseLiteralInt failed: %w", err)
		}
		if i < 0 {
			return nil, errors.Errorf("the number of rows must ne non-negative, got %d", i)
		}
		ui := uint64(i)
		rows = &ui
	}

	if ok, _, _ := p.parseKeyword("PRECEDING"); ok {
		return &sqlast.Preceding{Bound: rows}, nil
	}
	if ok, _, _ := p.parseKeyword("FOLLOWING"); ok {
		return &sqlast.Following{Bound: rows}, nil
	}
	log.Fatal("expected PRECEDING or FOLLOWING")
	return nil, nil
}

func (p *Parser) parseObjectName() (*sqlast.ObjectName, error) {
	idents, err := p.parseListOfIds(sqltoken.Period)
	if err != nil {
		return nil, errors.Errorf("parseListOfId: %w", err)
	}
	return &sqlast.ObjectName{
		Idents: idents,
	}, nil
}

func (p *Parser) parseSQLValue() (sqlast.Node, error) {
	return p.parseValue()
}

func (p *Parser) parseValue() (sqlast.Node, error) {
	tok, err := p.nextToken()
	if err != nil {
		return nil, errors.Errorf("nextToken failed: %w", err)
	}

	switch tok.Kind {
	case sqltoken.SQLKeyword:
		word := tok.Value.(*sqltoken.SQLWord)

		switch word.Keyword {
		case "TRUE":
			return &sqlast.BooleanValue{
				From:    tok.From,
				To:      tok.To,
				Boolean: true,
			}, nil
		case "FALSE":
			return &sqlast.BooleanValue{
				From:    tok.From,
				To:      tok.To,
				Boolean: false,
			}, nil
		case "NULL":
			return &sqlast.NullValue{
				From: tok.From,
				To:   tok.To,
			}, nil
		default:
			return nil, errors.Errorf("unexpected sqltoken %v", word)
		}
	case sqltoken.Number:
		num := tok.Value.(string)
		if strings.Contains(num, ".") {
			f, err := strconv.ParseFloat(num, 64)
			if err != nil {
				return nil, errors.Errorf("parseFloat failed %s", num)
			}
			return &sqlast.DoubleValue{
				From:   tok.From,
				To:     tok.To,
				Double: f,
			}, nil
		} else {
			i, _ := strconv.Atoi(num)
			return &sqlast.LongValue{
				Long: int64(i),
				From: tok.From,
				To:   tok.To,
			}, nil
		}
	case sqltoken.SingleQuotedString:
		str := tok.Value.(string)
		return &sqlast.SingleQuotedString{
			From:   tok.From,
			To:     tok.To,
			String: str,
		}, nil
	case sqltoken.NationalStringLiteral:
		str := tok.Value.(string)
		return &sqlast.NationalStringLiteral{
			String: str,
			From:   tok.From,
			To:     tok.To,
		}, nil
	default:
		return nil, errors.Errorf("unexpected sqltoken %v", tok)
	}

}

func (p *Parser) parseOptionalPrecision() (*uint, sqltoken.Pos, error) {
	if ok, _ := p.consumeToken(sqltoken.LParen); ok {
		n, _, err := p.parseLiteralInt()
		if err != nil {
			return nil, sqltoken.Pos{}, errors.Errorf("parseLiteralInt failed: %w", err)
		}
		tok, _ := p.nextToken()

		if tok.Kind != sqltoken.RParen {
			return nil, sqltoken.Pos{}, errors.Errorf("expected RParen but %s", tok)
		}
		i := uint(n)
		return &i, tok.To, nil
	} else {
		return nil, sqltoken.Pos{}, nil
	}
}

func (p *Parser) parseOptionalPrecisionScale() (*uint, *uint, error) {
	if ok, _ := p.consumeToken(sqltoken.LParen); !ok {
		return nil, nil, nil
	}
	n, _, err := p.parseLiteralInt()
	if err != nil {
		return nil, nil, errors.Errorf("parseLiteralInt failed: %w", err)
	}
	var scale *uint
	if ok, _ := p.consumeToken(sqltoken.Comma); ok {
		s, _, err := p.parseLiteralInt()
		if err != nil {
			return nil, nil, errors.Errorf("parseLiteralInt failed: %w", err)
		}
		us := uint(s)
		scale = &us
	}
	p.expectToken(sqltoken.RParen)
	i := uint(n)
	return &i, scale, nil
}

func (p *Parser) parseLiteralInt() (int, *sqltoken.Token, error) {
	tok, _ := p.nextToken()
	if tok.Kind != sqltoken.Number {
		return 0, nil, errors.Errorf("expect literal int but %v", tok.Kind)
	}
	istr := tok.Value.(string)
	i, err := strconv.Atoi(istr)
	if err != nil {
		return 0, nil, errors.Errorf("strconv.Atoi failed: %w", err)
	}

	return i, tok, nil
}

func (p *Parser) parseListOfIds(separator sqltoken.Kind) ([]*sqlast.Ident, error) {
	var idents []*sqlast.Ident
	expectIdentifier := true

	for {
		tok, _ := p.nextToken()
		if tok == nil {
			break
		}
		if tok.Kind == sqltoken.SQLKeyword && expectIdentifier {
			expectIdentifier = false
			word := tok.Value.(*sqltoken.SQLWord)
			idents = append(idents, &sqlast.Ident{
				Value: word.String(),
				From:  tok.From,
				To:    tok.To,
			})
			continue
		} else if tok.Kind == separator && !expectIdentifier {
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

func (p *Parser) parseCaseExpression() (sqlast.Node, error) {
	ok, tok, _ := p.parseKeyword("CASE")
	if !ok {
		return nil, errors.Errorf("expected CASE keyword but %s", tok)
	}

	var operand sqlast.Node
	if ok, _, _ := p.parseKeyword("WHEN"); !ok {
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}
		operand = expr
		p.expectKeyword("WHEN")
	}

	var conditions []sqlast.Node
	var results []sqlast.Node

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
		if ok, _, _ := p.parseKeyword("WHEN"); !ok {
			break
		}
	}
	var elseResult sqlast.Node

	if ok, _, _ := p.parseKeyword("ELSE"); ok {
		result, err := p.ParseExpr()
		if err != nil {
			return nil, errors.Errorf("ParseExpr failed: %w", err)
		}
		elseResult = result
	}
	ok, etok, _ := p.parseKeyword("END")
	if !ok {
		return nil, errors.Errorf("expect END keyword but %s", etok)
	}

	return &sqlast.CaseExpr{
		Case:       tok.From,
		CaseEnd:    etok.To,
		Operand:    operand,
		Conditions: conditions,
		Results:    results,
		ElseResult: elseResult,
	}, nil

}

func (p *Parser) parseCastExpression() (sqlast.Node, error) {
	ok, tok, _ := p.parseKeyword("CAST")
	if !ok {
		return nil, errors.Errorf("expected CAST but %+v", tok)
	}
	p.expectToken(sqltoken.LParen)
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, errors.Errorf("ParseExpr failed: %w", err)
	}
	p.expectKeyword("AS")
	dataType, err := p.ParseDataType()
	if err != nil {
		return nil, errors.Errorf("ParseDataType")
	}
	r, _ := p.nextToken()
	if r.Kind != sqltoken.RParen {
		return nil, errors.Errorf("expect RParen but %+v", r)
	}

	return &sqlast.Cast{
		Expr:     expr,
		DateType: dataType,
		Cast:     tok.From,
		RParen:   r.To,
	}, nil
}

func (p *Parser) parseExistsExpression(negatedTok *sqltoken.Token) (sqlast.Node, error) {
	ok, tok, _ := p.parseKeyword("EXISTS")
	if !ok {
		return nil, errors.Errorf("expect EXISTS but %+v", tok)
	}

	p.expectToken(sqltoken.LParen)
	expr, err := p.parseQuery()
	if err != nil {
		return nil, errors.Errorf("parseQuery failed: %w", err)
	}

	r, _ := p.nextToken()
	if r.Kind != sqltoken.RParen {
		return nil, errors.Errorf("expect RParen but %+v", r)
	}

	if negatedTok != nil {
		return &sqlast.Exists{
			Negated: true,
			Query:   expr,
			Not:     negatedTok.From,
			Exists:  tok.From,
			RParen:  r.To,
		}, nil
	}

	return &sqlast.Exists{
		Query:  expr,
		Exists: tok.From,
		RParen: r.To,
	}, nil
}

func (p *Parser) expectKeyword(expected string) *sqltoken.Token {
	ok, tok, err := p.parseKeyword(expected)
	if err != nil || !ok {
		for i := 0; i < int(p.index); i++ {
			fmt.Printf("%v", p.tokens[i].Value)
		}
		fmt.Println()
		log.Fatalf("should be expected keyword: %s err: %v", expected, err)
	}

	return tok
}

func (p *Parser) expectToken(expected sqltoken.Kind) {
	ok, err := p.consumeToken(expected)
	if err != nil || !ok {
		tok, _ := p.peekToken()

		for i := 0; i < int(p.index); i++ {
			fmt.Printf("%v", p.tokens[i].Value)
		}
		fmt.Println()
		log.Fatalf("should be %s sqltoken, but %+v,  err: %+v", expected, tok, err)
	}
}

func (p *Parser) consumeToken(expected sqltoken.Kind) (bool, error) {
	tok, err := p.peekToken()
	if err != nil {
		return false, err
	}

	if tok.Kind == expected {
		if _, err := p.nextToken(); err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}

func (p *Parser) mustNextToken() *sqltoken.Token {
	tok, err := p.nextToken()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	return tok
}

func (p *Parser) nextToken() (*sqltoken.Token, error) {
	for {
		tok, err := p.nextTokenNoSkip()
		if err != nil {
			return nil, err
		}
		if tok.Kind == sqltoken.Whitespace || tok.Kind == sqltoken.Comment {
			continue
		}
		return tok, nil
	}
}

var TokenAlreadyConsumed = errors.New("tokens are already consumed")

func (p *Parser) nextTokenNoSkip() (*sqltoken.Token, error) {
	if p.index < uint(len(p.tokens)) {
		p.index += 1
		return p.tokens[p.index-1], nil
	}
	return nil, TokenAlreadyConsumed
}

func (p *Parser) prevToken() *sqltoken.Token {
	for {
		tok := p.prevTokenNoSkip()
		if tok.Kind == sqltoken.Whitespace || tok.Kind == sqltoken.Comment {
			continue
		}
		return tok
	}
}

func (p *Parser) prevTokenNoSkip() *sqltoken.Token {
	if p.index > 0 {
		p.index -= 1
		return p.tokens[p.index]
	}
	return nil
}

func (p *Parser) peekToken() (*sqltoken.Token, error) {
	u, err := p.tilNonWhitespace()
	if err != nil {
		return nil, err
	}
	return p.tokens[u], nil
}

func (p *Parser) tilNonWhitespace() (uint, error) {
	idx := p.index
	for {
		if idx >= uint(len(p.tokens)) {
			return 0, TokenAlreadyConsumed
		}
		tok := p.tokens[idx]
		if tok.Kind == sqltoken.Whitespace || tok.Kind == sqltoken.Comment {
			idx += 1
			continue
		}
		return idx, nil
	}
}

// Deprecated
func (p *Parser) parseKeywords(keywords ...string) (bool, error) {
	idx := p.index

	for _, k := range keywords {
		if ok, _, _ := p.parseKeyword(k); !ok {
			p.index = idx
			return false, nil
		}
	}

	return true, nil
}

func (p *Parser) parseKeyword(expected string) (bool, *sqltoken.Token, error) {
	tok, err := p.peekToken()
	if err != nil {
		return false, nil, errors.Errorf("parseKeyword %s failed: %w", expected, err)
	}

	word, ok := tok.Value.(*sqltoken.SQLWord)
	if !ok {
		return false, tok, nil
	}

	if strings.EqualFold(word.Value, expected) {
		p.mustNextToken()
		return true, tok, nil
	}
	return false, tok, nil
}

func (p *Parser) Debug() {
	for i := 0; i < int(p.index); i++ {
		fmt.Printf("%v", p.tokens[i].Value)
	}
	fmt.Println()
}

func containsStr(strmap map[string]struct{}, t string) bool {
	_, ok := strmap[t]
	return ok
}
