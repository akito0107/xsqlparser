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
	Dialect dialect.Dialect
	src     io.Reader
	tokens  []*TokenSet
	index   uint
}

func NewParser(src io.Reader, dialect dialect.Dialect) *Parser {
	return &Parser{Dialect: dialect, src: src}
}

func (p *Parser) ParseSQL() ([]sqlast.SQLStmt, error) {
	tokenizer := NewTokenizer(p.src, p.Dialect)
	set, err := tokenizer.Tokenize()
	if err != nil {
		return nil, errors.Errorf("tokenize err %w", err)
	}
	p.tokens = set
	p.index = 0

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
	case "CREATE":
	case "DELETE":
	case "INSERT":
	case "ALTER":
	case "COPY":
	default:
		return nil, errors.Errorf("unexpected keyword %s", word.Keyword)
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

	panic("unimplemented")
}

func (p *Parser) parseQueryBody() (sqlast.SQLSetExpr, error) {
	panic("unimplemented")
}

func (p *Parser) parseSelect() (*sqlast.SQLSelect, error) {
	distinct, err := p.parseKeyword("DISTINCT")
	if err != nil {
		return nil, errors.Errorf("parseKeyword failed %w", err)
	}
}

func (p *Parser) parseSelectList() ([]sqlast.SQLSelectItem, error) {
	var projections []sqlast.SQLSelectItem

	for {

	}

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

func (p *Parser) expectToken(expected Token) {
	ok, err := p.consumeToken(expected)
	if err != nil || !ok {
		log.Fatalf("should be %s token, err: %v", expected, err)
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
		if tok, _ := p.peekToken(); tok.Tok == Comma {
			p.nextToken()
		} else {
			break
		}
	}

	return exprList, nil
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

	}
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
		switch word.Value {
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

	}

	log.Fatalf("no infix parser for token %+v", tok)
	return nil, nil
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

func (p *Parser) parsePGCast(expr sqlast.ASTNode) (sqlast.ASTNode, error) {

}

func (p *Parser) getNextPrecedence() (uint, error) {
	tok, err := p.peekToken()
	if err != nil {
		return -1, errors.Errorf("peekToken failed %w", err)
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
		}

	}
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

func (p *Parser) parseCaseExpression() (sqlast.ASTNode, error) {
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
		if idx > uint(len(p.tokens)) {
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
