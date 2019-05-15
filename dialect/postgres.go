package dialect

type PostgresqlDialect struct {
}

func (*PostgresqlDialect) IsIdentifierStart(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_'
}

func (*PostgresqlDialect) IsIdentifierPart(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '$' || r == '_'
}

func (*PostgresqlDialect) IsDelimitedIdentifierStart(r rune) bool {
	return r == '"' || r == '`'
}

var _ Dialect = &PostgresqlDialect{}
