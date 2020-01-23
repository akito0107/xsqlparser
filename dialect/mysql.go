package dialect

type MySQLDialect struct {
	GenericSQLDialect
}

func (*MySQLDialect) IsDelimitedIdentifierStart(r rune) bool {
	return r == '"' || r == '`'
}

var _ Dialect = &MySQLDialect{}
