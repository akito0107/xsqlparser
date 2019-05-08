package astprinter

import (
	"flag"
	"io"
	"log"
	"os"

	"github.com/k0kubun/pp"

	"github.com/akito0107/xsqlparser"
	"github.com/akito0107/xsqlparser/dialect"
)

var f = flag.String("f", "stdin", "input sql file (default stdin)")

func main() {
	flag.Parse()

	var src io.Reader
	if *f == "stdin" {
		src = os.Stdin
	} else {
		file, err := os.Open(*f)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
		src = file
	}

	parser, _ := xsqlparser.NewParser(src, &dialect.GenericSQLDialect{})
	stmt, err := parser.ParseStatement()
	if err != nil {
		log.Fatal(err)
	}

	pp.Println(stmt)
}
