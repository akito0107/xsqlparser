package sqlast

import (
	"log"

	"github.com/akito0107/xsqlparser/sqltoken"
)

type CommentMap map[Node][]*CommentGroup

func (cmap CommentMap) addComment(n Node, c *CommentGroup) {
	list := cmap[n]

	if len(list) == 0 {
		list = []*CommentGroup{c}
	} else {
		list = append(list, c)
	}

	cmap[n] = list
}

func nodeList(file *File) []Node {
	var list []Node

	Inspect(file, func(node Node) bool {
		switch node.(type) {
		case nil:
			return false
		default:
			list = append(list, node)
			return true
		}
	})
	return list
}

type commentListReader struct {
	list     []*CommentGroup
	comment  *CommentGroup
	idx      int
	pos, end sqltoken.Pos
}

func (r *commentListReader) eol() bool {
	return len(r.list) <= r.idx
}

func (r *commentListReader) next() {
	if !r.eol() {
		r.comment = r.list[r.idx]
		r.pos = r.comment.Pos()
		r.end = r.comment.End()
		r.idx++
	}
}

type nodeStack []Node

func (s *nodeStack) push(n Node) {
	s.pop(n.Pos())
	*s = append(*s, n)
}

func (s *nodeStack) pop(pos sqltoken.Pos) (top Node) {
	i := len(*s)

	for i > 0 && sqltoken.ComparePos((*s)[i-1].End(), pos) != 1 {
		top = (*s)[i-1]
		i--
	}

	return top
}

func NewCommentMap(file *File) CommentMap {
	if len(file.Comments) == 0 {
		return nil
	}

	cmap := make(CommentMap)

	nodes := nodeList(file)
	nodes = append(nodes, nil)

	tmp := make([]*CommentGroup, len(file.Comments))
	copy(tmp, file.Comments)
	r := commentListReader{list: tmp}
	r.next()

	var (
		p     Node
		pend  sqltoken.Pos
		pg    Node
		pgend sqltoken.Pos
		stack nodeStack
	)

	for _, q := range nodes {
		var qpos sqltoken.Pos
		if q != nil {
			qpos = q.Pos()
		} else {
			const infinity = 1 << 30
			qpos = sqltoken.NewPos(infinity, infinity)
		}

		for sqltoken.ComparePos(qpos, r.end) != -1 {
			if top := stack.pop(r.comment.Pos()); top != nil {
				pg = top
				pgend = pg.End()
			}

			var assoc Node

			switch {
			case pg != nil && (pgend.Line == r.pos.Line || pgend.Line+1 == r.pos.Line && r.end.Line+1 < qpos.Line):
				assoc = pg
			case p != nil && (pend.Line == r.pos.Line || pend.Line+1 == r.pos.Line && r.end.Line+1 < qpos.Line || q == nil):
				assoc = p
			default:
				if q == nil {
					log.Fatal("internal error")
				}
				assoc = q
			}
			cmap.addComment(assoc, r.comment)

			if r.eol() {
				return cmap
			}
			r.next()
		}

		p = q
		pend = p.End()

		switch q.(type) {
		case Stmt, *QueryStmt, *InsertStmt:
			stack.push(q)
		}
	}

	return cmap
}
