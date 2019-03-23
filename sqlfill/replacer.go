/*
Copyright (c) 2019 Simon Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/


package sqlfill

import "fmt"
import "strings"
import "github.com/xwb1989/sqlparser"

var dual *sqlparser.Select
func init() {
	stmt,err := sqlparser.Parse("select a")
	if err!=nil { panic(err) }
	dual = stmt.(*sqlparser.Select)
}

func subBuffer(tb *sqlparser.TrackedBuffer,f sqlparser.NodeFormatter) (ftb *sqlparser.TrackedBuffer) {
	ftb = sqlparser.NewTrackedBuffer(f)
	ftb.Buffer = tb.Buffer
	return
}

// func (buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode)
// func (node sqlparser.SQLNode) (kontinue bool, err error)

type tableContext struct {
	*processor
	// deprecated
	//Inner CatalogImpl
	aliased []*sqlparser.AliasedTableExpr
	count int
}

func (p *tableContext) examineTableExprs(node sqlparser.SQLNode) {
	switch v := node.(type) {
	case sqlparser.TableExprs:
		for _,vv := range v {
			p.examineTableExprs(vv)
		}
	case *sqlparser.AliasedTableExpr:
		p.aliased = append(p.aliased,v)
	case *sqlparser.ParenTableExpr:
		p.examineTableExprs(v.Exprs)
	case *sqlparser.JoinTableExpr:
		p.examineTableExprs(v.LeftExpr)
		p.examineTableExprs(v.RightExpr)
	}
}

func (p *tableContext) unify(s string) string {
	if p.isCaseSens { return strings.ToLower(s) }
	return s
}
func selExprs(s sqlparser.SelectStatement) (raw sqlparser.SelectExprs) {
	for {
		switch v := s.(type) {
		case *sqlparser.ParenSelect: s = v.Select
		case *sqlparser.Union: s = v.Left
		case *sqlparser.Select: return v.SelectExprs
		default: return
		}
	}
	return
}
func (p *tableContext) convertExpr(tn sqlparser.TableName, raw sqlparser.SelectExpr) (sqlparser.SelectExpr) {
	var e sqlparser.Expr
	switch v := raw.(type) {
	case *sqlparser.AliasedExpr:
		if !v.As.IsEmpty() {
			return &sqlparser.AliasedExpr{Expr:&sqlparser.ColName{Name:v.As,Qualifier:tn}}
		}
		e = v.Expr
	case sqlparser.Nextval:
		e = v.Expr
	default: return raw
	}
	switch v := e.(type) {
	case *sqlparser.ColName:
		return &sqlparser.AliasedExpr{Expr:&sqlparser.ColName{Name:v.Name,Qualifier:tn}}
	}
	switch v := raw.(type) {
	case *sqlparser.AliasedExpr:
		ci := sqlparser.NewColIdent(fmt.Sprintf("__i_col_%x",p.count))
		p.count++
		v.As = ci
		return &sqlparser.AliasedExpr{Expr:&sqlparser.ColName{Name:ci,Qualifier:tn}}
	case sqlparser.Nextval:
		panic("converting to name sqlparser.Nextval without inner name")
	}
	panic("unreachable")
}

func (p *tableContext) starExpr(tnP *sqlparser.TableName, expr *sqlparser.AliasedTableExpr) (exprs sqlparser.SelectExprs) {
	var tn sqlparser.TableName
	if tnP != nil { tn = *tnP }
	if tn.IsEmpty() { tn.Name = expr.As }
	switch v := expr.Expr.(type) {
	case sqlparser.TableName:
		if tn.IsEmpty() { tn = v }
		n := p.catalog.LookupTable(v)
		if len(n)==0 { return nil }
		exprs = make(sqlparser.SelectExprs,len(n))
		if len(p.aliased)==1 { tn = sqlparser.TableName {} }
		for i,nn := range n {
			ci := sqlparser.NewColIdent(nn)
			exprs[i] = &sqlparser.AliasedExpr{Expr:&sqlparser.ColName{Name:ci,Qualifier:tn}}
		}
	case *sqlparser.Subquery:
		raw := selExprs(v.Select)
		if len(raw)==0 { return nil }
		exprs = make(sqlparser.SelectExprs,len(raw))
		if len(p.aliased)==1 { tn = sqlparser.TableName {} }
		for i,re := range raw {
			exprs[i] = p.convertExpr(tn,re)
		}
	}
	
	return
}
func (p *tableContext) star(tn sqlparser.TableName) (expr sqlparser.SelectExprs) {
	var ate *sqlparser.AliasedTableExpr
	if tn.IsEmpty() {
		for _,aliased := range p.aliased {
			expr = append(expr,p.starExpr(nil,aliased)...)
		}
		return
	}
	if tn.Qualifier.IsEmpty() {
		for _,aliased := range p.aliased {
			if p.unify(aliased.As.String()) == p.unify(tn.Name.String()) {
				ate = aliased
				goto done
			}
			
			if atn,ok := aliased.Expr.(sqlparser.TableName); ok {
				if p.unify(atn.Name.String()) == p.unify(tn.Name.String()) {
					ate = aliased
					goto done
				}
			}
		}
	}
	
	done:
	if ate==nil { return nil }
	
	return p.starExpr(&tn,ate)
}

func (p *tableContext) expressions(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
	switch v := node.(type){
	case *sqlparser.StarExpr:
		n := p.star(v.TableName)
		if len(n)==0 {
			node.Format(subBuffer(buf,nil))
		} else {
			n.Format(buf)
		}
		return
	case *sqlparser.Subquery: node.Format(subBuffer(buf,nil))
	}
	node.Format(buf)
}


type processor struct {
	catalog LiteCatalog
	isCaseSens bool
}
func (p *processor) tc() *tableContext {
	t := new(tableContext)
	t.processor = p
	return t
}

func (p *processor) visit(node sqlparser.SQLNode) (kontinue bool, err error) {
	kontinue = true
	switch v := node.(type) {
	case *sqlparser.Select:
		kontinue = false
		err = sqlparser.Walk(p.visit,v.From)
		if err!=nil { return }
		err = p.ppSelect(v)
		if err!=nil { return }
		err = sqlparser.Walk(p.visit,
			v.Comments,
			v.SelectExprs,
			v.Where,
			v.GroupBy,
			v.Having,
			v.OrderBy,
			v.Limit,
		)
	}
	return
}

func (p *processor) ppSelect(s *sqlparser.Select) (err error) {
	defer func(){
		r := recover()
		if err==nil&&r!=nil {
			if ee,ok := r.(error); ok {
				err = ee
			} else {
				err = fmt.Errorf("%v",r)
			}
		}
	}()
	tc := p.tc()
	tc.examineTableExprs(s.From)
	tb := sqlparser.NewTrackedBuffer(tc.expressions)
	sp := *s
	sp.From = dual.From
	sp.Format(tb)
	stmt,err := sqlparser.Parse(tb.String())
	if err!=nil { return err }
	s2 := stmt.(*sqlparser.Select)
	s2.From = s.From
	*s = *s2
	
	return nil
}

func Substitute(catalog LiteCatalog,node sqlparser.SQLNode,caseSensitive bool) error {
	pro := &processor{catalog,caseSensitive}
	
	return sqlparser.Walk(pro.visit,node)
}
func SubstituteStr(catalog LiteCatalog,str string,caseSensitive bool) (string,error) {
	stmt,err := sqlparser.Parse(str)
	if err!=nil { return "",err }
	
	pro := &processor{catalog,caseSensitive}
	
	err = sqlparser.Walk(pro.visit,stmt)
	
	return sqlparser.String(stmt),err
}

