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


package planbuilder

import (
	"github.com/xwb1989/sqlparser"
	"github.com/mad-day/db-utils/poet/engine"
)
import "fmt"


func Convert(q sqlparser.Statement,s engine.PSchema) (engine.Primitive,error) {
	
	return nil,fmt.Errorf("not supported %T",q)
}

func convertTe(q sqlparser.TableExpr,s engine.PSchema) (pn preColumns,err error) {
	
	switch v := q.(type) {
	case *sqlparser.AliasedTableExpr:
		pn,err = convertSte(v.Expr,s)
		if err!=nil { return }
		if !v.As.IsEmpty() {
			n := v.As.String()
			cols := pn.iCols()
			for i := range cols {
				cols[i].name[0] = ""
				cols[i].name[1] = n
			}
		}
		if te,ok := pn.(*tablesExpr); ok {
			te.sql = &sqlparser.AliasedTableExpr{Expr:te.sql.(sqlparser.SimpleTableExpr),As:v.As}
		}
	case *sqlparser.ParenTableExpr:
		pns := make([]preNode,len(v.Exprs))
		for i,expr := range v.Exprs {
			pns[i],err = convertTe(expr,s)
			if err!=nil { return }
		}
		//dests := make([]engine.PDest,0,len(pns))
		/*
		for _,pnx := range pns {
			
			if te,ok := pnx.(*tablesExpr); ok {
			
			}
		}
		*/
	default: err = eUnimpl
	}
	//return nil,eUnimpl
	return
}
func convertSte(q sqlparser.SimpleTableExpr,s engine.PSchema) (pn preColumns,err error) {
	switch v := q.(type) {
	case sqlparser.TableName:
		tbl,err := s.FindTable(v)
		if err!=nil { return nil,err }
		return &tablesExpr{
			mkCols(mkTsid(v),tbl.Dest,tbl.Columns),
			[]*engine.Table{tbl},
			tbl.Name,
			true,
			tbl.Dest,
		},nil
	}
	return nil,eUnimpl
}

//---

