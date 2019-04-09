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


package schema

import "github.com/mad-day/db-utils/table"
import "github.com/xwb1989/sqlparser"
import "strings"
import "fmt"

type insertCompiler struct {
	t table.Table
	tm map[string]int
	allCols bool
	cols []int
	
	values [][]interface{}
	op table.TableOp
	
	ondup_cols []int
	ondup_vals []interface{}
}
func (c *insertCompiler) setupTable() {
	c.tm = make(map[string]int)
	for i,n := range c.t.Columns() {
		c.tm[strings.ToLower(n)] = i
	}
}
func (c *insertCompiler) getCol(n string) int {
	i,ok := c.tm[strings.ToLower(n)]
	if !ok { panic("column not found: "+n) }
	return i
}
func (c *insertCompiler) compileInsert(s *Schema,dml *sqlparser.Insert) {
	if(dml.OnDup!=nil) { panic("unsupported: on duplicate key update ...") }
	
	c.t = s.Get(dml.Table.Name.String())
	if c.t==nil { panic("table not found: "+sqlparser.String(dml.Table)) }
	c.setupTable()
	
	c.allCols = len(dml.Columns)==0
	c.cols = make([]int,len(dml.Columns))
	for i,col := range dml.Columns { c.cols[i] = c.getCol(col.String()) }
	
	switch v := dml.Rows.(type) {
	case sqlparser.Values:
		c.values = make([][]interface{},len(v))
		for j,vv := range v {
			row := make([]interface{},len(vv))
			c.values[j] = row
			for jj,vvv := range vv {
				row[jj] = resolveValue(vvv)
			}
		}
	default: panic(fmt.Sprintf("unsupported in insert: %T(%v)",v,v))
	}
	switch dml.Action {
	case sqlparser.InsertStr:
		if dml.Ignore=="ignore " {
			c.op = table.T_InsertIgnore
		} else {
			c.op = table.T_Insert
		}
	case sqlparser.ReplaceStr:
		if dml.Ignore=="ignore " { panic("illegal: replace ignore into ...") }
		c.op = table.T_Replace
	}
	
	c.ondup_cols = make([]int,len(dml.OnDup))
	c.ondup_vals = make([]interface{},len(dml.OnDup))
	
	for i,upd := range dml.OnDup {
		c.ondup_cols[i] = c.getCol(upd.Name.Name.String())
		c.ondup_vals[i] = resolveValue(upd.Expr)
	}
}

/*
type InsertJob struct {
	Table table.Table
	
	AllCols bool
	Cols []int
	
	Values [][]interface{}
	Op table.TableOp
	
	OndupCols []int
	OndupVals []interface{}
	
}
*/

// Compiles an insert-statement.
func (s *Schema) CompileInsert(dml *sqlparser.Insert) (tab table.Table,job *table.TableInsert,err error) {
	defer func() { if r := recover(); r!=nil { err = any2err(r) } }()
	c := new(insertCompiler)
	c.compileInsert(s,dml)
	
	tab = c.t
	job = new(table.TableInsert)
	job.AllCols = c.allCols
	job.Cols = c.cols
	job.Values = c.values
	job.Op = c.op
	
	job.OndupCols = c.ondup_cols
	job.OndupVals = c.ondup_vals
	return
}

