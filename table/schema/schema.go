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
import "strconv"
import "fmt"
import "time"
import "encoding/hex"

type PlaceHolder struct{
	ListArg bool
	Name string
}

type Setter struct{
	ptr *interface{}
	arr []interface{}
	listArg bool
}
func (s *Setter) Reset() {
	if !s.listArg { return }
	s.arr = s.arr[:0]
	*s.ptr = s.arr
}
func (s *Setter) Put(val interface{}) {
	if !s.listArg {
		*s.ptr = val
		return
	}
	s.arr = append(s.arr,val)
	*s.ptr = s.arr
}

type SetterMap map[string]*Setter

func (sm SetterMap) Reset() {
	for _,s := range sm { s.Reset() }
}
func (sm SetterMap) useph(i *interface{}) {
	ph,ok := (*i).(PlaceHolder)
	if !ok { return }
	sm[ph.Name] = &Setter{i,nil,ph.ListArg}
	if ph.ListArg {
		*i = []interface{}(nil)
	} else {
		*i = nil
	}
}

// If you know what you are doing!
func (sm SetterMap) Dangerous_Inspect(i *interface{}) { sm.useph(i) }

func (sm SetterMap) InspectTableScan(scan *table.TableScan) {
	for i := range scan.Filter {
		sm.useph(&(scan.Filter[i].Value))
		sm.useph(&(scan.Filter[i].Escape))
	}
}

func (sm SetterMap) InspectTuple(tuple []interface{}) {
	for i := range tuple {
		sm.useph(&(tuple[i]))
	}
}
func (sm SetterMap) InspectTuples(tuples [][]interface{}) {
	for _,tuple := range tuples {
		sm.InspectTuple(tuple)
	}
}


/*
This function is defined as:

	sm := make(SetterMap)
	sm.InspectTableScan(scan)
	return sm
*/
func FindPlaceHolders(scan *table.TableScan) SetterMap {
	sm := make(SetterMap)
	sm.InspectTableScan(scan)
	return sm
}

func any2err(i interface{}) error {
	e,ok := i.(error)
	if !ok { e = fmt.Errorf("%v",i) }
	return e
}

const tsf2 = "2006-01-02 15:04:05.999999999"
const tsf1 = "2006-01-02 15:04:05"
const tdf1 = "2006-01-02"
const tdf2 = "15:04:05"

func parseOptUint(s string,u bool) (i int64,e error) {
	if u {
		var ui uint64
		ui,e = strconv.ParseUint(s,-1,64)
		i = int64(ui)
	} else {
		i,e = strconv.ParseInt(s,-1,64)
	}
	return
}
func tryConvert(i interface{},ct *sqlparser.ConvertType) interface{}{
	// http://www.mysqltutorial.org/mysql-cast/
	switch ct.Type {
	// int64
	case "signed","unsigned":
		switch v := i.(type) {
		case nil: return int64(0)
		case int64: return i
		case float64: return int64(v)
		case string:
			b,e := parseOptUint(v,ct.Type=="unsigned")
			if e!=nil { panic(e) }
			return b
		case []byte:
			b,e := parseOptUint(string(v),ct.Type=="unsigned")
			if e!=nil { panic(e) }
			return b
		}
	// float64
	case "decimal":
		switch v := i.(type) {
		case nil: return float64(0)
		case int64: return float64(v)
		case float64: return i
		case string:
			b,e := strconv.ParseFloat(v,64)
			if e!=nil { panic(e) }
			return b
		case []byte:
			b,e := strconv.ParseFloat(string(v),64)
			if e!=nil { panic(e) }
			return b
		}
	// []byte
	case "binary":
		switch v := i.(type) {
		case nil: return []byte(nil)
		case int64: return []byte(strconv.FormatInt(v,10))
		case float64: return []byte(strconv.FormatFloat(v,'f',-1,64))
		case bool: if v { return []byte("true") } else { return []byte("false") }
		case []byte: return i
		case string: return []byte(v)
		case time.Time: return v.AppendFormat(make([]byte,0,len(tsf2)),tsf2)
		}
	// string
	case "char":
		switch v := i.(type) {
		case nil: return ""
		case int64: return strconv.FormatInt(v,10)
		case float64: return strconv.FormatFloat(v,'f',-1,64)
		case bool: if v { return "true" } else { return "false" }
		case []byte: return string(v)
		case string: return i
		case time.Time: return v.Format(tsf2)
		}
	// time.Time
	case "time","date","datetime":
		s := ""
		switch v := i.(type) {
		case nil: return time.Time{}
		case []byte: s = string(v)
		case string: s = v
		default: goto done
		}
		if s == "" { return time.Time{} }
		var tm time.Time
		var e error
		switch ct.Type {
		case "datetime":
			tm,e = time.Parse(tsf2,s)
			if e==nil { break }
			tm,e = time.Parse(tsf1,s)
		case "date":
			tm,e = time.Parse(tdf1,s)
		case "time":
			tm,e = time.Parse(tdf2,s)
		}
		if e!=nil { panic(e) }
		return tm
	}
	
	done:
	panic(fmt.Sprintf("cant convert %T(%v) into %s",i,i,ct.Type))
}

func resolveValue(expr sqlparser.Expr) interface{} {
	switch v := expr.(type) {
	case *sqlparser.ParenExpr: return resolveValue(v.Expr)
	case *sqlparser.NullVal,nil: return nil
	case sqlparser.BoolVal: return bool(v)
	case *sqlparser.SQLVal:
		switch v.Type {
		case sqlparser.StrVal: return string(v.Val)
		case sqlparser.IntVal,sqlparser.HexNum:
			i,err := strconv.ParseInt(string(v.Val),0,64)
			if err!=nil { panic(err) }
			return i
		case sqlparser.FloatVal:
			f,err := strconv.ParseFloat(string(v.Val),64)
			if err!=nil { panic(err) }
			return f
		case sqlparser.HexVal:
			nv := make([]byte,len(v.Val)/2)
			i,err := hex.Decode(nv,v.Val)
			if err!=nil { panic(err) }
			return nv[:i]
		//case sqlparser.BitVal:
		case sqlparser.ValArg: return PlaceHolder{false,string(v.Val[1:])}
		}
	case sqlparser.ListArg: return PlaceHolder{true,string(v[2:])}
	case *sqlparser.ConvertExpr: return tryConvert(resolveValue(v.Expr),v.Type)
	}
	panic("error resolving "+sqlparser.String(expr))
}


type Schema struct {
	Tables map[string]table.Table
}
func (s *Schema) Put(n string,t table.Table) {
	if s.Tables==nil { s.Tables = make(map[string]table.Table) }
	s.Tables[strings.ToLower(n)] = t
}
func (s *Schema) Get(n string) table.Table {
	if s==nil { return nil }
	return s.Tables[strings.ToLower(n)]
}



type compiler struct {
	s *Schema
	t table.Table
	tm map[string]int
	cols []int
	
	scan *table.TableScan
	
	op table.TableOp
	updCols []int
	updVals []interface{}
}
func (c *compiler) setupTable() {
	c.tm = make(map[string]int)
	for i,n := range c.t.Columns() {
		c.tm[strings.ToLower(n)] = i
	}
}
func (c *compiler) setTable(expr sqlparser.TableExpr) {
	restart:
	switch v := expr.(type) {
	case *sqlparser.ParenTableExpr:
		if len(v.Exprs)!=1 { panic("invalid table expression: << "+sqlparser.String(expr)+" >>") }
		expr = v.Exprs[0]
		goto restart
	case *sqlparser.AliasedTableExpr:
		if sx,ok := v.Expr.(sqlparser.TableName); ok {
			n := sx.Name.String()
			c.t = c.s.Get(n)
			if c.t==nil { panic("table not found: "+sqlparser.String(sx)) }
			c.setupTable()
		} else {
			panic("invalid table expression: << "+sqlparser.String(v.Expr)+" >>")
		}
	}
}
func (c *compiler) appendCols0(s sqlparser.Expr) {
	restart:
	switch v := s.(type) {
	case *sqlparser.ParenExpr: s = v.Expr; goto restart
	case *sqlparser.ColName:
		if i,ok := c.tm[strings.ToLower(v.Name.String())]; ok {
			c.cols = append(c.cols,i)
		} else {
			panic("column not found: "+v.Name.String())
		}
	default:
		panic("invalid select expression: << "+sqlparser.String(s)+" >>")
	}
}
func (c *compiler) getColumn(s sqlparser.Expr) int {
	restart:
	switch v := s.(type) {
	case *sqlparser.ParenExpr: s = v.Expr; goto restart
	case *sqlparser.ColName:
		if i,ok := c.tm[strings.ToLower(v.Name.String())]; ok {
			return i
		} else {
			panic("column not found: "+v.Name.String())
		}
	}
	panic("invalid column expression: << "+sqlparser.String(s)+" >>")
}
func (c *compiler) appendCols(s sqlparser.SelectExpr) {
	switch v := s.(type) {
	case *sqlparser.StarExpr:
		for i := range c.t.Columns() {
			c.cols = append(c.cols,i)
		}
	case *sqlparser.AliasedExpr:
		c.appendCols0(v.Expr)
	default:
		panic("invalid select expression: << "+sqlparser.String(s)+" >>")
	}
}
func (c *compiler) addFilter(expr sqlparser.Expr) {
	switch v := expr.(type) {
	case *sqlparser.ParenExpr:
		c.addFilter(v.Expr)
	case *sqlparser.AndExpr:
		c.addFilter(v.Left)
		c.addFilter(v.Right)
	case *sqlparser.ComparisonExpr:
		{
			i := c.getColumn(v.Left)
			r := resolveValue(v.Right)
			e := resolveValue(v.Escape)
			c.scan.Filter = append(c.scan.Filter,table.ColumnFilter{i,v.Operator,r,e})
		}
	case *sqlparser.RangeCond:
		{
			b,e := ">=","<="
			if v.Operator==sqlparser.NotBetweenStr { b,e = "<",">" }
			i := c.getColumn(v.Left)
			f := resolveValue(v.From)
			t := resolveValue(v.To)
			c.scan.Filter = append(c.scan.Filter,table.ColumnFilter{i,b,f,nil},table.ColumnFilter{i,e,t,nil})
		}
	}
}
func (c *compiler) addOrder(o *sqlparser.Order) {
	i := c.getColumn(o.Expr)
	c.scan.Order = append(c.scan.Order,table.ColumnOrder{i,o.Direction==sqlparser.DescScr})
}

func (c *compiler) addLimit(l *sqlparser.Limit) {
	if l!=nil {
		panic("not supported: limit")
	}
}
func (c *compiler) compileSel(s *sqlparser.Select) {
	c.scan = new(table.TableScan)
	if len(s.From)!=1 { panic("invalid table expression: << "+sqlparser.String(s.From)+" >>") }
	if len(s.GroupBy)!=0 { panic("unsupported group_by") }
	c.setTable(s.From[0])
	for _,expr := range s.SelectExprs {
		c.appendCols(expr)
	}
	if s.Where!=nil { c.addFilter(s.Where.Expr) }
	if s.Having!=nil { c.addFilter(s.Having.Expr) }
	for _,o := range s.OrderBy { c.addOrder(o) }
	
	c.addLimit(s.Limit)
}

func (s *Schema) CompileSelect(q *sqlparser.Select) (t table.Table,cols []int, scan *table.TableScan, err error) {
	defer func() { if r := recover(); r!=nil { err = any2err(r) } }()
	c := new(compiler)
	c.s = s
	c.compileSel(q)
	t = c.t
	cols = c.cols
	scan = c.scan
	return
}

func (c *compiler) compileUpd(s *sqlparser.Update) {
	c.scan = new(table.TableScan)
	c.op = table.T_Update
	if len(s.TableExprs)!=1 { panic("invalid table expression: << "+sqlparser.String(s.TableExprs)+" >>") }
	c.setTable(s.TableExprs[0])
	if s.Where!=nil { c.addFilter(s.Where.Expr) }
	for _,o := range s.OrderBy { c.addOrder(o) }
	
	c.updCols = make([]int,len(s.Exprs))
	c.updVals = make([]interface{},len(s.Exprs))
	for i,upd := range s.Exprs {
		c.updCols[i] = c.getColumn(upd.Name)
		c.updVals[i] = resolveValue(upd.Expr)
	}
	
	c.addLimit(s.Limit)
}
func (c *compiler) compileDel(s *sqlparser.Delete) {
	c.scan = new(table.TableScan)
	c.op = table.T_Delete
	if len(s.Targets)!=0 { panic("not supported: targets") }
	if len(s.TableExprs)!=1 { panic("invalid table expression: << "+sqlparser.String(s.TableExprs)+" >>") }
	c.setTable(s.TableExprs[0])
	if s.Where!=nil { c.addFilter(s.Where.Expr) }
	for _,o := range s.OrderBy { c.addOrder(o) }
	
	c.addLimit(s.Limit)
}

/*
type UpdateDeleteJob struct{
	Tab table.Table
	Op  table.TableOp
	Scan *table.TableScan
	
	UpdCols []int
	UpdVals []interface{}
}
*/

// Compiles an update statement.
func (s *Schema) CompileUpdate(dml *sqlparser.Update) (tab table.Table, job *table.TableUpdate,err error) {
	defer func() { if r := recover(); r!=nil { err = any2err(r) } }()
	
	c := new(compiler)
	c.s = s
	c.compileUpd(dml)
	
	job = new(table.TableUpdate)
	tab = c.t
	job.Op = c.op
	job.UpdCols = c.updCols
	job.UpdVals = c.updVals
	job.Scan = c.scan
	return
}

// Compiles an delete statement.
func (s *Schema) CompileDelete(dml *sqlparser.Delete) (tab table.Table, job *table.TableUpdate,err error) {
	defer func() { if r := recover(); r!=nil { err = any2err(r) } }()
	
	c := new(compiler)
	c.s = s
	c.compileDel(dml)
	
	job = new(table.TableUpdate)
	tab = c.t
	job.Op = c.op
	job.UpdCols = c.updCols
	job.UpdVals = c.updVals
	job.Scan = c.scan
	return
}
