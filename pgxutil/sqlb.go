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


package pgxutil

import (
	"github.com/jackc/pgx"
	"fmt"
	"strings"
	"reflect"
)

func EscapeIdentifier(name string) string {
	return "\""+strings.Replace(name,"\"","\"\"",-1)+"\""
}

type CommonArg interface{
	isCommonArg()
}
/*
type CommonArg_Impl struct{}
func (CommonArg_Impl) isCommonArg() {}
var CommonArg_Inst CommonArg = CommonArg_Impl{}
*/

//
type QueryArg interface{
	CommonArg
	isQueryArg()
}
type cQueryArgNum int
func (c cQueryArgNum) String() string { return fmt.Sprintf("$%d",int(c)) }
func (c cQueryArgNum) isCommonArg(){}
func (c cQueryArgNum) isQueryArg(){}

func NewQueryArg(i int) QueryArg { return cQueryArgNum(i) }

type cQueryArgText string
func (c cQueryArgText) String() string { return "'"+strings.Replace(string(c),"'","''",-1)+"'" }
func (c cQueryArgText) isCommonArg(){}
func (c cQueryArgText) isQueryArg(){}

type cQueryArgSrc string
func (c cQueryArgSrc) isCommonArg(){}
func (c cQueryArgSrc) isQueryArg(){}

type cQueryArgArray []QueryArg
func (c cQueryArgArray) String() string {
	b := new(strings.Builder)
	b.WriteString("ARRAY[")
	for i,v := range c {
		if i!=0 { b.WriteByte(',') }
		fmt.Fprint(b,v)
	}
	b.WriteString("]")
	return b.String()
}
func (c cQueryArgArray) isCommonArg(){}
func (c cQueryArgArray) isQueryArg(){}

func NewQueryArgArray(args ...QueryArg) QueryArg { return cQueryArgArray(args) }

type cQueryArgRow []QueryArg
func (c cQueryArgRow) String() string {
	b := new(strings.Builder)
	b.WriteString("ROW(")
	for i,v := range c {
		if i!=0 { b.WriteByte(',') }
		fmt.Fprint(b,v)
	}
	b.WriteString(")")
	return b.String()
}
func (c cQueryArgRow) isCommonArg(){}
func (c cQueryArgRow) isQueryArg(){}

func NewQueryArgRow(args ...QueryArg) QueryArg { return cQueryArgRow(args) }

func NewQueryArgValue(i interface{}) QueryArg {
	ityp := reflect.TypeOf(i)
	if ityp==nil { return cQueryArgSrc("NULL") }
	ival := reflect.ValueOf(i)
	switch ityp.Kind() {
	case reflect.String: return cQueryArgText(ival.String())
	case reflect.Slice:
		if ityp.Elem().Kind()==reflect.Uint8 {
			return cQueryArgSrc(fmt.Sprintf(`bytea '\x%X'`,ival.Bytes()))
		}
		fallthrough
	case reflect.Array:
		arr := make(cQueryArgArray,ival.Len())
		for i := range arr {
			arr[i] = NewQueryArgValue(ival.Index(i).Interface())
		}
		return arr
	case reflect.Int,reflect.Int8,reflect.Int16,reflect.Int32,reflect.Int64:
		return cQueryArgSrc(fmt.Sprintf(`%d`,ival.Int()))
	case reflect.Uint,reflect.Uint8,reflect.Uint16,reflect.Uint32,reflect.Uint64,reflect.Uintptr:
		return cQueryArgSrc(fmt.Sprintf(`%d`,ival.Uint()))
	case reflect.Float32,reflect.Float64:
		return cQueryArgSrc(fmt.Sprintf(`%f`,ival.Float()))
	case reflect.Complex64,reflect.Complex128:
		comp := ival.Complex()
		return cQueryArgSrc(fmt.Sprintf(`%f`,real(comp)))
	case reflect.Ptr:
		return NewQueryArgValue(ival.Elem().Interface())
	case reflect.Interface:
		panic("illegal state: interface is not a concrete type")
	case reflect.Struct:
		arr := make(cQueryArgRow,ival.NumField())
		for i := range arr {
			arr[i] = NewQueryArgValue(ival.Field(i).Interface())
		}
		return arr
	case reflect.UnsafePointer: panic(fmt.Sprint("illegal use of ",ityp," as literal"))
	case reflect.Chan: panic(fmt.Sprint("illegal use of channel as literal: ",ityp))
	case reflect.Func: panic(fmt.Sprint("illegal use of function as literal: ",ityp))
	//case reflect.Map:
	default: panic(fmt.Sprint("illegal use of ",ityp.Kind()," as literal: ",ityp))
	}
	panic("unreachable")
}

type FieldUpdate interface{
	CommonArg
	Update(fieldExpr string) string
}

func toFieldUpdate(ca CommonArg) FieldUpdate {
	if qa,ok := ca.(QueryArg) ; ok { return fieldUpdateSet{qa} }
	return ca.(FieldUpdate)
}


type fieldUpdateSet struct{
	arg QueryArg
}
func (f fieldUpdateSet) Update(fieldExpr string) string { return fmt.Sprint(f.arg) }
func (f fieldUpdateSet) isCommonArg(){}
func FieldUpdateSet(arg QueryArg) FieldUpdate { return fieldUpdateSet{arg} }



type fieldUpdateArraySet struct{
	idx QueryArg
	nval FieldUpdate
}
func (f fieldUpdateArraySet) isCommonArg(){}
func (f fieldUpdateArraySet) Update(fieldExpr string) string {
	idx := fmt.Sprint(f.idx)
	nfield := fmt.Sprintf("%s[%s]",fieldExpr,idx)
	nval := f.nval.Update(nfield)
	
	return fmt.Sprintf(`array_cat( array_append(%s[:%s - 1],%v),%s[%s + 1:])`,fieldExpr,idx,nval,fieldExpr,idx)
}
// nval = QueryArg|FieldUpdate
func FieldUpdateArraySet(idx QueryArg,nval CommonArg) FieldUpdate { return fieldUpdateArraySet{idx,toFieldUpdate(nval)} }

type FieldFilter interface{
	CommonArg
	Filter(fieldExpr string) string
}

func toFieldFilter(ca CommonArg) FieldFilter {
	if qa,ok := ca.(QueryArg); ok { return fieldFilterOp{"=",qa} }
	return ca.(FieldFilter)
}

type fieldFilterOp struct {
	op string
	qa QueryArg
}
func (f fieldFilterOp) isCommonArg(){}
func (f fieldFilterOp) Filter(fieldExpr string) string {
	return fmt.Sprint(fieldExpr,f.op,f.qa)
}

func FieldFilterEq(q QueryArg) FieldFilter { return fieldFilterOp{"=",q} }
func FieldFilterLe(q QueryArg) FieldFilter { return fieldFilterOp{"<=",q} }
func FieldFilterGe(q QueryArg) FieldFilter { return fieldFilterOp{">=",q} }
func FieldFilterLt(q QueryArg) FieldFilter { return fieldFilterOp{"<",q} }
func FieldFilterGt(q QueryArg) FieldFilter { return fieldFilterOp{">",q} }

type sqlFilter struct {
	name string
	filter FieldFilter
}
type sqlFilters []sqlFilter
func (s *sqlFilters) AddWhere(name string,filter CommonArg) { *s = append(*s,sqlFilter{name,toFieldFilter(filter)}) }

type sqlUpdate struct {
	name string
	update FieldUpdate
}
type sqlUpdates []sqlUpdate
func (s *sqlUpdates) AddUpdate(name string,update CommonArg) { *s = append(*s,sqlUpdate{name,toFieldUpdate(update)}) }

type sqlUpdateBuilder struct {
	table string
	sqlUpdates
	sqlFilters
}
func (s sqlUpdateBuilder) String() string {
	b := new(strings.Builder)
	fmt.Fprintf(b,`UPDATE %s `,EscapeIdentifier(s.table))
	for i,u := range s.sqlUpdates {
		if i==0 { b.WriteString("SET ") } else { b.WriteString(", ") }
		fmt.Fprintf(b,"%s = %s",EscapeIdentifier(u.name),u.update.Update(EscapeIdentifier(u.name)))
	}
	for i,f := range s.sqlFilters {
		if i==0 { b.WriteString(" WHERE ") } else { b.WriteString("AND ") }
		b.WriteString(f.filter.Filter(EscapeIdentifier(f.name)))
	}
	return b.String()
}
func (s sqlUpdateBuilder) PrepareWith(q IQueryable,name string) (*pgx.PreparedStatement, error) {
	return q.Prepare(name,s.String())
}

type SqlUpdate interface{
	AddUpdate(name string,update CommonArg) // update = QueryArg|FieldUpdate
	AddWhere(name string,filter CommonArg) // filter = QueryArg|FieldFilter
	String() string
	PrepareWith(q IQueryable,name string) (*pgx.PreparedStatement, error)
}

func NewSqlUpdate(table string) SqlUpdate {
	return &sqlUpdateBuilder{table:table}
}

type sqlSelectBuilder struct{
	exprs string
	table string
	sqlFilters
}
func (s *sqlSelectBuilder) SetExprs(exprs string) { s.exprs = exprs }
func (s *sqlSelectBuilder) AddExpr(expr string) {
	if s.exprs!="" { s.exprs += " , " }
	s.exprs += expr
}
func (s sqlSelectBuilder) String() string {
	b := new(strings.Builder)
	b.WriteString("SELECT ")
	b.WriteString(s.exprs)
	b.WriteString(" FROM ")
	b.WriteString(EscapeIdentifier(s.table))
	for i,f := range s.sqlFilters {
		if i==0 { b.WriteString(" WHERE ") } else { b.WriteString("AND ") }
		b.WriteString(f.filter.Filter(EscapeIdentifier(f.name)))
	}
	return b.String()
}
func (s sqlSelectBuilder) PrepareWith(q IQueryable,name string) (*pgx.PreparedStatement, error) {
	return q.Prepare(name,s.String())
}

type SqlSelect interface{
	SetExprs(exprs string)
	AddExpr(expr string)
	AddWhere(name string,filter CommonArg) // filter = QueryArg|FieldFilter
	String() string
	PrepareWith(q IQueryable,name string) (*pgx.PreparedStatement, error)
}

func NewSqlSelect(table string) SqlSelect {
	return &sqlSelectBuilder{table:table}
}

