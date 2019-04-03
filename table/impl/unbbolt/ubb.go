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


package ubbolt

import (
	bolt "github.com/maxymania/go-unstable/bbolt"
	"github.com/mad-day/db-utils/table"
	"github.com/byte-mug/golibs/msgpackx"
	"reflect"
	"fmt"
	"time"
)

func setInKey(dest interface{}, val []byte) error{
	switch v := dest.(type) {
	case *string: *v = string(val)
	case *[]byte: *v = val
	default: return fmt.Errorf("Invalid assignment %T <- []byte",dest)
	}
	return nil
}
func setInPtr(dest, val interface{}) error {
	if val==nil {
		dv := reflect.ValueOf(dest)
		dv = reflect.Indirect(dv)
		dv.Set(reflect.Zero(dv.Type()))
		return nil
	}
	
	var ok bool
	var rv reflect.Value
	switch v := dest.(type) {
	case *int64: *v,ok = val.(int64)
	case *float64: *v,ok = val.(float64)
	case *bool: *v,ok = val.(bool)
	case *[]byte:
		ok = true
		switch s := val.(type) {
		case string: *v = []byte(s)
		case []byte: *v = s
		default: ok = false
		}
	case *string:
		ok = true
		switch s := val.(type) {
		case string: *v = s
		case []byte: *v = string(s)
		default: ok = false
		}
	case *time.Time: *v,ok = val.(time.Time)
	case nil: return nil
	default: goto skipSpecific
	}
	if ok { return nil }
	
	rv = reflect.ValueOf(val)
	rv = reflect.Indirect(rv)
	switch v := dest.(type) {
	case *int64:
		ok = true
		switch rv.Kind(){
		case reflect.Int8,reflect.Int16,reflect.Int32,reflect.Int64: *v = rv.Int()
		case reflect.Uint8,reflect.Uint16,reflect.Uint32,reflect.Uint64: *v = int64(rv.Uint())
		default: ok = false
		}
	case *float64:
		ok = true
		switch rv.Kind(){
		case reflect.Float32,reflect.Float64: *v = rv.Float()
		default: ok = false
		}
	case *bool:
		ok = true
		switch rv.Kind(){
		case reflect.Bool: *v = rv.Bool()
		default: ok = false
		}
	case *[]byte:
		switch rv.Kind(){
		case reflect.Slice:
			if rv.Type().Elem().Kind()==reflect.Uint8 {
				*v = rv.Bytes()
				ok = true
			}
		case reflect.String:
			*v = []byte(rv.String())
		}
	case *string:
		switch rv.Kind(){
		case reflect.Slice:
			if rv.Type().Elem().Kind()==reflect.Uint8 {
				*v = string(rv.Bytes())
				ok = true
			}
		case reflect.String:
			*v = rv.String()
		}
	}
	
	if ok { return nil }
	
	skipSpecific:
	
	return fmt.Errorf("Invalid assignment %T <- %T",dest,val)
}

func getPtr(ptr interface{}) interface{} {
	switch vp := ptr.(type){
	case *int64: return *vp
	case *float64: return *vp
	case *bool: return *vp
	case *[]byte: return *vp
	case *string: return *vp
	case *time.Time: return *vp
	case nil: return nil
	}
	return reflect.Indirect(reflect.ValueOf(ptr)).Interface()
}

type tableI struct {
	tx  *bolt.Tx
	cur *bolt.Cursor
	key,val,end []byte
	pick,incl bool
	rec []interface{}
	active bool
}
func (t *tableI) discard() {
	if t.active { return }
	t.Close()
}
func (t *tableI) Close() error {
	err := t.tx.Rollback()
	t.tx = nil
	return err
}
func (t *tableI) next() (key,val []byte,err error) {
	panic("...")
}
func (t *tableI) Next(cols []int,vals []interface{}) error {
	key,val,err := t.next()
	if err!=nil { return err }
	t.rec[0] = key
	err = msgpackx.Unmarshal(val,t.rec[1:]...)
	if err!=nil { return err }
	for i,j := range cols {
		vals[j] = getPtr(t.rec[i])
	}
	return nil
}


type DBTable struct {
	DB *bolt.DB
	Bucket []byte
	Fields []string
	Types  []reflect.Type
}

func (db *DBTable) Columns() []string {
	return db.Fields
}
func (db *DBTable) iter() (*tableI,error) {
	tx,err := db.DB.Begin(false)
	if err!=nil { return nil,err }
	tbl := &tableI{tx:tx}
	defer tbl.discard()
	bkt := tx.Bucket(db.Bucket)
	if bkt==nil { return nil,fmt.Errorf("Bucket not found: %q",db.Bucket) }
	tbl.cur = bkt.Cursor()
	tbl.rec = make([]interface{},len(db.Types))
	for i,t := range db.Types {
		v := reflect.New(t)
		tbl.rec[i] = v.Interface()
	}
	tbl.active = true
	return tbl,nil
}
func (db *DBTable) TableScan(cols []int,meta *table.TableScan) (table.TableIterator,error) {
	ti,err := db.iter()
	if err!=nil { return nil,err }
	defer ti.discard()
	ti.active = false
	
	var op string
	var key []byte
	for i := range meta.Order {
		if meta.Order[i].Index != 0 { return nil,meta.Order[i].Err(table.E_ORDERBY_ORDER_FIELD,db.Fields) }
		if meta.Order[i].Desc { return nil,meta.Order[i].Err(table.E_ORDERBY_ORDER,db.Fields) }
	}
	for i := range meta.Filter {
		if meta.Filter[i].Index != 0 { return nil,meta.Filter[i].Err(table.E_FILTER_FIELD_UNSUPP,db.Fields) }
		
		switch meta.Filter[i].Operator {
		case "=","<=>","<","<=",">",">=":
			switch v := meta.Filter[i].Value.(type) {
				case []byte: key = v
				case string: key = []byte(v)
			}
			op = meta.Filter[i].Operator
			switch op {
			case "=","<=>":
				ti.end = key
				ti.incl = true
				ti.key,ti.val = ti.cur.Seek(key)
				ti.pick = true
			case ">",">=":
				ti.key,ti.val = ti.cur.Seek(key)
				ti.pick = op==">="
			case "<","<=":
				ti.end = key
				ti.incl = op=="<="
			}
		default: return nil,meta.Filter[i].Err(table.E_FILTER_OPERATOR_UNSUPP,db.Fields)
		}
	}
	
	return ti,nil
	//fmt.Printf("%s %q\n",op,key)
	//panic("???")
}

