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
	"io"
	"bytes"
	"github.com/mad-day/db-utils/table/util"
)

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
	if t.pick {
		key,val = t.key,t.val
		t.pick = false
	} else {
		key,val = t.cur.Next()
	}
	if len(key)==0 { err = io.EOF; return }
	if len(t.end)==0 { return }
	switch bytes.Compare(key,t.end) {
	case 0: if !t.incl { err = io.EOF }
	case 1: err = io.EOF
	}
	return
}
func (t *tableI) Next(cols []int,vals []interface{}) error {
	key,val,err := t.next()
	if err!=nil { return err }
	t.rec[0] = key
	err = msgpackx.Unmarshal(val,t.rec[1:]...)
	if err!=nil { return err }
	for i,j := range cols {
		vals[i] = util.GetPtr(t.rec[j])
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
	foundFilter := false
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
			foundFilter = true
		default: return nil,meta.Filter[i].Err(table.E_FILTER_OPERATOR_UNSUPP,db.Fields)
		}
	}
	if !foundFilter {
		ti.key,ti.val = ti.cur.First()
		ti.pick = true
	}
	ti.active = true
	return ti,nil
}

func (db *DBTable) ADM_create() error{
	return db.DB.Update(func(tx *bolt.Tx) error {
		_,err := tx.CreateBucketIfNotExists(db.Bucket)
		return err
	})
}
func (db *DBTable) ADM_insert(vals ...interface{}) error {
	rec := make([]interface{},len(db.Types))
	for i,t := range db.Types {
		v := reflect.New(t)
		util.SetInPtr(v.Interface(),vals[i])
		rec[i] = v.Elem().Interface()
	}
	return db.DB.Update(func(tx *bolt.Tx) error {
		row,err := msgpackx.Marshal(rec[1:]...)
		if err!=nil { return err }
		return tx.Bucket(db.Bucket).Put(rec[0].([]byte),row)
	})
}

