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
	//t.rec[0] = key
	util.SetInKey(t.rec[0],key)
	err = msgpackx.Unmarshal(val,t.rec[1:]...)
	if err!=nil { return err }
	for i,j := range cols {
		vals[i] = util.GetPtr(t.rec[j])
	}
	return nil
}

type placer struct {
	bolt.VisitorDefault
	op bolt.VisitOp
}
func (p *placer) VisitFull(key, value []byte) bolt.VisitOp { return p.op }
func (p *placer) VisitEmpty(key []byte) bolt.VisitOp { return p.op }

type tableM struct {
	tableI
	buf []interface{}
}
func (t *tableM) Close() error {
	err := t.tx.Commit()
	t.tx = nil
	return err
}
func (t *tableM) Abort() error {
	err := t.tx.Rollback()
	t.tx = nil
	return err
}
func (t *tableM) TableUpdate(tu *table.TableUpdate) error {
	p := new(placer)
	switch tu.Op {
	case table.T_Update:
		for {
			_,val,err := t.next()
			if err==io.EOF { return nil }
			if err!=nil { return err }
			err = msgpackx.Unmarshal(val,t.rec[1:]...)
			if err!=nil { return err }
			for i,j := range tu.UpdCols {
				err = util.SetInPtr(t.rec[j],tu.UpdVals[i])
				if err!=nil { return err }
			}
			for i := range t.buf {
				t.buf[i] = util.GetPtr(t.rec[i+1])
			}
			val,err = msgpackx.Marshal(t.buf...)
			if err!=nil { return err }
			p.op = bolt.VisitOpSET(val)
			err = t.cur.Accept(p,true)
			if err!=nil { return err }
		}
	case table.T_Delete:
		p.op = bolt.VisitOpDELETE()
		for {
			_,_,err := t.next()
			if err==io.EOF { return nil }
			if err!=nil { return err }
			err = t.cur.Accept(p,true)
			if err!=nil { return err }
		}
	default:
		return fmt.Errorf("Illegal op %v",tu.Op)
	}
	
	panic("unreachable")
}

type tableC struct {
	bolt.VisitorDefault
	op  table.TableOp
	tx  *bolt.Tx
	bkt *bolt.Bucket
	rec,orig []interface{}
	buf []interface{}
	active bool
	err error
	updCols []int
	updVals []interface{}
}

func (t *tableC) encode() ([]byte,error) {
	for i := range t.buf {
		t.buf[i] = util.GetPtr(t.rec[i+1])
	}
	return msgpackx.Marshal(t.buf...)
}
func (t *tableC) errOp(err error) (op bolt.VisitOp) {
	t.err = err
	return
}
func (t *tableC) VisitEmpty(key []byte) (op bolt.VisitOp) {
	val,err := t.encode()
	if err!=nil { return t.errOp(err) }
	return bolt.VisitOpSET(val)
}
func (t *tableC) VisitFull(key, value []byte) bolt.VisitOp {
	switch t.op {
	case table.T_Insert:
		if len(t.updCols)!=0 { goto upd }
		t.err = table.ErrDuplicateKey
		fallthrough
	case table.T_InsertIgnore: return bolt.VisitOp{}
	case table.T_Replace:
		return t.VisitEmpty(key)
	}
	upd:
	err := msgpackx.Unmarshal(value,t.buf[1:])
	if err!=nil { return t.errOp(err) }
	for i,j := range t.updCols {
		err = util.SetInPtr(t.rec[j],t.updVals[i])
		if err!=nil { return t.errOp(err) }
	}
	return t.VisitEmpty(key)
}

func (t *tableC) discard() {
	if t.active { return }
	t.Abort()
}
func (t *tableC) Close() error {
	err := t.tx.Commit()
	t.tx = nil
	return err
}
func (t *tableC) Abort() error {
	err := t.tx.Rollback()
	t.tx = nil
	return err
}
func (t *tableC) TableInsert(ti *table.TableInsert) (err error) {
	var key []byte
	for _,value := range ti.Values {
		for i,p := range t.rec { util.SetInPtr(p,t.orig[i]) }
		if ti.AllCols {
			for i,p := range t.rec {
				err = util.SetInPtr(p,value[i])
				if err!=nil { return }
			}
		} else {
			for i,j := range ti.Cols {
				err = util.SetInPtr(t.rec[j],value[i])
				if err!=nil { return }
			}
		}
		key,err = util.GetKey(t.rec[0])
		if err!=nil { return }
		err = t.bkt.Accept(key,t,true)
		if err!=nil { return }
		if t.err!=nil { return t.err }
	}
	return
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
func (db *DBTable) modify() (*tableM,error) {
	tx,err := db.DB.Begin(true)
	if err!=nil { return nil,err }
	tbl := &tableM{tableI{tx:tx},nil}
	defer tbl.discard()
	bkt,err := tx.CreateBucketIfNotExists(db.Bucket)
	if err!=nil { return nil,err }
	tbl.cur = bkt.Cursor()
	tbl.rec = make([]interface{},len(db.Types))
	tbl.buf = make([]interface{},len(db.Types)-1)
	for i,t := range db.Types {
		v := reflect.New(t)
		tbl.rec[i] = v.Interface()
	}
	tbl.active = true
	return tbl,nil
}
func (db *DBTable) creator() (*tableC,error) {
	tx,err := db.DB.Begin(true)
	if err!=nil { return nil,err }
	tbl := &tableC{tx:tx}
	defer tbl.discard()
	bkt,err := tx.CreateBucketIfNotExists(db.Bucket)
	if err!=nil { return nil,err }
	tbl.bkt = bkt
	tbl.rec = make([]interface{},len(db.Types))
	tbl.orig = make([]interface{},len(db.Types))
	tbl.buf = make([]interface{},len(db.Types)-1)
	for i,t := range db.Types {
		v := reflect.New(t)
		tbl.rec[i] = v.Interface()
		tbl.orig[i] = v.Elem().Interface()
	}
	tbl.active = true
	return tbl,nil
}
func (db *DBTable) TableScan(cols []int,meta *table.TableScan) (table.TableIterator,error) {
	ti,err := db.iter()
	if err!=nil { return nil,err }
	defer ti.discard()
	ti.active = false
	_,err = db.tableScan(ti,cols,meta)
	if err!=nil { return nil,err }
	ti.active = true
	return ti,nil
}
func (db *DBTable) TablePrepareUpdate(tu *table.TableUpdate) (table.TableUpdateStmt,error) {
	for _,j := range tu.UpdCols { if j==0 { return nil,fmt.Errorf("Trying to update the primary key") } }
	ti,err := db.modify()
	if err!=nil { return nil,err }
	defer ti.discard()
	ti.active = false
	_,err = db.tableScan(&(ti.tableI),nil,tu.Scan)
	if err!=nil { return nil,err }
	ti.active = true
	return ti,nil
}

func (db *DBTable) tableScan(ti *tableI,cols []int,meta *table.TableScan) (*int,error) {
	
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
	return nil,nil
}

func (db *DBTable) TablePrepareInsert(ti *table.TableInsert) (table.TableInsertStmt,error) {
	tc,err := db.creator()
	if err!=nil { return nil,err }
	defer tc.discard()
	if !ti.AllCols {
		cnt := 0
		for _,j := range ti.Cols { if j==0 { cnt++ } }
		if cnt==0 { return nil,fmt.Errorf("Primary key not specified") }
	}
	for _,j := range ti.OndupCols { if j==0 { return nil,fmt.Errorf("Trying to update the primary key") } }
	tc.updCols = ti.OndupCols
	tc.updVals = ti.OndupVals
	return tc,nil
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

