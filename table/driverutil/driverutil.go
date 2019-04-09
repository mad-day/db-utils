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


package driverutil

import (
	"database/sql/driver"
	"github.com/mad-day/db-utils/table"
	"github.com/mad-day/db-utils/table/schema"
	"github.com/xwb1989/sqlparser"
	"context"
	"fmt"
)

type abstractScanner interface {
	scan() (driver.Rows,error)
	lng() int
}

type tableResultSet struct {
	tab  table.Table
	iter table.TableIterator
	cols []int
	vals []interface{}
}
func (t *tableResultSet) Columns() []string { return t.tab.Columns() }
func (t *tableResultSet) Close() error { return t.iter.Close() }
func (t *tableResultSet) Next(dest []driver.Value) error {
	err := t.iter.Next(t.cols, t.vals )
	n := len(t.vals)
	if m := len(dest) ; m<n { n = m }
	for i,v := range t.vals[:n] { dest[i] = v }
	return err
}

type tableScanner struct {
	tab  table.Table
	cols []int
	tscn *table.TableScan
}
func (t *tableScanner) lng() int { return len(t.cols) }
func (t *tableScanner) scan() (driver.Rows,error) {
	ti,err := t.tab.TableScan(t.cols,t.tscn)
	if err!=nil { return nil,err }
	return &tableResultSet{t.tab,ti,t.cols,make([]interface{},len(t.cols))},nil
}

type sqlSelect struct {
	abstractScanner
	sm schema.SetterMap
}
func (s *sqlSelect) Close() error { return nil }
func (s *sqlSelect) NumInput() int { return -1 }
func (s *sqlSelect) Exec(args []driver.Value) (driver.Result, error) { return nil,fmt.Errorf("unsupported") }
func (s *sqlSelect) Query(args []driver.Value) (driver.Rows, error) { return nil,nil }
func (s *sqlSelect) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	s.sm.Reset()
	for _,arg := range args {
		if s := s.sm[arg.Name]; s!=nil { s.Put(arg.Value) }
	}
	return s.scan()
}

type abstractModifier interface {
	Close() error
	execute() (driver.Result,error)
}

type insertModifier struct {
	tbl  table.TableInsertStmt
	meta *table.TableInsert
}
func (i *insertModifier) Close() error { return i.tbl.Close() }
func (i *insertModifier) execute() (driver.Result,error) {
	return i.tbl.TableInsert(i.meta)
}

type updateModifier struct {
	tbl  table.TableUpdateStmt
	meta *table.TableUpdate
}
func (i *updateModifier) Close() error { return i.tbl.Close() }
func (i *updateModifier) execute() (driver.Result,error) {
	return i.tbl.TableUpdate(i.meta)
}

type sqlModify struct {
	abstractModifier
	sm schema.SetterMap
}
//func (s *sqlModify) Close() error { return nil }
func (s *sqlModify) NumInput() int { return -1 }
func (s *sqlModify) Exec(args []driver.Value) (driver.Result, error) { return nil,nil }
func (s *sqlModify) Query(args []driver.Value) (driver.Rows, error) { return nil,fmt.Errorf("unsupported") }
func (s *sqlModify) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	s.sm.Reset()
	for _,arg := range args {
		if s := s.sm[arg.Name]; s!=nil { s.Put(arg.Value) }
	}
	return s.execute()
}

type Database struct {
	Sch schema.Schema
}
func (db *Database) iPrepare(q sqlparser.Statement) (driver.Stmt,error) {
	sm := make(schema.SetterMap)
	switch v := q.(type) {
	case *sqlparser.Select:
		{
			tab,cols,scan,err := db.Sch.CompileSelect(v)
			if err!=nil { return nil,err }
			sm.InspectTableScan(scan)
			return &sqlSelect{&tableScanner{tab,cols,scan},sm},nil
		}
	case *sqlparser.Insert:
		{
			tab,job,err := db.Sch.CompileInsert(v)
			if err!=nil { return nil,err }
			itab,_ := tab.(table.InsertableTable)
			if itab==nil { return nil,fmt.Errorf("table not updatible") }
			sm.InspectTuples(job.Values)
			sm.InspectTuple(job.OndupVals)
			ti,err := itab.TablePrepareInsert(job)
			if err!=nil { return nil,err }
			return &sqlModify{&insertModifier{ti,job},sm},nil
		}
	case *sqlparser.Update,*sqlparser.Delete:
		{
			var tab table.Table
			var job *table.TableUpdate
			var err error
			switch vv := q.(type) {
			case *sqlparser.Update: tab,job,err = db.Sch.CompileUpdate(vv)
			case *sqlparser.Delete: tab,job,err = db.Sch.CompileDelete(vv)
			}
			if err!=nil { return nil,err }
			utab,_ := tab.(table.UpdateableTable)
			if utab==nil { return nil,fmt.Errorf("table not updatible") }
			sm.InspectTableScan(job.Scan)
			sm.InspectTuple(job.UpdVals)
			tu,err := utab.TablePrepareUpdate(job)
			if err!=nil { return nil,err }
			return &sqlModify{&updateModifier{tu,job},sm},nil
		}
	default: return nil,fmt.Errorf("unsupported query %T",q)
	}
	panic("iPrepare")
}

func (db *Database) Close() error { return nil }
func (db *Database) Begin() (driver.Tx, error) { return nil,nil }
func (db *Database) Prepare(query string) (driver.Stmt, error) {
	stmt,err := sqlparser.Parse(query)
	if err!=nil { return nil,err }
	return db.iPrepare(stmt)
}

