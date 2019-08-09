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
	"context"
)

type Preparable interface{
	PrepareWith(q IQueryable,name string) (*pgx.PreparedStatement, error)
}

type cQueryBinding struct {
	prep Preparable
	name string
}
type QueryCollection struct{
	bindings []cQueryBinding
	i int
}
func (qc *QueryCollection) Init(q IQueryable) error {
	for i := range qc.bindings {
		_,err := qc.bindings[i].prep.PrepareWith(q,qc.bindings[i].name)
		if err!=nil { return err }
	}
	return nil
}
func (qc *QueryCollection) AddPrepared(p Preparable, name string) {
	qc.bindings = append(qc.bindings,cQueryBinding{p,name})
}

const AutoName = "i_query_%i"

type QC_Query func(q IQueryable,args ...interface{}) (*pgx.Rows, error)

func (qc *QueryCollection) AddQuery(p Preparable) QC_Query {
	qc.i++
	name := fmt.Sprintf(AutoName,qc.i)
	qc.AddPrepared(p,name)
	
	return func(q IQueryable,args ...interface{}) (*pgx.Rows, error) {
		return q.Query(name,args...)
	}
}

type QC_QueryEx func(q IQueryable,ctx context.Context,args ...interface{}) (*pgx.Rows, error)

func (qc *QueryCollection) AddQueryEx(p Preparable,options *pgx.QueryExOptions) QC_QueryEx {
	qc.i++
	name := fmt.Sprintf(AutoName,qc.i)
	qc.AddPrepared(p,name)
	
	return func(q IQueryable,ctx context.Context,args ...interface{}) (*pgx.Rows, error) {
		return q.QueryEx(ctx,name,options,args...)
	}
}

type QC_QueryRow func(q IQueryable,args ...interface{}) *pgx.Row

func (qc *QueryCollection) AddQueryRow(p Preparable) QC_QueryRow {
	qc.i++
	name := fmt.Sprintf(AutoName,qc.i)
	qc.AddPrepared(p,name)
	
	return func(q IQueryable,args ...interface{}) *pgx.Row {
		return q.QueryRow(name,args...)
	}
}

type QC_QueryRowEx func(q IQueryable,ctx context.Context,args ...interface{}) *pgx.Row

func (qc *QueryCollection) AddQueryRowEx(p Preparable,options *pgx.QueryExOptions) QC_QueryRowEx {
	qc.i++
	name := fmt.Sprintf(AutoName,qc.i)
	qc.AddPrepared(p,name)
	
	return func(q IQueryable,ctx context.Context,args ...interface{}) *pgx.Row {
		return q.QueryRowEx(ctx,name,options,args...)
	}
}


type QC_Exec func(q IQueryable,args ...interface{}) (pgx.CommandTag, error)

func (qc *QueryCollection) AddExec(p Preparable) QC_Exec {
	qc.i++
	name := fmt.Sprintf(AutoName,qc.i)
	qc.AddPrepared(p,name)
	
	return func(q IQueryable,args ...interface{}) (pgx.CommandTag, error) {
		return q.Exec(name,args...)
	}
}

type QC_ExecEx func(q IQueryable,ctx context.Context,args ...interface{}) (pgx.CommandTag, error)

func (qc *QueryCollection) AddExecEx(p Preparable,options *pgx.QueryExOptions) QC_ExecEx {
	qc.i++
	name := fmt.Sprintf(AutoName,qc.i)
	qc.AddPrepared(p,name)
	
	return func(q IQueryable,ctx context.Context,args ...interface{}) (pgx.CommandTag, error) {
		return q.ExecEx(ctx,name,options,args...)
	}
}



