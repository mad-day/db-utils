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

var (
	eUnimpl = fmt.Errorf("not implemented")
)

func mergePdests(targ,add map[engine.PDest]bool) map[engine.PDest]bool {
	if targ==nil { targ = make(map[engine.PDest]bool) }
	for p,ok := range add {
		if !ok { continue }
		targ[p] = true
	}
	return targ
}

type column struct {
	name ctsid
	dest engine.PDest
	col  engine.Column
}

func mkCol(id tsid,dest engine.PDest,c engine.Column) column {
	return column{id.c(c.Name),dest,c}
}
func mkCols(id tsid,dest engine.PDest,c []engine.Column) []column {
	r := make([]column,len(c))
	for i,cc := range c { r[i] = mkCol(id,dest,cc) }
	return r
}

type tsid [2]string
func mkTsid(s sqlparser.TableName) tsid { return tsid{s.Qualifier.String(),s.Name.String()} }

func (t tsid) q() string { return t[0] }
func (t tsid) c(s string) ctsid { return ctsid{t[0],t[1],s} }

func (t tsid) match(o tsid) bool {
	return t[1]==o[1] && ( (t[0]==o[0]) || (o[0]=="") )
}

type ctsid [3]string
func (c ctsid) q() tsid { return tsid{c[0],c[1]} }
func (c ctsid) match(o ctsid) bool {
	return c[2]==o[2] && (c.q().match(o.q()) || o.q()==tsid{"",""} )
}

//---

