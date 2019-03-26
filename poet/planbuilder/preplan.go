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

type preNode interface{
	iNode()
}
type preColumns interface{
	preNode
	iCols() []column
}

type tablesExpr struct {
	cols   []column
	tables []*engine.Table
	
	// sqlparser.TableExprs, sqlparser.TableExpr or sqlparser.SimpleTableExpr
	sql    sqlparser.SQLNode
	single bool
	dest   engine.PDest
}
func (*tablesExpr) iNode() {}
func (t *tablesExpr) iCols() []column { return t.cols }

