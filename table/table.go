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


package table

import "fmt"

func toString(i interface{}) string {
	switch i.(type){
	case []byte: return fmt.Sprintf("B%q",i)
	case string: return fmt.Sprintf("%q",i)
	}
	return fmt.Sprint(i)
}

type ColumnFilter struct {
	Index int
	Operator string
	Value,Escape interface{}
}
func (c *ColumnFilter) Err(code ErrCode,cols []string) (se ScanError) {
	se.ErrCode = code
	se.Operator = c.Operator
	// More...?
	
	se.FieldIndex = c.Index
	if len(cols)>c.Index {
		se.FieldString = cols[c.Index]
	}
	return
}
func (c ColumnFilter) String() string {
	if c.Escape == nil {
		return fmt.Sprintf("{$%d %s %s}",c.Index,c.Operator,toString(c.Value))
	}
	return fmt.Sprintf("{$%d %s %s escape %s}",c.Index,c.Operator,toString(c.Value),toString(c.Escape))
}

type ColumnOrder struct {
	Index int
	Desc  bool
}
func (c *ColumnOrder) Operator() string {
	if c.Desc { return "desc" }
	return "asc"
}
func (c *ColumnOrder) Err(code ErrCode,cols []string) (se ScanError) {
	se.ErrCode = code
	if c.Desc {
		se.Operator = "desc"
	} else {
		se.Operator = "asc"
	}
	se.FieldIndex = c.Index
	if len(cols)>c.Index {
		se.FieldString = cols[c.Index]
	}
	return
}
func (c ColumnOrder) String() string {
	s := "asc"
	if c.Desc { s = "desc" }
	return fmt.Sprintf("$%d %s",c.Index,s)
}

type TableScan struct {
	Filter []ColumnFilter
	Order  []ColumnOrder
}

/*
This interface represents a Scannable Table.

	var tab table.Table
	cols := []int{0,1,2,3}
	vals := make([]interface{},4)
	
	iter,_ := tab.TableScan(cols,new(table.TableScan))
	defer iter.Close()
	for {
		err := iter.Next(cols,vals)
		if err != nil {
			break
		}
	}
*/
type Table interface {
	Columns() []string
	
	// 'cols' must be specified, an the same array must be used in
	// any .Next() call onto the returned TableIterator object.
	// Otherwise the behavoir is undefined.
	// 
	// If the *TableScan object is not acceptible, return an error,
	// preferribly a ScanError{} 
	TableScan(cols []int,meta *TableScan) (TableIterator,error)
}

type TableIterator interface {
	Close() error
	// Scan the next row. Return nil on success, io.EOF on end-of-table
	// and any other error if there is a true error.
	Next(cols []int,vals []interface{}) error
}

type TableInsert struct {
	AllCols bool
	Cols []int
	
	Values [][]interface{}
	Op TableOp
	
	OndupCols []int
	OndupVals []interface{}
}
type TableInsertStmt interface {
	Close() error
	Abort() error
	
	TableInsert(ti *TableInsert) error
}

type InsertableTable interface {
	Table
	
	TablePrepareInsert(ti *TableInsert) (TableInsertStmt,error)
}

type TableUpdate struct {
	Op  TableOp
	Scan *TableScan
	
	UpdCols []int
	UpdVals []interface{}
}

type TableUpdateStmt interface {
	Close() error
	Abort() error
	
	TableUpdate(tu *TableUpdate) error
}

type UpdateableTable interface {
	Table
	
	TablePrepareUpdate(tu *TableUpdate) (TableInsertStmt,error)
}

type TableOp int
const (
	// Insert Job
	T_Insert TableOp = iota
	T_InsertIgnore
	T_Replace
	
	// Update/Delete Job
	T_Update
	T_Delete
)


type ErrCode int
const (
	E_FILTER_FIELD_UNSUPP ErrCode = iota
	E_FILTER_OPERATOR_UNSUPP
	E_FILTER_OPERATOR_FIELD
	E_ORDERBY_FIELD
	E_ORDERBY_ORDER_FIELD
	E_ORDERBY_ORDER
)
var ErrCode_names = map[ErrCode]string{
	E_FILTER_FIELD_UNSUPP:    "filter: field not supported for filtering",
	E_FILTER_OPERATOR_UNSUPP: "filter: operator not supported for filtering",
	E_FILTER_OPERATOR_FIELD:  "filter: operator not supported on field",
	E_ORDERBY_FIELD:          "order_by: ordering not supported on field",
	E_ORDERBY_ORDER_FIELD:    "order_by: [asc|desc] not supported on field",
	E_ORDERBY_ORDER:          "order_by: [asc|desc] generally unsupported",
}

type ScanError struct {
	ErrCode
	Operator string
	FieldIndex  int
	FieldString string
}
func (c ScanError) Error() string {
	str := c.FieldString
	if str=="" { str = fmt.Sprintf("$%d",c.FieldIndex) }
	msg := ErrCode_names[c.ErrCode]
	if msg=="" { msg = fmt.Sprintf("gen: Code_%02d",int(c.ErrCode)) }
	return msg+": "+c.Operator+" "+str
}

