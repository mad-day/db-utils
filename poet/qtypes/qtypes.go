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


package qtypes

import "github.com/xwb1989/sqlparser/dependency/querypb"
import "github.com/xwb1989/sqlparser/dependency/sqltypes"
import "strconv"
import "errors"

var (
	E_Type_Mismatch = errors.New("type mismatch")
	E_Illegal_Value = errors.New("illegal value")
)

const (
	T_NONE = iota
	T_INT
	T_UINT
	T_FLOAT
	T_TEXT
	T_BINARY
	T_DECIMAL
	T_EXPRESSION
)

type Column struct {
	Type querypb.Type
	Name string
}

type Result struct {
	Columns []Column
	RowsAffected uint64
	InsertID     uint64
	Rows    [][]querypb.Value
}

func ValueQ2S(v sqltypes.Value) querypb.Value { return querypb.Value{Type:v.Type(),Value:v.Raw()} }
func ValueS2Q(v querypb.Value) sqltypes.Value { return sqltypes.MakeTrusted(v.Type,v.Value) }

func TypeSize(t querypb.Type) (bitSize, typ int) {
	bitSize = -1
	switch t {
	case sqltypes.Int8,sqltypes.Uint8: bitSize = 8
	case sqltypes.Int16,sqltypes.Uint16: bitSize = 16
	case sqltypes.Int32,sqltypes.Uint32,sqltypes.Float32: bitSize = 32
	case sqltypes.Int64,sqltypes.Uint64,sqltypes.Float64: bitSize = 64
	}
	switch {
	case t==sqltypes.Null: typ = T_NONE
	case sqltypes.IsSigned(t): typ = T_INT
	case sqltypes.IsUnsigned(t): typ = T_UINT
	case sqltypes.IsFloat(t): typ = T_FLOAT
	case sqltypes.IsText(t): typ = T_TEXT
	case sqltypes.IsBinary(t): typ = T_BINARY
	case t==sqltypes.Decimal: typ = T_DECIMAL
	case t==sqltypes.Expression: typ = T_EXPRESSION
	}
	return
}

func ValueInt(v querypb.Value) (int64,error) {
	if !sqltypes.IsSigned(v.Type) { return 0,E_Type_Mismatch }
	return strconv.ParseInt(string(v.Value), 10, 64 )
}
func ValueInt32(v querypb.Value) (int32,error) {
	if !sqltypes.IsSigned(v.Type) { return 0,E_Type_Mismatch }
	i,e := strconv.ParseInt(string(v.Value), 10, 32 )
	return int32(i),e
}
func ValueUint(v querypb.Value) (uint64,error) {
	if !sqltypes.IsUnsigned(v.Type) { return 0,E_Type_Mismatch }
	return strconv.ParseUint(string(v.Value), 10, 64 )
}
func ValueFloat(v querypb.Value) (float64,error) {
	if !sqltypes.IsFloat(v.Type) { return 0,E_Type_Mismatch }
	return strconv.ParseFloat(string(v.Value), 64 )
}
func ValueFloat32(v querypb.Value) (float32,error) {
	if !sqltypes.IsFloat(v.Type) { return 0,E_Type_Mismatch }
	f,e := strconv.ParseFloat(string(v.Value), 64 )
	return float32(f),e
}
func NewBool(b bool) sqltypes.Value {
	m := []byte{'0'}
	if b { m[0] = '1' }
	return sqltypes.MakeTrusted(sqltypes.Bit,m)
}
func ValueBool(v querypb.Value) (bool,error) {
	if v.Type==sqltypes.Bit {
		switch string(v.Value) {
		case "","0": return false,nil
		case "1": return true,nil
		}
		return v.Value[0]!='0',nil
	}
	if sqltypes.IsSigned(v.Type) {
		s,e := strconv.ParseInt(string(v.Value), 10, 64 )
		return s!=0,e
	}
	if sqltypes.IsUnsigned(v.Type) {
		u,e := strconv.ParseUint(string(v.Value), 10, 64 )
		return u!=0,e
	}
	
	return false,E_Type_Mismatch
}

