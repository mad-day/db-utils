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


package util

import (
	"reflect"
	"fmt"
	"time"
)

func GetKey(dest interface{}) ([]byte,error){
	switch v := dest.(type) {
	case *string: return []byte(*v),nil
	case *[]byte: return *v,nil
	default: return nil,fmt.Errorf("Invalid assignment %T <- []byte",dest)
	}
	panic("unreachable")
}

func SetInKey(dest interface{}, val []byte) error{
	switch v := dest.(type) {
	case *string: *v = string(val)
	case *[]byte: *v = val
	default: return fmt.Errorf("Invalid assignment %T <- []byte",dest)
	}
	return nil
}
func SetInPtr(dest, val interface{}) error {
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

func GetPtr(ptr interface{}) interface{} {
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

type ValueType uint

const (
	VT_INT   ValueType = iota
	VT_FLOAT
	VT_BOOL
	VT_BYTES
	VT_STRING
	VT_TIMESTAMP
)

var vt_map = map[ValueType]reflect.Type {
	VT_INT: reflect.TypeOf(int64(0)),
	VT_FLOAT: reflect.TypeOf(float64(0)),
	VT_BOOL: reflect.TypeOf(false),
	VT_BYTES: reflect.TypeOf([]byte{}),
	VT_STRING: reflect.TypeOf(""),
	VT_TIMESTAMP: reflect.TypeOf(time.Time{}),
}
func (v ValueType) Type() reflect.Type {
	t,ok := vt_map[v]
	if !ok { panic("invalid/undefined ValueType") }
	return t
}

