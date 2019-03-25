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


package plan

import "github.com/xwb1989/sqlparser/dependency/querypb"
import "github.com/mad-day/db-utils/poet/qtypes"

type InstID interface{
	IsInstID()
}

type DefInstID struct{}
func (*DefInstID) IsInstID() {}

type PCursor interface {
	ExecuteQuery(inst InstID, query string, bindvars map[string]*querypb.BindVariable,callback func(res *qtypes.Result) error) error
}

type Primitive interface {
	ExecuteQuery(cur PCursor, bindvars map[string]*querypb.BindVariable,callback func(res *qtypes.Result) error) error
}

