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


package sqlfill

import "github.com/xwb1989/sqlparser"
import "strings"

type LiteCatalog interface{
	LookupTable(table sqlparser.TableName) []string
}

type LiteTN [2]string
func (l LiteTN) Lower() LiteTN {
	return LiteTN{strings.ToLower(l[0]),strings.ToLower(l[1])}
}
func toLiteTN(tn sqlparser.TableName) LiteTN {
	return LiteTN{tn.Qualifier.String(),tn.Name.String()}
}


type CatalogImpl struct {
	CaseSensitive bool
	DefaultSchema string
	Map map[LiteTN][]string
}
func (c *CatalogImpl) Init() {
	c.Map = make(map[LiteTN][]string)
}
func (c CatalogImpl) conv(t LiteTN) LiteTN {
	if t[0]=="" { t[0] = c.DefaultSchema }
	if c.CaseSensitive { return t }
	return t.Lower()
}
func (c CatalogImpl) LookupTable(table sqlparser.TableName) []string {
	return c.Map[c.conv(toLiteTN(table))]
}
func (c CatalogImpl) PutTable0(tn string,cols []string) {
	c.Map[c.conv(LiteTN{"",tn})] = cols
}
func (c CatalogImpl) PutTable(tn LiteTN,cols []string) {
	c.Map[c.conv(tn)] = cols
}

