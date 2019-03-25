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

import "github.com/xwb1989/sqlparser/dependency/sqltypes"
import "github.com/xwb1989/sqlparser/dependency/querypb"
import "time"


// Go "time" format for Type_TIMESTAMP
const TimestampLayoutNano = "2006-01-02 15:04:05.999999999"

// Go "time" format for Type_TIMESTAMP
const timestampLayoutMicro = "2006-01-02 15:04:05.999999"

// Go "time" format for Type_DATETIME
const TimestampLayout = "2006-01-02 15:04:05"

// Go "time" format for Type_DATE
const DateLayout = "2006-01-02"

// Go "time" format for Type_TIME
const TimeLayout = "15:04:05"

// Go "time" format for Type_TIMESTAMP and Type_DATETIME and Type_DATE
// https://github.com/MariaDB/server/blob/mysql-5.5.36/sql-common/my_time.c#L124
var tsInputLayouts = []string{
	TimestampLayout,
	DateLayout,
	time.RFC3339Nano,
	time.RFC3339,
	"20060102150405",
	"20060102",
}

var tmInputLayouts = []string{
	TimeLayout,
}

func NewTimestamp(t time.Time) sqltypes.Value {
	return sqltypes.MakeTrusted(sqltypes.Timestamp,t.AppendFormat(nil, TimestampLayoutNano))
}
func NewTimestampCompat(t time.Time) sqltypes.Value {
	return sqltypes.MakeTrusted(sqltypes.Timestamp,t.AppendFormat(nil, timestampLayoutMicro))
}
func NewDatetime(t time.Time) sqltypes.Value {
	return sqltypes.MakeTrusted(sqltypes.Datetime,t.AppendFormat(nil, TimestampLayout))
}
func NewDate(t time.Time) sqltypes.Value {
	return sqltypes.MakeTrusted(sqltypes.Date,t.AppendFormat(nil, DateLayout))
}
func NewTime(t time.Time) sqltypes.Value {
	return sqltypes.MakeTrusted(sqltypes.Time,t.AppendFormat(nil, TimeLayout))
}

func ValueGotime(v querypb.Value) (t time.Time,err error) {
	var str string
	switch v.Type {
	case sqltypes.Timestamp,sqltypes.Datetime,sqltypes.Date:
		str = string(v.Value)
		for _,l := range tsInputLayouts {
			t,err = time.Parse(l, str)
			if err==nil { break }
		}
	case sqltypes.Time:
		str = string(v.Value)
		for _,l := range tmInputLayouts {
			t,err = time.Parse(l, str)
			if err==nil { break }
		}
	default:
		err = E_Type_Mismatch
	}
	return
}

