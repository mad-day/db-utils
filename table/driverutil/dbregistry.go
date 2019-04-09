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
	"sync"
	"errors"
)

var ENoDatabase = errors.New("ENoDatabase")

type DbRegistry struct {
	Rwm sync.RWMutex
	Map map[string]*Database
}
func (db *DbRegistry) Open(name string) (driver.Conn, error) {
	db.Rwm.RLock(); defer db.Rwm.RUnlock()
	n := db.Map[name]
	if n==nil { return nil,ENoDatabase }
	return n,nil
}
func (db *DbRegistry) OpenConnector(name string) (driver.Connector, error) {
	db.Rwm.RLock(); defer db.Rwm.RUnlock()
	n := db.Map[name]
	if n==nil { return nil,ENoDatabase }
	return &DatabaseContext{db,n},nil
}
func (db *DbRegistry) RegisterDb(name string,base *Database) {
	db.Rwm.Lock(); defer db.Rwm.Unlock()
	if db.Map==nil { db.Map = make(map[string]*Database) }
	db.Map[name] = base
}

