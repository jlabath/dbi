package dbi

import (
	"database/sql"
	"errors"
	"math/big"
	"os"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	. "gopkg.in/check.v1"
)

type cWriter struct {
	c *C
}

func (w cWriter) Write(data []byte) (int, error) {
	w.c.Log(strings.TrimSpace(string(data)))
	return len(data), nil
}

//hookup gocheck
func Test(t *testing.T) { TestingT(t) }

type BasicSuite struct {
	testDB string
	conn   *sql.DB
}

var _ = Suite(&BasicSuite{"basic_suite.db", nil})

func (s *BasicSuite) SetUpSuite(c *C) {
	var err error
	s.conn, err = sql.Open("sqlite3", s.testDB)
	c.Assert(err, IsNil)
}

func (s *BasicSuite) TearDownSuite(c *C) {
	s.conn.Close()
	os.Remove(s.testDB)
}

func (s *BasicSuite) Test1Create(c *C) {
	db := New(s.conn, cWriter{c})
	cp := &Company{}
	db.DropTable(cp)
	err := db.CreateTable(cp)
	c.Assert(err, IsNil)
	//insert a record
	cp.ID = 1
	cp.Name = "IBM"
	cp.Ticker = "IBM"
	dbclmn, err := db.Insert(cp)
	c.Assert(err, IsNil)
	//get a record
	c1 := Company{ID: dbclmn.Val.(int64)}
	err = db.Get(&c1)
	c.Assert(err, IsNil)
	c.Assert(c1.ID, Equals, cp.ID)
	c.Assert(c1.Name, Equals, cp.Name)
	c.Assert(c1.Ticker, Equals, cp.Ticker)
	//update
	c1.Name = "International Business Machines"
	err = db.Update(&c1)
	c.Assert(err, IsNil)
	c2 := Company{ID: c1.ID}
	err = db.Get(&c2)
	c.Assert(err, IsNil)
	c.Assert(c2.ID, Equals, c1.ID)
	c.Assert(c2.Name, Equals, c1.Name)
	c.Assert(c2.Ticker, Equals, c1.Ticker)
}

func (s *BasicSuite) Test2InsertSelect(c *C) {
	db := New(s.conn, cWriter{c})
	cp := &Company{}
	db.DropTable(cp)
	err := db.CreateTable(cp)
	c.Assert(err, IsNil)
	sample := [][]string{
		{"Red Hat", "RHT"},
		{"Intel", "INTC"},
		{"Google", "GOOG"},
		{"IBM", "IBM"},
		{"Oracle Corporation", "ORCL"},
	}
	for _, v := range sample {
		cp.Name = v[0]
		cp.Ticker = v[1]
		_, err = db.Insert(cp)
		c.Assert(err, IsNil)
	}
	results, err := db.Select(cp, "WHERE Ticker != ? ORDER BY ID", "INTC")
	c.Assert(err, IsNil)
	for _, v := range results {
		v := v.(*Company)
		c.Assert(v.Ticker, Not(Equals), "INTC")
	}
}

func (s *BasicSuite) Test3NewDBAndUtility(c *C) {
	const foo = "foo.db"
	conn, err := sql.Open("sqlite3", foo)
	c.Assert(err, IsNil)
	db := New(conn, nil)
	c.Assert(db.conn, Equals, conn)
	cp := &Company{}
	err = db.CreateTable(cp)
	c.Assert(err, IsNil)
	err = conn.Close()
	c.Assert(err, IsNil)
	err = os.Remove(foo)
	c.Assert(err, IsNil)
}

func (s *BasicSuite) Test4AnnualReports(c *C) {
	db := New(s.conn, cWriter{c})
	cp := Company{ID: 1}
	err := db.Get(&cp)
	c.Assert(err, IsNil)
	ar := &AnnualReport{
		CompanyID: cp.ID,
		Year:      2015,
		Sales:     big.NewInt(100000000000)}
	err = db.CreateTable(ar)
	c.Assert(err, IsNil)
	pk, err := db.Insert(ar)
	c.Assert(err, IsNil)
	c.Assert(pk, FitsTypeOf, Col{})
	ar2 := &AnnualReport{ID: pk.Val.(int64)}
	err = db.Get(ar2)
	c.Assert(err, IsNil)
	//check that values match
	c.Assert(ar2.CompanyID, Equals, cp.ID)
	c.Assert(ar2.Year, Equals, 2015)
	c.Assert(ar2.Sales.String(), Equals, ar.Sales.String())
	c.Assert(ar2.NetIncome, IsNil)
	net := big.NewInt(50000000000)
	ar2.NetIncome = net
	err = db.Update(ar2)
	c.Assert(err, IsNil)
	ar3 := &AnnualReport{ID: ar2.ID}
	err = db.Get(ar3)
	c.Assert(err, IsNil)
	c.Assert(ar3.CompanyID, Equals, cp.ID)
	c.Assert(ar3.Year, Equals, 2015)
	c.Assert(ar3.Sales.String(), Equals, ar.Sales.String())
	c.Assert(ar3.NetIncome.String(), Equals, net.String())
	//now run query on the reports
	results, err := db.Select(ar, "")
	c.Assert(err, IsNil)
	c.Assert(results, HasLen, 1)
	ar4 := results[0].(*AnnualReport)
	c.Assert(ar4.CompanyID, Equals, cp.ID)
	c.Assert(ar4.Year, Equals, 2015)
	c.Assert(ar4.Sales.String(), Equals, ar.Sales.String())
	c.Assert(ar4.NetIncome.String(), Equals, net.String())
}

var ErrBusted = errors.New("busted")

type BustedResult struct {
}

func (b BustedResult) LastInsertId() (int64, error) {
	return 0, ErrBusted
}

func (b BustedResult) RowsAffected() (int64, error) {
	return 0, ErrBusted
}

func (s *BasicSuite) Test5PersonDemo(c *C) {
	db := New(s.conn, cWriter{c})
	p := &Person{
		FirstName: "John",
		LastName:  "Doe",
	}
	err := db.CreateTable(p)
	c.Assert(err, IsNil)
	pk, err := db.Insert(p)
	c.Assert(err, IsNil)
	c.Assert(pk, FitsTypeOf, Col{})
	up := &Person{ID: pk.Val.(int)}
	err = db.Get(up)
	c.Assert(err, IsNil)
	c.Assert(up.LastName, Equals, "Doe")
	up.LastName = "Moe"
	err = db.Update(up)
	c.Assert(err, IsNil)
	results, err := db.Select(p, "WHERE last = ? ORDER BY last", "Moe")
	c.Assert(err, IsNil)
	c.Assert(results, HasLen, 1)
	//test driver that does not support sql.Result
	p1 := &Person{
		FirstName: "A.T.",
		LastName:  "Tappman",
	}
	pk, err = db.Insert(p1)
	c.Assert(err, IsNil)
	tx, err := s.conn.Begin()
	c.Assert(err, IsNil)
	newpk, err := db.lastInsertPKID(tx, p1, BustedResult{})
	c.Assert(err, IsNil)
	c.Assert(newpk.Val.(int), Equals, pk.Val.(int))
	c.Assert(tx.Commit(), IsNil)
}
