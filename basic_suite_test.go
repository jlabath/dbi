package dbi

import (
	"context"
	"database/sql"
	"errors"
	"math/big"
	"testing"
	"time"
)

type BasicSuite struct{}

func (s *BasicSuite) Name() string {
	return "BasicSuite"
}

func (s *BasicSuite) Test1Create(t *testing.T, db *H) {
	cp := &Company{}
	db.DropTable(cp)
	err := db.CreateTable(cp)
	if err != nil {
		t.Fatalf("could not create Company table %s", err)
	}
	//insert a record
	cp.ID = 1
	cp.Name = "IBM"
	cp.Ticker = "IBM"
	dbclmn, err := db.Insert(cp)
	if err != nil {
		t.Fatalf("could not insert a record %s", err)
	}
	//get a record
	c1 := Company{ID: dbclmn.Val.(int64)}
	err = db.Get(&c1)
	if err != nil {
		t.Fatalf("could not get a record %s", err)
	}
	if c1.ID != cp.ID {
		t.Fatalf("IDs do not equal")
	}
	if c1.Name != cp.Name {
		t.Fatalf("Name does not equal")
	}
	if c1.Ticker != cp.Ticker {
		t.Fatalf("Ticker does not equal")
	}
	//update
	c1.Name = "International Business Machines"
	err = db.Update(&c1)

	if err != nil {
		t.Fatalf("could not update a record %s", err)
	}

	c2 := Company{ID: c1.ID}
	err = db.Get(&c2)
	if err != nil {
		t.Fatalf("could not get a record %s", err)
	}
	if c1.ID != c2.ID {
		t.Fatalf("IDs do not equal")
	}
	if c1.Name != c2.Name {
		t.Fatalf("Name does not equal")
	}
	if c1.Ticker != c2.Ticker {
		t.Fatalf("Ticker does not equal")
	}

	//test DB
	dbHandle, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	if dbHandle == nil {
		t.Fatal("DB() handle should be non nil")
	}
}

func (s *BasicSuite) Test2InsertSelect(t *testing.T, db *H) {
	cp := &Company{}
	db.DropTable(cp)
	err := db.CreateTable(cp)
	if err != nil {
		t.Fatalf("Unable to create table %s", err)
	}
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
		if err != nil {
			t.Fatalf("Unable to insert %s", err)
		}
	}
	var results []*Company
	err = db.Select(&results, "WHERE Ticker != @ticker ORDER BY ID", sql.Named("ticker", "INTC"))
	if err != nil {
		t.Fatalf("Unable to select %s", err)
	}
	for _, v := range results {
		if v.Ticker == "INTC" {
			t.Fatalf("Expected INTC must be missing from results")
		}
	}
}

func (s *BasicSuite) Test4AnnualReports(t *testing.T, db *H) {
	cp := Company{ID: 1}
	if err := db.Get(&cp); err != nil {
		t.Fatal(err)
	}
	ar := &AnnualReport{
		CompanyID: cp.ID,
		Year:      2015,
		Sales:     big.NewInt(100000000000)}
	db.DropTable(ar)
	err := db.CreateTable(ar)
	if err != nil {
		t.Fatal(err)
	}
	pk, err := db.Insert(ar)
	if err != nil {
		t.Fatal(err)
	}
	ar2 := &AnnualReport{ID: pk.Val.(int64)}
	err = db.Get(ar2)
	if err != nil {
		t.Fatal(err)
	}
	//check that values match
	if ar2.CompanyID != cp.ID {
		t.Fatal("CompanyID mismatch")
	}
	if ar2.Year != 2015 {
		t.Fatal("year mismatch")
	}
	if ar2.Sales.String() != ar.Sales.String() {
		t.Fatal("sales string mismatch")
	}
	if ar2.NetIncome != nil {
		t.Fatal("NetIncome should be nil")
	}
	net := big.NewInt(50000000000)
	ar2.NetIncome = net
	err = db.Update(ar2)
	if err != nil {
		t.Fatal(err)
	}

	ar3 := &AnnualReport{ID: ar2.ID}
	err = db.Get(ar3)
	if err != nil {
		t.Fatal(err)
	}
	if ar3.CompanyID != cp.ID {
		t.Fatal("CompanyID mismatch")
	}
	if ar3.Year != 2015 {
		t.Fatal("year mismatch")
	}
	if ar3.Sales.String() != ar.Sales.String() {
		t.Fatal("sales string mismatch")
	}
	if ar3.NetIncome.String() != net.String() {
		t.Fatal("NetIncome mismatch")
	}

	//now run query on the reports
	var results []AnnualReport
	err = db.Select(&results, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatal("results should have 1 result")
	}
	ar4 := results[0]
	if ar4.CompanyID != cp.ID {
		t.Fatal("CompanyID mismatch")
	}
	if ar4.Year != 2015 {
		t.Fatal("year mismatch")
	}
	if ar4.Sales.String() != ar.Sales.String() {
		t.Fatal("sales string mismatch")
	}
	if ar4.NetIncome.String() != net.String() {
		t.Fatal("NetIncome mismatch")
	}
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

func (s *BasicSuite) Test5PersonDemo(t *testing.T, db *H) {
	p := &Person{
		FirstName: "John",
		LastName:  "Doe",
	}
	db.DropTable(p)
	err := db.CreateTable(p)
	if err != nil {
		t.Fatal(err)
	}
	pk, err := db.Insert(p)
	if err != nil {
		t.Fatal(err)
	}
	up := &Person{ID: pk.Val.(int)}
	err = db.Get(up)
	if err != nil {
		t.Fatal(err)
	}
	if up.LastName != "Doe" {
		t.Fatal("LastName mismatch")
	}

	up.LastName = "Moe"
	err = db.Update(up)
	if err != nil {
		t.Fatal(err)
	}

	var results []Person
	err = db.Select(&results, "WHERE last = @last ORDER BY last", sql.Named("last", "Moe"))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatal("results should have 1 result")
	}

	//test driver that does not support sql.Result
	p1 := &Person{
		FirstName: "A.T.",
		LastName:  "Tappman",
	}
	pk, err = db.Insert(p1)
	if err != nil {
		t.Fatal(err)
	}

	conn, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}

	tx, err := conn.Begin()
	if err != nil {
		t.Fatal(err)
	}

	newpk, err := db.lastInsertPKID(tx, p1, BustedResult{})
	if err != nil {
		t.Fatal(err)
	}

	if newpk.Val.(int) != pk.Val.(int) {
		t.Fatal("newpk pk mismatch")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	p1.ID = newpk.Val.(int)
	err = db.Get(p1)
	if err != nil {
		t.Fatal(err)
	}

	err = db.Delete(p1)
	if err != nil {
		t.Fatal(err)
	}

	p2 := Person{ID: p1.ID}
	err = db.Get(&p2)
	if err != ErrNotFound {
		t.Fatalf("want %v got %v", ErrNotFound, err)
	}
	p3 := Person{ID: 243}
	err = db.Get(&p3)

	if err != ErrNotFound {
		t.Fatalf("want %v got %v", ErrNotFound, err)
	}

	p3.FirstName = "Steve"
	p3.LastName = "Blank"
	err = db.Update(&p3)
	if err != ErrNotFound {
		t.Fatalf("want %v got %v", ErrNotFound, err)
	}
}

func (s *BasicSuite) Test6PersonNewDemo(t *testing.T, db *H) {
	p := &Person{
		FirstName: "John",
		LastName:  "Doe",
	}
	err := db.DropTable(p)
	if err != nil {
		t.Fatal(err)
	}
	err = db.CreateTable(p)
	if err != nil {
		t.Fatal(err)
	}
	pk, err := db.Insert(p)
	if err != nil {
		t.Fatal(err)
	}
	up := &Person{ID: pk.Val.(int)}
	err = db.Get(up)
	if err != nil {
		t.Fatal(err)
	}
	if up.LastName != "Doe" {
		t.Fatalf("want %s got %s", "Doe", up.LastName)
	}
	up.LastName = "Moe"
	err = db.Update(up)
	if err != nil {
		t.Fatal(err)
	}
	if !up.TimeStamp.IsZero() {
		t.Fatal("timestamp should be Zero")
	}
	var results []Person
	newF := func() RowUnmarshaler {
		return &Person{
			TimeStamp: time.Now(),
		}
	}
	ctx := context.Background()
	err = db.SelectOption(
		&results,
		WithQO(
			NewFuncQO(newF),
			WithContextQO(ctx),
		),
		"WHERE last = @last ORDER BY last",
		sql.Named("last", "Moe"))
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 1 {
		t.Fatal("results should have 1 result")
	}

	if results[0].TimeStamp.IsZero() {
		t.Fatal("timestamp should not be Zero")
	}
}
