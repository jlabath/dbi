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
	db.DropTable(cp, nil)
	err := db.CreateTable(cp, nil)
	if err != nil {
		t.Fatalf("could not create Company table %s", err)
	}
	//insert a record
	cp.ID = 1
	cp.Name = "IBM"
	cp.Ticker = "IBM"
	dbclmn, err := db.Insert(cp, nil)
	if err != nil {
		t.Fatalf("could not insert a record %s", err)
	}
	//get a record
	c1 := Company{ID: dbclmn.Val.(int64)}
	err = db.Get(&c1, nil)
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
	err = db.Update(&c1, nil)

	if err != nil {
		t.Fatalf("could not update a record %s", err)
	}

	c2 := Company{ID: c1.ID}
	err = db.Get(&c2, nil)
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
	dbHandle := db.DB()
	if dbHandle == nil {
		t.Fatal("DB() handle should be non nil")
	}
}

func (s *BasicSuite) Test2InsertSelect(t *testing.T, db *H) {
	cp := &Company{}
	db.DropTable(cp, WithContext(context.Background()))
	err := db.CreateTable(cp, WithContext(context.Background()))
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
		_, err = db.Insert(cp, WithContext(context.Background()))
		if err != nil {
			t.Fatalf("Unable to insert %s", err)
		}
	}
	var results []*Company
	err = db.Select(&results, nil, "WHERE Ticker != @ticker ORDER BY ID", sql.Named("ticker", "INTC"))
	if err != nil {
		t.Fatalf("Unable to select %s", err)
	}
	for _, v := range results {
		if v.Ticker == "INTC" {
			t.Fatalf("Expected INTC must be missing from results")
		}
	}
	//now the same test with @ticker being last
	var results2 []*Company
	err = db.Select(&results, nil, "WHERE Ticker != @ticker", sql.Named("ticker", "INTC"))
	if err != nil {
		t.Fatalf("Unable to select %s", err)
	}
	for _, v := range results2 {
		if v.Ticker == "INTC" {
			t.Fatalf("Expected INTC must be missing from results")
		}
	}

}

func (s *BasicSuite) Test4AnnualReports(t *testing.T, db *H) {
	cp := Company{ID: 1}
	if err := db.Get(&cp, nil); err != nil {
		t.Fatal(err)
	}
	ar := &AnnualReport{
		CompanyID: cp.ID,
		Year:      2015,
		Sales:     big.NewInt(100000000000)}
	db.DropTable(ar, nil)
	err := db.CreateTable(ar, nil)
	if err != nil {
		t.Fatal(err)
	}
	pk, err := db.Insert(ar, nil)
	if err != nil {
		t.Fatal(err)
	}
	ar2 := &AnnualReport{ID: pk.Val.(int64)}
	err = db.Get(ar2, nil)
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
	err = db.Update(ar2, nil)
	if err != nil {
		t.Fatal(err)
	}

	ar3 := &AnnualReport{ID: ar2.ID}
	err = db.Get(ar3, nil)
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
	err = db.Select(&results, nil, "")
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
	db.DropTable(p, nil)
	err := db.CreateTable(p, nil)
	if err != nil {
		t.Fatal(err)
	}
	pk, err := db.Insert(p, nil)
	if err != nil {
		t.Fatal(err)
	}
	up := &Person{ID: pk.Val.(int)}
	err = db.Get(up, nil)
	if err != nil {
		t.Fatal(err)
	}
	if up.LastName != "Doe" {
		t.Fatal("LastName mismatch")
	}

	up.LastName = "Moe"
	err = db.Update(up, WithContext(context.Background()))
	if err != nil {
		t.Fatal(err)
	}

	var results []Person
	err = db.Select(&results, nil, "WHERE last = @last ORDER BY last", sql.Named("last", "Moe"))
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
	pk, err = db.Insert(p1, nil)
	if err != nil {
		t.Fatal(err)
	}

	conn := db.DB()
	if conn == nil {
		t.Fatal("DB() should not be nil")
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	qc := StmtContext{}
	initStmContext(&qc, nil)
	newpk, err := lastInsertPKID(tx.tx, &qc, tx.dbi.placeholder, tx.dbi.lw, p1, BustedResult{})
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
	err = db.Get(p1, nil)
	if err != nil {
		t.Fatal(err)
	}

	err = db.Delete(p1, WithContext(context.Background()))
	if err != nil {
		t.Fatal(err)
	}

	p2 := Person{ID: p1.ID}
	err = db.Get(&p2, nil)
	if err != ErrNotFound {
		t.Fatalf("want %v got %v", ErrNotFound, err)
	}
	p3 := Person{ID: 243}
	err = db.Get(&p3, nil)

	if err != ErrNotFound {
		t.Fatalf("want %v got %v", ErrNotFound, err)
	}

	p3.FirstName = "Steve"
	p3.LastName = "Blank"
	err = db.Update(&p3, nil)
	if err != ErrNotFound {
		t.Fatalf("want %v got %v", ErrNotFound, err)
	}
}

func (s *BasicSuite) Test6PersonNewDemo(t *testing.T, db *H) {
	p := &Person{
		FirstName: "John",
		LastName:  "Doe",
	}
	err := db.DropTable(p, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = db.CreateTable(p, nil)
	if err != nil {
		t.Fatal(err)
	}
	pk, err := db.Insert(p, nil)
	if err != nil {
		t.Fatal(err)
	}
	up := &Person{ID: pk.Val.(int)}
	err = db.Get(up, nil)
	if err != nil {
		t.Fatal(err)
	}
	if up.LastName != "Doe" {
		t.Fatalf("want %s got %s", "Doe", up.LastName)
	}
	up.LastName = "Moe"
	err = db.Update(up, WithContext(context.Background()))
	if err != nil {
		t.Fatal(err)
	}
	if !up.TimeStamp.IsZero() {
		t.Fatal("timestamp should be Zero")
	}
	var results []Person
	newF := func() DBRowUnmarshaler {
		return &Person{
			TimeStamp: time.Now(),
		}
	}
	ctx := context.Background()
	err = db.Select(
		&results,
		Compose(
			WithNewFunc(newF),
			WithContext(ctx),
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

func (s *BasicSuite) Test7InsertSelectTransaction(t *testing.T, db *H) {
	cp := &Company{}
	db.DropTable(cp, nil)
	err := db.CreateTable(cp, nil)
	if err != nil {
		t.Fatalf("Unable to create table %s", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
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
		_, err = tx.Insert(cp, nil)
		if err != nil {
			t.Fatalf("Unable to insert %s", err)
		}
	}
	var results []*Company
	err = tx.Select(&results, nil, "WHERE Ticker != @ticker ORDER BY ID", db.Named("ticker", "INTC"))
	if err != nil {
		t.Fatalf("Unable to select %s", err)
	}
	for _, v := range results {
		if v.Ticker == "INTC" {
			t.Fatalf("Expected INTC must be missing from results")
		}
	}
	err = tx.Commit()
	if err != nil {
		t.Error(err)
	}
	dbFromTx := tx.DBI()
	if db != dbFromTx {
		t.Error("DBI for transaction does not equal original DBI")
	}
}

func (s *BasicSuite) Test8PersonDemoInTransaction(t *testing.T, db *H) {

	p := &Person{
		FirstName: "John",
		LastName:  "Doe",
	}
	db.DropTable(p, nil)
	err := db.CreateTable(p, nil)
	if err != nil {
		t.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	pk, err := tx.Insert(p, nil)
	if err != nil {
		t.Fatal(err)
	}
	up := &Person{ID: pk.Val.(int)}
	err = tx.Get(up, nil)
	if err != nil {
		t.Fatal(err)
	}
	if up.LastName != "Doe" {
		t.Fatal("LastName mismatch")
	}

	up.LastName = "Moe"
	err = tx.Update(up, nil)
	if err != nil {
		t.Fatal(err)
	}

	var results []Person
	err = tx.Select(&results, nil, "WHERE last = @last ORDER BY last", sql.Named("last", "Moe"))
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
	pk, err = tx.Insert(p1, nil)
	if err != nil {
		t.Fatal(err)
	}

	conn := tx.DBI().DB()
	if conn == nil {
		t.Fatal("DB() should not be nil")
	}

	p1.ID = pk.Val.(int)
	err = tx.Get(p1, nil)
	if err != nil {
		t.Fatal(err)
	}

	err = tx.Delete(p1, nil)
	if err != nil {
		t.Fatal(err)
	}

	p2 := Person{ID: p1.ID}
	err = tx.Get(&p2, nil)
	if err != ErrNotFound {
		t.Fatalf("want %v got %v", ErrNotFound, err)
	}
	p3 := Person{ID: 243}
	err = tx.Get(&p3, nil)

	if err != ErrNotFound {
		t.Fatalf("want %v got %v", ErrNotFound, err)
	}

	p3.FirstName = "Steve"
	p3.LastName = "Blank"
	err = tx.Update(&p3, nil)
	if err != ErrNotFound {
		t.Fatalf("want %v got %v", ErrNotFound, err)
	}
	err = tx.Commit()
	if err != nil {
		t.Error(err)
	}

	//test rollback
	p4 := &Person{
		FirstName: "John",
		LastName:  "Milton",
	}
	tx, err = db.Begin()
	if err != nil {
		t.Error(err)
	}
	pk, err = tx.Insert(p4, nil)
	if err != nil {
		t.Fatal(err)
	}
	p5 := &Person{ID: pk.Val.(int)}
	err = tx.Get(p5, nil)
	if err != nil {
		t.Fatal(err)
	}
	if p5.LastName != "Milton" {
		t.Fatal("LastName mismatch")
	}
	err = tx.Rollback()
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()
	err = db.Get(p5, WithContext(ctx))
	if err == nil {
		t.Error("Expected error when retrieving rolled back data")
	}

}
