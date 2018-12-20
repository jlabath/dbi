package dbi

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
)

//ErrNotFound returned when the row with the given primary key was not found
var ErrNotFound = errors.New("Record with given primary key not found")

func initStmContext(qc *StmtContext, optionFunc StmtOption) error {
	if optionFunc != nil {
		if err := optionFunc(qc); err != nil {
			return err
		}
	}
	//must have context
	if qc.context == nil {
		qc.context = context.Background()
	}
	return nil
}

//Get a record from SQL using the supplied PrimaryKey
func (db *H) Get(s DBRowUnmarshaler, optionFunc StmtOption) error {
	qc := StmtContext{}
	if err := initStmContext(&qc, optionFunc); err != nil {
		return err
	}
	return get(db.DB(), &qc, db.placeholder, db.lw, s)
}

//Get a record from SQL using the supplied PrimaryKey
func get(conn connection, qc *StmtContext, phMaker func() placeHolderFunc, lw io.Writer, s DBRowUnmarshaler) error {
	phFunc := phMaker()
	row := s.DBRow()
	pkey := getPKFromColumns(row)
	if pkey == nil {
		return ErrNoPrimaryKey
	}
	var buf bytes.Buffer
	buf.WriteString("SELECT ")
	for i, v := range row {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(v.Name)
	}
	buf.WriteString(" FROM ")
	buf.WriteString(s.DBName())
	buf.WriteString(" WHERE ")
	buf.WriteString(pkey.Name)
	buf.WriteString("=")
	buf.WriteString(phFunc())
	fmt.Fprintln(lw, buf.String(), pkey.Val)
	dbrow := conn.QueryRowContext(qc.context, buf.String(), pkey.Val)
	err := s.DBScan(dbrow)
	if err == sql.ErrNoRows {
		return ErrNotFound
	}
	return err
}
