package dbi

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
)

//ErrNotFound returned when the row with the given primary key was not found
var ErrNotFound = errors.New("Record with given primary key not found")

//Get a record from SQL using the supplied PrimaryKey
func (db *H) Get(s DBRowUnmarshaler) error {
	return get(db.DB(), db.placeholder, db.lw, s)
}

//Get a record from SQL using the supplied PrimaryKey
func get(conn connection, phMaker func() placeHolderFunc, lw io.Writer, s DBRowUnmarshaler) error {
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
	dbrow := conn.QueryRow(buf.String(), pkey.Val)
	err := s.DBScan(dbrow)
	if err == sql.ErrNoRows {
		return ErrNotFound
	}
	return err
}
