package dbi

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

//ErrNoPrimaryKey is returned when the model does not have a column marked as PrimaryKey
var ErrNoPrimaryKey = errors.New("No primary key defined. Use PrimaryKey flag")

//Delete deletes a single row from db using the given models PrimaryKey
func (db *H) Delete(s DBRowMarshaler) error {
	return delete(db.DB(), db.placeholder, db.lw, s)
}

func delete(conn connection, phMaker func() placeHolderFunc, lw io.Writer, s DBRowMarshaler) error {
	row := s.DBRow()
	phFunc := phMaker()
	pkey := getPKFromColumns(row)
	if pkey == nil {
		return ErrNoPrimaryKey
	}
	var buf bytes.Buffer
	buf.WriteString("DELETE FROM ")
	buf.WriteString(s.DBName())
	buf.WriteString(" WHERE ")
	buf.WriteString(pkey.Name)
	buf.WriteString("=")
	buf.WriteString(phFunc())
	fmt.Fprintln(lw, buf.String(), pkey.Val)
	_, err := conn.Exec(buf.String(), pkey.Val)
	return err
}
