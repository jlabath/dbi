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
func (db *H) Delete(s DBRowMarshaler, optionFunc StmtOption) error {
	qc := StmtContext{}
	if err := initStmContext(&qc, optionFunc); err != nil {
		return err
	}
	return delete(db.DB(), &qc, db.placeholder, db.lw, s)
}

func delete(conn connection, qc *StmtContext, phMaker func() placeHolderFunc, lw io.Writer, s DBRowMarshaler) error {
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
	_, err := conn.ExecContext(qc.context, buf.String(), pkey.Val)
	return err
}
