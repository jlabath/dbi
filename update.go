package dbi

import (
	"bytes"
	"fmt"
	"io"
)

//Update a record in SQL using the supplied data
func (db *H) Update(s DBRowUnmarshaler) error {
	return update(db.DB(), db.placeholder, db.lw, s)
}

func update(conn connection, phMaker func() placeHolderFunc, lw io.Writer, s DBRowUnmarshaler) error {
	phFunc := phMaker()
	row := s.DBRow()
	pkey := getPKFromColumns(row)
	if pkey == nil {
		return ErrNoPrimaryKey
	}
	args := make([]interface{}, 0, len(row))
	var buf bytes.Buffer
	buf.WriteString("UPDATE ")
	buf.WriteString(s.DBName())
	buf.WriteString(" SET ")
	for _, v := range row {
		if v.isPrimaryKey() {
			continue
		}
		if len(args) > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(v.Name)
		buf.WriteString("=")
		buf.WriteString(phFunc())
		args = append(args, v.Val)
	}
	buf.WriteString(" WHERE ")
	buf.WriteString(pkey.Name)
	buf.WriteString("=")
	buf.WriteString(phFunc())
	args = append(args, pkey.Val)
	fmt.Fprintln(lw, buf.String(), args)
	res, err := conn.Exec(buf.String(), args...)
	if err == nil {
		if n, err := res.RowsAffected(); err == nil && n == 0 {
			return ErrNotFound
		}
	}
	return err
}
