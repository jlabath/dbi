package dbi

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
)

//Insert a record into sql and return a Col with the primary key and any error
func (db *H) Insert(s DBRowMarshaler) (Col, error) {
	return insert(db.conn, db.dbType, db.placeholder, db.lw, s)
}

func insert(conn connection, dbType dbTyp, phMaker func() placeHolderFunc, lw io.Writer, s DBRowMarshaler) (Col, error) {
	var (
		buf   bytes.Buffer
		retPK Col
	)
	phFunc := phMaker()
	buf.WriteString("INSERT INTO ")
	buf.WriteString(s.DBName())
	buf.WriteString("(")
	row := s.DBRow()
	args := make([]interface{}, 0, len(row))
	for _, v := range row {
		if v.skipOnInsert() {
			continue
		}
		if len(args) > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(v.Name)
		args = append(args, v.Val)
	}
	buf.WriteString(")  VALUES (")
	for i := 0; i < len(args); i++ {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(phFunc())
	}
	buf.WriteString(")")
	sql := buf.String()
	if dbType == postgres {
		//postgres inserts should use returning
		return postgresInsert(conn, s, lw, sql, args)
	}
	fmt.Fprintln(lw, sql, args)
	result, err := conn.Exec(sql, args...)
	if err != nil {
		return retPK, err
	}
	retPK, err = lastInsertPKID(conn, phMaker, lw, s, result)
	if err != nil {
		return retPK, err
	}
	return retPK, err
}

func lastInsertPKID(tx connection, phMaker func() placeHolderFunc, lw io.Writer, s DBRowMarshaler, result sql.Result) (Col, error) {
	var (
		buf   bytes.Buffer
		retPK Col
	)
	phFunc := phMaker()
	//first let's make sure this even has a primary key
	row := s.DBRow()
	pk := getPKFromColumns(row)
	if pk == nil {
		return retPK, nil
	}
	//just return pk if not autoincrement
	if !pk.skipOnInsert() {
		return *pk, nil
	}
	retPK.Name = pk.Name
	//ok check if we can get it from result if driver implements this
	if liid, err := result.LastInsertId(); err == nil {
		cnvtLiid, err := forceToTypeOfVal(pk, liid)
		if err == nil {
			retPK.Val = cnvtLiid
			return retPK, nil
		}
		return retPK, err
	}
	buf.WriteString("SELECT ")
	buf.WriteString(pk.Name)
	buf.WriteString(" FROM ")
	buf.WriteString(s.DBName())
	buf.WriteString(" WHERE ")
	args := make([]interface{}, 0, len(row))
	for _, v := range row {
		if v.isPrimaryKey() || v.skipOnInsert() || v.isBinaryBlob() {
			continue
		}
		if len(args) > 0 {
			buf.WriteString(" AND ")
		}
		buf.WriteString(v.Name)
		buf.WriteString("=")
		buf.WriteString(phFunc())
		args = append(args, v.Val)
	}
	buf.WriteString(" ORDER BY ")
	buf.WriteString(pk.Name)
	buf.WriteString(" DESC ") //presumably order by highest first
	fmt.Fprintln(lw, buf.String(), args)
	rows, err := tx.Query(buf.String(), args...)
	if err != nil {
		return retPK, err
	}
	if rows.Next() {
		retPK.Val, err = deduceHowToScanVal(pk, rows)
	}
	err = rows.Close()
	return retPK, err
}

func postgresInsert(conn connection, s DBRowMarshaler, lw io.Writer, sql string, args []interface{}) (Col, error) {
	plainInsert := false
	//first let's make sure this even has a primary key
	row := s.DBRow()
	pk := getPKFromColumns(row)
	if pk == nil {
		plainInsert = true
	}

	if plainInsert {
		fmt.Fprintln(lw, sql, args)
		_, err := conn.Exec(sql, args...)
		return Col{}, err
	}

	//turn into returning query
	sql = fmt.Sprintf("%s RETURNING %s", sql, pk.Name)
	fmt.Fprintln(lw, sql, args)
	var liid int64
	if err := conn.QueryRow(sql, args...).Scan(&liid); err != nil {
		return *pk, err
	}
	cnvtLiid, err := forceToTypeOfVal(pk, liid)
	if err == nil {
		pk.Val = cnvtLiid
	}
	return *pk, err
}
