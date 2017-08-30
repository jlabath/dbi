package dbi

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"reflect"
)

//Select runs an SQL query and populates dst and returns an error if any.
//It uses the supplied dst to deduce original type to be able to call DBRow(), DBName() etc.
//The where is any where/order by/limit type of clause - if empty it will simply do SELECT col1,col2,... FROM table_name
//args are any params to be used in the SQL query to replace ?
//It expects dst to be a pointer to a slice of RowUnmarshaler(s), and it will return an error if it is not.
func (db *H) Select(dst interface{}, where string, args ...sql.NamedArg) error {
	return db.SelectOption(dst, nil, where, args...)
}

//SelectOption is functionaly the same as Select however by allowing the user to pass options it is possible to perform
//additional initializations before the DBName, DBRow, or DBScan are even called.
//it's also possible to provide context to allow cancellable queries introduced in go 1.8
func (db *H) SelectOption(
	dst interface{},
	optionFunc StmtOption,
	where string,
	args ...sql.NamedArg) error {
	qc := StmtContext{}
	if optionFunc != nil {
		if err := optionFunc(&qc); err != nil {
			return err
		}
	}
	return selectQuery(db.conn, db.placeholder, db.namedArgPrefix, db.lw, dst, &qc, where, args...)
}

func selectQuery(
	conn connection,
	placeholderMaker func() placeHolderFunc,
	namedArgPrefix rune,
	lw io.Writer,
	dst interface{},
	qc *StmtContext,
	where string,
	args ...sql.NamedArg) error {
	var (
		buf          bytes.Buffer
		btIsPointer  bool
		baseBaseType reflect.Type
	)
	//first reflect base type from dst
	baseType, err := reflectBaseType(dst)
	if err != nil {
		return err
	}
	//now figure out if baseType is pointer since we support both
	if baseType.Kind() == reflect.Ptr {
		btIsPointer = true
		baseBaseType = baseType.Elem()
	} else {
		baseBaseType = baseType
	}
	//crt new pointer to baseBaseType or call newFunc if not nil
	var newValue reflect.Value
	if qc.newFunc == nil {
		newValue = reflect.New(baseBaseType)
	} else {
		newValue = reflect.ValueOf(qc.newFunc())
	}
	source, isUnmarshaler := newValue.Interface().(DBRowUnmarshaler)
	if !isUnmarshaler {
		return ErrNoUnmarshaler
	}
	row := source.DBRow()
	buf.WriteString("SELECT ")
	for i, v := range row {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(v.Name)
	}
	buf.WriteString(" FROM ")
	buf.WriteString(source.DBName())
	buf.WriteString(" ")
	buf.WriteString(where)
	//we are done with assembling the query
	query := buf.String()
	//now translate the query from named format to serial one
	query, keywords, err := produceQuery(
		namedArgPrefix,
		placeholderMaker(),
		query)
	if err != nil {
		return err
	}

	//populate the args
	qargs, err := mapNamedArgsToValues(keywords, args)
	if err != nil {
		return err
	}
	//log the query to logger
	fmt.Fprintln(lw, query, qargs)
	//execute
	var rows *sql.Rows
	if qc.context != nil {
		rows, err = conn.QueryContext(qc.context, query, qargs...)
	} else {
		rows, err = conn.Query(query, qargs...)
	}
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	dstv := reflect.ValueOf(dst).Elem()
	for rows.Next() {
		var rowScn DBScanner
		if qc.newFunc == nil {
			rowScn = reflect.New(baseBaseType).Interface().(DBScanner)
		} else {
			rowScn = qc.newFunc()
		}
		err = rowScn.DBScan(rows)
		if err != nil {
			return err
		}
		vToAppend := reflect.ValueOf(rowScn)
		if !btIsPointer {
			vToAppend = vToAppend.Elem()
		}
		dstv.Set(reflect.Append(dstv, vToAppend))
	}
	return nil
}

func mapNamedArgsToValues(keywords []string, args []sql.NamedArg) ([]interface{}, error) {
	kmap := make(map[string]interface{})
	for _, v := range args {
		kmap[v.Name] = v.Value
	}
	qargs := make([]interface{}, 0, len(keywords))
	for _, keyword := range keywords {
		v, ok := kmap[keyword]
		if !ok {
			return qargs, fmt.Errorf(
				"No such named keyword argument: %s", keyword)
		}
		qargs = append(qargs, v)
	}
	return qargs, nil
}
