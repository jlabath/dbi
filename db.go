package dbi

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
)

func guessSQLType(c Col) string {
	if c.Opt != nil && c.Opt.Type != "" {
		return c.Opt.Type
	}
	switch c.Val.(type) {
	case int, uint, int64, int32, int16, int8, uint64, uint32, uint16, uint8:
		return "int"
	default:
		return "varchar(255)"
	}
}

func getPKFromColumns(cols []Col) *Col {
	for _, v := range cols {
		if v.isPrimaryKey() {
			return &v
		}
	}
	return nil
}

func deduceHowToScanVal(col *Col, src Scanner) (interface{}, error) {
	switch col.Val.(type) {
	case int:
		var v int
		err := src.Scan(&v)
		return v, err
	case int64:
		var v int64
		err := src.Scan(&v)
		return v, err
	case int32:
		var v int32
		err := src.Scan(&v)
		return v, err
	case int16:
		var v int16
		err := src.Scan(&v)
		return v, err
	case int8:
		var v int8
		err := src.Scan(&v)
		return v, err
	case uint:
		var v uint
		err := src.Scan(&v)
		return v, err
	case uint64:
		var v uint64
		err := src.Scan(&v)
		return v, err
	case uint32:
		var v uint32
		err := src.Scan(&v)
		return v, err
	case uint16:
		var v uint16
		err := src.Scan(&v)
		return v, err
	case uint8:
		var v uint8
		err := src.Scan(&v)
		return v, err
	default:
		var v string
		err := src.Scan(&v)
		return v, err
	}
}

//ErrPrimaryKeyOverflow is returned sql.Result.LastInsertId overflows the declared int type
var ErrPrimaryKeyOverflow = errors.New("Last insert ID returned by database overflows model's type")

func doesIntMatch(someint interface{}, orig int64) bool {
	switch someint := someint.(type) {
	case int64:
		return someint == orig
	case int:
		return int64(someint) == orig
	case int32:
		return int64(someint) == orig
	case int16:
		return int64(someint) == orig
	case int8:
		return int64(someint) == orig
	case uint:
		return int64(someint) == orig
	case uint64:
		return int64(someint) == orig
	case uint32:
		return int64(someint) == orig
	case uint16:
		return int64(someint) == orig
	case uint8:
		return int64(someint) == orig
	}
	return false
}

func forceToTypeOfVal(col *Col, liid int64) (interface{}, error) {
	chk := func(i interface{}) error {
		if !doesIntMatch(i, liid) {
			return ErrPrimaryKeyOverflow
		}
		return nil
	}
	switch col.Val.(type) {
	case int:
		v := int(liid)
		return v, chk(v)
	case int64:
		return liid, nil
	case int32:
		v := int32(liid)
		return v, chk(v)
	case int16:
		v := int16(liid)
		return v, chk(v)
	case int8:
		v := int8(liid)
		return v, chk(v)
	case uint:
		v := uint(liid)
		return v, chk(v)
	case uint64:
		v := uint64(liid)
		return v, chk(v)
	case uint32:
		v := uint32(liid)
		return v, chk(v)
	case uint16:
		v := uint16(liid)
		return v, chk(v)
	case uint8:
		v := uint8(liid)
		return v, chk(v)
	default:
		err := fmt.Errorf("Expected integer type for Val of PrimaryKey but got %T", col.Val)
		return nil, err
	}
}

//ErrNoPointerToSlice is returned if dst argument to Select is not a pointer to slice.
var ErrNoPointerToSlice = errors.New("Expected dst to be a pointer to a slice")

//ErrNoUnmarshaler is returned when element of slice dst does not implement RowUnmarshaler
var ErrNoUnmarshaler = errors.New("Elements of slice dst do not implement DBRowUnmarshaler")

func reflectBaseType(s interface{}) (reflect.Type, error) {
	//need to reflect and make it
	typ := reflect.TypeOf(s)
	if typ.Kind() != reflect.Ptr {
		return nil, ErrNoPointerToSlice
	}
	//typ of slice
	typ = typ.Elem()
	if typ.Kind() != reflect.Slice {
		return nil, ErrNoPointerToSlice
	}
	//get base type from slice
	return typ.Elem(), nil
}

//place holder functions
type placeHolderFunc func() string

func defaultPlaceHolder() placeHolderFunc {
	return func() string { return "?" }
}

func pgPlaceHolder() placeHolderFunc {
	var count int
	return func() string {
		count++
		return fmt.Sprintf("$%d", count)
	}
}

type dbTyp int

const (
	sqlite dbTyp = iota
	postgres
	mysql
)

//H is our handle supporting Insert/Get/Update to be used by client
type H struct {
	conn           *sql.DB
	lw             io.Writer
	placeholder    func() placeHolderFunc
	dbType         dbTyp
	namedArgPrefix rune
}

func newH(conn *sql.DB) *H {
	return &H{
		conn:           conn,
		lw:             ioutil.Discard,
		placeholder:    defaultPlaceHolder,
		namedArgPrefix: '@',
	}
}

//New return a new DB handle
func New(conn *sql.DB, options ...func(*H) error) (*H, error) {
	h := newH(conn)
	for _, opt := range options {
		if opt == nil {
			continue
		}
		if err := opt(h); err != nil {
			return h, err
		}
	}
	return h, nil
}

//DB returns the underlying sql.DB connection handle
func (db *H) DB() *sql.DB {
	return db.conn
}

//CreateTable executes CREATE TABLE as per DBRow()
func (db *H) CreateTable(source DBRowMarshaler) error {
	var buf bytes.Buffer
	buf.WriteString("CREATE TABLE ")
	buf.WriteString(source.DBName())
	buf.WriteString(" (")
	for idx, c := range source.DBRow() {
		if idx > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(c.Name)
		buf.WriteString(" ")
		buf.WriteString(guessSQLType(c))
	}
	buf.WriteString(")")
	_, err := db.conn.Exec(buf.String())
	fmt.Fprintln(db.lw, buf.String())
	return err
}

//DropTable executes DROP TABLE
func (db *H) DropTable(source DBNamer) error {
	var buf bytes.Buffer
	buf.WriteString("DROP TABLE ")
	buf.WriteString(source.DBName())
	fmt.Fprintln(db.lw, buf.String())
	_, err := db.conn.Exec(buf.String())
	return err
}

//Named is just a convenience method to avoid the need to import database/sql
func (db *H) Named(n string, v interface{}) sql.NamedArg {
	return sql.Named(n, v)
}

//ColOptFlag for storing meta information about parent Col
type ColOptFlag uint16

const (
	//NoInsert means do not include this column on inserts
	NoInsert ColOptFlag = 1 << (16 - 1 - iota)
	//PrimaryKey marks this column as primary key
	PrimaryKey
)

//ColOpt is struct for optional meta information
//e.g. to mark Field as PrimaryKey or to use custom type for TableCreate
type ColOpt struct {
	Type  string     // type to use when CREATE TABLE is called e.g. text, blob
	Flags ColOptFlag // meta information about the column such as PrimaryKey
}

//Col is our basic structure consisting of Name,Val pair and optional Type and Flags attributes
type Col struct {
	Name string      // column name in DB
	Val  interface{} // value to store in DB
	Opt  *ColOpt     // options
}

//NewCol returns a new Col object
func NewCol(name string, val interface{}, opt *ColOpt) Col {
	return Col{Name: name, Val: val, Opt: opt}
}

func (d Col) skipOnInsert() bool {
	if d.Opt == nil {
		return false
	}
	return NoInsert == d.Opt.Flags&NoInsert
}

func (d Col) isPrimaryKey() bool {
	if d.Opt == nil {
		return false
	}
	return PrimaryKey == d.Opt.Flags&PrimaryKey
}

func (d Col) isBinaryBlob() bool {
	_, fact := d.Val.([]byte)
	return fact
}

//DBNamer is anything that can tells us its table name
type DBNamer interface {
	DBName() string
}

//DBRowMarshaler composed of needed interfaces to insert a row into sql
type DBRowMarshaler interface {
	DBNamer
	DBRow() []Col
}

//Scanner is abstraction interface for sql.Row and sql.Rows
//as it relates to sql.Row.Scan() which is the one call we care about on model level.
type Scanner interface {
	Scan(dest ...interface{}) error
}

//DBScanner is something that can scan from a sql.Row/sql.Rows (abstracted into Scanner above) to initialize itself
type DBScanner interface {
	DBScan(Scanner) error
}

//DBRowUnmarshaler is composed of needed interfaces to get a row from sql using a primary key
type DBRowUnmarshaler interface {
	DBRowMarshaler
	DBScanner
}

//connection is and abstraction for either sql.DB or sql.Tx
//in other words either sql connections or sql transactions will satisfy the interface
type connection interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Exec(query string, args ...interface{}) (sql.Result, error)
}
