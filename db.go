package dbi

import (
	"bytes"
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

//ErrNewScannerInstance is returned when H was not able to create a new instance of T
var ErrNewScannerInstance = errors.New("Unable to create new instance, ensure type implements DBScanner() or implement custom DBNew().")

func newScannerInstance(something interface{}) (DBScanner, error) {
	if n, ok := something.(DBNewer); ok {
		//imlements newer so just return that
		return n.DBNew(), nil
	}
	typ := reflect.TypeOf(something)
	// if a pointer to a struct is passed, get the type of the dereferenced object
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	nPtr := reflect.New(typ)
	if n, ok := nPtr.Interface().(DBScanner); ok {
		return n, nil
	}
	return nil, ErrNewScannerInstance
}

//H is our handle supporting Insert/Get/Update to be used by client
type H struct {
	conn *sql.DB
	lw   io.Writer
}

//New return a new DB handle
func New(conn *sql.DB, logw io.Writer) *H {
	h := H{
		conn: conn,
		lw:   logw}
	if logw == nil {
		h.lw = ioutil.Discard
	}
	return &h
}

//CreateTable executes CREATE TABLE as per DBRow()
func (db *H) CreateTable(source RowMarshaler) error {
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

//Insert a record into sql and return a Col with the primary key and any error
func (db *H) Insert(s RowMarshaler) (Col, error) {
	var (
		buf   bytes.Buffer
		retPK Col
	)
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
		buf.WriteString("?")
	}
	buf.WriteString(")")
	fmt.Fprintln(db.lw, "BEGIN")
	tx, err := db.conn.Begin()
	fmt.Fprintln(db.lw, buf.String(), args)
	_, err = tx.Exec(buf.String(), args...)
	if err != nil {
		fmt.Fprintln(db.lw, "ROLLBACK")
		tx.Rollback()
		return retPK, err
	}
	retPK, err = db.lastInsertPKID(tx, s)
	if err != nil {
		fmt.Fprintln(db.lw, "ROLLBACK")
		tx.Rollback()
		return retPK, err
	}
	fmt.Fprintln(db.lw, "COMMIT")
	err = tx.Commit()
	return retPK, err
}

func (db *H) lastInsertPKID(tx *sql.Tx, s RowMarshaler) (Col, error) {
	var (
		buf   bytes.Buffer
		retPK Col
	)
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
		buf.WriteString("=?")
		args = append(args, v.Val)
	}
	buf.WriteString(" ORDER BY ")
	buf.WriteString(pk.Name)
	buf.WriteString(" DESC ") //presumably order by highest first
	fmt.Fprintln(db.lw, buf.String(), args)
	rows, err := tx.Query(buf.String(), args...)
	if err != nil {
		return retPK, err
	}
	if rows.Next() {
		retPK.Val, err = deduceHowToScanVal(pk, rows)
	}
	rows.Close()
	return retPK, err
}

//Get a record from SQL using the supplied PrimaryKey
func (db *H) Get(s RowUnmarshaler) error {
	row := s.DBRow()
	pkey := getPKFromColumns(row)
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
	buf.WriteString("=?")
	fmt.Fprintln(db.lw, buf.String(), pkey.Val)
	dbrow := db.conn.QueryRow(buf.String(), pkey.Val)
	err := s.DBScan(dbrow)
	return err
}

//Update a record in SQL using the supplied data
func (db *H) Update(s RowUnmarshaler) error {
	row := s.DBRow()
	pkey := getPKFromColumns(row)
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
		buf.WriteString("=?")
		args = append(args, v.Val)
	}
	buf.WriteString(" WHERE ")
	buf.WriteString(pkey.Name)
	buf.WriteString("=?")
	args = append(args, pkey.Val)
	fmt.Fprintln(db.lw, buf.String(), args)
	_, err := db.conn.Exec(buf.String(), args...)
	return err
}

//Select runs an SQL query and returns sql.Rows and error if any
//it uses the supplied source to call DBRow() to know what columns to ask for
//the where is any where/order by/limit type of clause - if empty it will simply do SELECT col1,col2,... FROM table_name
//args are any params to be used in the SQL query to replace ?
func (db *H) Select(s RowUnmarshaler, where string, args ...interface{}) ([]interface{}, error) {
	var (
		buf bytes.Buffer
	)
	row := s.DBRow()
	buf.WriteString("SELECT ")
	for i, v := range row {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(v.Name)
	}
	buf.WriteString(" FROM ")
	buf.WriteString(s.DBName())
	buf.WriteString(" ")
	buf.WriteString(where)
	fmt.Fprintln(db.lw, buf.String(), args)
	rows, err := db.conn.Query(buf.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	results := make([]interface{}, 0, 20)
	for rows.Next() {
		r, err := newScannerInstance(s)
		if err != nil {
			return nil, err
		}
		err = r.DBScan(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	return results, nil
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

//Flag for storing meta information about parent Col
type Flag uint16

const (
	//NoInsert means do not include this column on inserts
	NoInsert Flag = 1 << (16 - 1 - iota)
	//PrimaryKey marks this column as primary key
	PrimaryKey
)

//ColOpt is struct for optional meta information
//e.g. to mark Field as PrimaryKey or to use custom type for TableCreate
type ColOpt struct {
	Type  string // type to use when CREATE TABLE is called e.g. text, blob
	Flags Flag   // meta information about the column such as PrimaryKey
}

//Col is our basic structure consisting of Name,Val pair and optional Type and Flags attributes
type Col struct {
	Name string      // column name in DB
	Val  interface{} // value to store in DB
	Opt  *ColOpt     // options
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

//RowMarshaler composed of needed interfaces to insert a row into sql
type RowMarshaler interface {
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

//RowUnmarshaler is composed of needed interfaces to get a row from sql using a primary key
type RowUnmarshaler interface {
	RowMarshaler
	DBScanner
}

//DBNewer is something that has custom method DBNew to create a new instance of itself.
//If model in question does not implement DBNewer, reflection will be used to create new instances.
type DBNewer interface {
	DBNew() DBScanner
}
