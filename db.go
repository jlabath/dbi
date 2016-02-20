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

//ErrPrimaryKeyOverflow is returned sql.Result.LastInsertId overflows the declared int type
var ErrPrimaryKeyOverflow = errors.New("Last insert ID returned by database overflows model's type.")

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
var ErrNoPointerToSlice = errors.New("Expected dst to be a pointer to a slice.")

//ErrNoUnmarshaler is returned when element of slice dst does not implement RowUnmarshaler
var ErrNoUnmarshaler = errors.New("Elements of slice dst do not implement RowUnmarshaler.")

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
)

//H is our handle supporting Insert/Get/Update to be used by client
type H struct {
	conn        Connection
	lw          io.Writer
	placeholder func() placeHolderFunc
	dbType      dbTyp
}

func newH(conn Connection) *H {
	return &H{
		conn:        conn,
		lw:          ioutil.Discard,
		placeholder: defaultPlaceHolder}
}

//New return a new DB handle
func New(conn Connection, options ...func(*H) error) (*H, error) {
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

func (db *H) setLogger(w io.Writer) error {
	if w == nil {
		return errors.New("logger writer is nil")
	}
	db.lw = w
	return nil
}

//Logger is an optional configuration option if logging of SQL statements by dbi is desired
//db, err := New(mySqlConn, Logger(myWriter))
func Logger(w io.Writer) func(*H) error {
	return func(db *H) error {
		return db.setLogger(w)
	}
}

//Postgres is an optional configuration option to activate Postgres behavior as expected by lib/pq postgres driver.
//db, err := New(mySqlConn, Logger(myWriter))
func Postgres(db *H) error {
	db.placeholder = pgPlaceHolder
	db.dbType = postgres
	return nil
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
	phFunc := db.placeholder()
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
	if db.dbType == postgres {
		//postgres inserts should use returning
		return db.postgresInsert(s, sql, args)
	}
	fmt.Fprintln(db.lw, sql, args)
	result, err := db.conn.Exec(sql, args...)
	if err != nil {
		return retPK, err
	}
	retPK, err = db.lastInsertPKID(db.conn, s, result)
	if err != nil {
		return retPK, err
	}
	return retPK, err
}

func (db *H) lastInsertPKID(tx Connection, s RowMarshaler, result sql.Result) (Col, error) {
	var (
		buf   bytes.Buffer
		retPK Col
	)
	phFunc := db.placeholder()
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
	fmt.Fprintln(db.lw, buf.String(), args)
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

func (db *H) postgresInsert(s RowMarshaler, sql string, args []interface{}) (Col, error) {
	plainInsert := false
	//first let's make sure this even has a primary key
	row := s.DBRow()
	pk := getPKFromColumns(row)
	if pk == nil {
		plainInsert = true
	}

	if plainInsert {
		fmt.Fprintln(db.lw, sql, args)
		_, err := db.conn.Exec(sql, args...)
		return Col{}, err
	}

	//turn into returning query
	sql = fmt.Sprintf("%s RETURNING %s", sql, pk.Name)
	fmt.Fprintln(db.lw, sql, args)
	var liid int64
	if err := db.conn.QueryRow(sql, args...).Scan(&liid); err != nil {
		return *pk, err
	}
	cnvtLiid, err := forceToTypeOfVal(pk, liid)
	if err == nil {
		pk.Val = cnvtLiid
	}
	return *pk, err
}

//ErrNotFound returned when the row with the given primary key was not found
var ErrNotFound = errors.New("Record with given primary key not found.")

//Get a record from SQL using the supplied PrimaryKey
func (db *H) Get(s RowUnmarshaler) error {
	phFunc := db.placeholder()
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
	fmt.Fprintln(db.lw, buf.String(), pkey.Val)
	dbrow := db.conn.QueryRow(buf.String(), pkey.Val)
	err := s.DBScan(dbrow)
	if err == sql.ErrNoRows {
		return ErrNotFound
	}
	return err
}

//Update a record in SQL using the supplied data
func (db *H) Update(s RowUnmarshaler) error {
	phFunc := db.placeholder()
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
	fmt.Fprintln(db.lw, buf.String(), args)
	res, err := db.conn.Exec(buf.String(), args...)
	if err == nil {
		if n, err := res.RowsAffected(); err == nil && n == 0 {
			return ErrNotFound
		}
	}
	return err
}

//Select runs an SQL query and populates dst and returns an error if any.
//It uses the supplied dst to deduce original type to be able to call DBRow(), DBName() etc.
//The where is any where/order by/limit type of clause - if empty it will simply do SELECT col1,col2,... FROM table_name
//args are any params to be used in the SQL query to replace ?
//It expects dst to be a pointer to a slice of RowUnmarshaler(s), and it will return an error if it is not.
func (db *H) Select(dst interface{}, where string, args ...interface{}) error {
	return db.SelectNew(dst, nil, where, args...)
}

//SelectNew is functionaly the same as Select however by allowing the user to pass newFunc it is possible to perform
//additional initializations before the DBName, DBRow, or DBScan are even called.
func (db *H) SelectNew(dst interface{}, newFunc func() RowUnmarshaler, where string, args ...interface{}) error {
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
	if newFunc == nil {
		newValue = reflect.New(baseBaseType)
	} else {
		newValue = reflect.ValueOf(newFunc())
	}
	source, isUnmarshaler := newValue.Interface().(RowUnmarshaler)
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
	query := db.fixQuery(buf.String())
	fmt.Fprintln(db.lw, query, args)
	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	dstv := reflect.ValueOf(dst).Elem()
	for rows.Next() {
		var rowScn DBScanner
		if newFunc == nil {
			rowScn = reflect.New(baseBaseType).Interface().(DBScanner)
		} else {
			rowScn = newFunc()
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

func (db *H) fixQuery(origQuery string) string {
	const placeHolderRune rune = '?'
	if db.dbType == sqlite {
		//all done
		return origQuery
	}
	phf := db.placeholder()
	var buf bytes.Buffer
	rbuf := bytes.NewBufferString(origQuery)
	for {
		r, _, err := rbuf.ReadRune()
		if err != nil {
			break
		}
		switch r {
		case placeHolderRune:
			buf.WriteString(phf())
		default:
			buf.WriteRune(r)
		}
	}
	return buf.String()
}

//ErrNoPrimaryKey is returned when the model does not have a column marked as PrimaryKey
var ErrNoPrimaryKey = errors.New("No primary key defined. Use PrimaryKey flag.")

//Delete deletes a single row from db using the given models PrimaryKey
func (db *H) Delete(s RowMarshaler) error {
	row := s.DBRow()
	pkey := getPKFromColumns(row)
	if pkey == nil {
		return ErrNoPrimaryKey
	}
	var buf bytes.Buffer
	buf.WriteString("DELETE FROM ")
	buf.WriteString(s.DBName())
	buf.WriteString(" WHERE ")
	buf.WriteString(pkey.Name)
	buf.WriteString("=?")
	fmt.Fprintln(db.lw, buf.String(), pkey.Val)
	_, err := db.conn.Exec(buf.String(), pkey.Val)
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

//Connection is and abstraction for either sql.DB or sql.Tx
//in other words either sql connections or sql transactions will satisfy the interface
type Connection interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Exec(query string, args ...interface{}) (sql.Result, error)
}
