package dbi

import (
	"database/sql"
	"fmt"
)

//TxContext stores options for transactions
type TxContext struct{}

//TxOption configures a transaction
type TxOption func(*TxContext) error

//Begin starts a transaction
func (db *H) Begin(opts ...TxOption) (*Tx, error) {
	fmt.Fprintf(db.lw, "BEGIN\n")
	sqlTx, err := db.DB().Begin()
	if err != nil {
		return nil, err
	}
	return newTx(db, sqlTx)
}

//Tx is a reference to a specific transaction
type Tx struct {
	dbi *H
	tx  *sql.Tx
}

func newTx(db *H, tx *sql.Tx) (*Tx, error) {
	return &Tx{
		dbi: db,
		tx:  tx,
	}, nil
}

//Commit commits this transaction
func (tx *Tx) Commit() error {
	fmt.Fprintf(tx.dbi.lw, "COMMIT\n")
	return tx.tx.Commit()
}

//Rollback aborts this transaction
func (tx *Tx) Rollback() error {
	fmt.Fprintf(tx.dbi.lw, "ROLLBACK\n")
	return tx.tx.Rollback()
}

//Insert a record into sql and return a Col with the primary key and any error
func (tx *Tx) Insert(s DBRowMarshaler) (Col, error) {
	return insert(tx.tx, tx.dbi.dbType, tx.dbi.placeholder, tx.dbi.lw, s)
}

//Select runs an SQL query and populates dst and returns an error if any.
//It uses the supplied dst to deduce original type to be able to call DBRow(), DBName() etc.
//The where is any where/order by/limit type of clause - if empty it will simply do SELECT col1,col2,... FROM table_name
//args are any params to be used in the SQL query to replace ?
//It expects dst to be a pointer to a slice of RowUnmarshaler(s), and it will return an error if it is not.
func (tx *Tx) Select(dst interface{}, where string, args ...sql.NamedArg) error {
	return tx.SelectOption(dst, nil, where, args...)
}

//SelectOption is functionaly the same as Select however by allowing the user to pass options it is possible to perform
//additional initializations before the DBName, DBRow, or DBScan are even called.
//it's also possible to provide context to allow cancellable queries introduced in go 1.8
func (tx *Tx) SelectOption(
	dst interface{},
	optionFunc StmtOption,
	where string, args ...sql.NamedArg) error {
	qc := StmtContext{}
	if optionFunc != nil {
		if err := optionFunc(&qc); err != nil {
			return err
		}
	}
	return selectQuery(
		tx.tx,
		tx.dbi.placeholder,
		tx.dbi.namedArgPrefix,
		tx.dbi.lw,
		dst,
		&qc,
		where,
		args...)
}

//DBI returns the originating DBI handle for this transaction
func (tx *Tx) DBI() *H {
	return tx.dbi
}

//Get a record from SQL using the supplied PrimaryKey
func (tx *Tx) Get(s DBRowUnmarshaler) error {
	return get(tx.tx, tx.dbi.placeholder, tx.dbi.lw, s)
}

//Update a record in SQL using the supplied data
func (tx *Tx) Update(s DBRowUnmarshaler) error {
	return update(tx.tx, tx.dbi.placeholder, tx.dbi.lw, s)
}

//Delete deletes a single row from db using the given models PrimaryKey
func (tx *Tx) Delete(s DBRowMarshaler) error {
	return delete(tx.tx, tx.dbi.placeholder, tx.dbi.lw, s)
}
