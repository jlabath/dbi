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

//Commit commits a transaction and marks it as completed and unusable
func (tx *Tx) Commit() error {
	fmt.Fprintf(tx.dbi.lw, "COMMIT\n")
	return tx.tx.Commit()
}

//Insert a record into sql and return a Col with the primary key and any error
func (tx *Tx) Insert(s RowMarshaler) (Col, error) {
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
	optionFunc QueryOption,
	where string, args ...sql.NamedArg) error {
	qc := QueryContext{}
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
