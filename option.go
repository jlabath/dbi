package dbi

import (
	"errors"
	"io"
)

//DBOption is configuration option when creating DBI handle
type DBOption func(*H) error

func (db *H) setLogger(w io.Writer) error {
	if w == nil {
		return errors.New("logger writer is nil")
	}
	db.lw = w
	return nil
}

//Logger is an optional configuration option if logging of SQL statements by dbi is desired
//db, err := New(mySqlConn, Logger(myWriter))
func Logger(w io.Writer) DBOption {
	return func(db *H) error {
		return db.setLogger(w)
	}
}

//Postgres is an optional configuration option to activate Postgres behavior as expected by lib/pq or pgx postgres drivers
//db, err := New(mySqlConn, Postgres(), Logger(myWriter))
func Postgres() DBOption {
	return func(db *H) error {
		db.placeholder = pgPlaceHolder
		db.dbType = postgres
		return nil
	}
}

//Mysql is an optional configuration option to activate MySQL behavior
//db, err := New(mySqlConn, Mysql(), Logger(myWriter))
func Mysql() DBOption {
	return func(db *H) error {
		db.placeholder = defaultPlaceHolder
		db.dbType = mysql
		return nil
	}
}
