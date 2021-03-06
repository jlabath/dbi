package dbi

import (
	"database/sql"
	"os"
	"reflect"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/stdlib"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

type setupFunc func() (*H, error)
type tearDownFunc func(db *H) error

//sqlite
func sqliteSetup() (*H, error) {
	conn, err := sql.Open("sqlite3", "basic_suite.db")
	if err != nil {
		return nil, err
	}
	return New(conn)
}

func sqliteTearDown(db *H) error {
	db.DB().Close()
	os.Remove("basic_suite.db")
	return nil
}

//postgres pq
func pqSetup() (*H, error) {
	strconn := os.ExpandEnv("user=$PGUSER host=$PGHOST dbname=$PGDATABASE sslmode=disable")
	conn, err := sql.Open("postgres", strconn)
	if err != nil {
		return nil, err
	}

	//overwrite pkMeta from models_test.go
	pkMeta = &ColOpt{"SERIAL PRIMARY KEY", NoInsert | PrimaryKey}
	blobMeta = &ColOpt{Type: "bytea"}
	return New(conn, Postgres())
}

func pqTearDown(db *H) error {
	db.DB().Close()
	return nil
}

//postgres pgx
func pgxSetup() (*H, error) {
	conn, err := sql.Open(
		"pgx",
		os.ExpandEnv("user=$PGUSER host=$PGHOST database=$PGDATABASE sslmode=disable"))
	if err != nil {
		return nil, err
	}

	//overwrite pkMeta from models_test.go
	pkMeta = &ColOpt{"SERIAL PRIMARY KEY", NoInsert | PrimaryKey}
	blobMeta = &ColOpt{Type: "bytea"}
	return New(conn, Postgres())
}

func pgxTearDown(db *H) error {
	db.DB().Close()
	return nil
}

//go-sql mysql
func gosqlSetup() (*H, error) {
	conn, err := sql.Open(
		"mysql",
		os.ExpandEnv("$MYSQLUSER@tcp($PGHOST:3306)/$PGDATABASE"))
	if err != nil {
		return nil, err
	}

	//overwrite pkMeta from models_test.go
	pkMeta = &ColOpt{"SERIAL PRIMARY KEY", NoInsert | PrimaryKey}
	blobMeta = &ColOpt{Type: "BLOB"}
	return New(conn, Mysql())
}

func gosqlTearDown(db *H) error {
	db.DB().Close()
	return nil
}

type TestSuite interface {
	Name() string
}

func TestDBI(t *testing.T) {
	var tests = []struct {
		name     string
		setup    setupFunc
		tearDown tearDownFunc
		suits    []TestSuite
	}{
		{"sqlite", sqliteSetup, sqliteTearDown, []TestSuite{&BasicSuite{}}},
		{"pq[postgres]", pqSetup, pqTearDown, []TestSuite{&BasicSuite{}}},
		{"pgx[postgres]", pgxSetup, pgxTearDown, []TestSuite{&BasicSuite{}}},
		{"go-sql-driver[mysql]", gosqlSetup, gosqlTearDown, []TestSuite{&BasicSuite{}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, suite := range test.suits {
				t.Run(suite.Name(), func(t *testing.T) {
					db, err := test.setup()
					if err != nil {
						t.Fatal(err)
					}
					//run suite
					runSuite(t, db, suite)
					err = test.tearDown(db)
					if err != nil {
						t.Error(err)
					}
				})
			}
		})
	}

}

//wrap t.Log as io.Writer
//that DBI can then use
type cWriter struct {
	t *testing.T
}

func (w cWriter) Write(data []byte) (int, error) {
	w.t.Log(strings.TrimSpace(string(data)))
	return len(data), nil
}

//allow DBI logger to log via t.Log
func localizeLogger(db *H, t *testing.T) (*H, error) {
	fn := Logger(&cWriter{t})
	err := fn(db)
	return db, err
}

//executes each test method os suite in its own subtest
func runSuite(t *testing.T, db *H, suite interface{}) {
	rv := reflect.ValueOf(suite)
	//if not pointer quit
	if rv.Kind() != reflect.Ptr {
		return
	}

	rt := rv.Type()
	for i := 0; i < rt.NumMethod(); i++ {
		m := rt.Method(i)
		if strings.HasPrefix(m.Name, "Test") {
			t.Run(m.Name, func(t *testing.T) {

				db, err := localizeLogger(db, t)
				if err != nil {
					t.Fatal(err)
				}

				var args = []reflect.Value{
					rv,
					reflect.ValueOf(t),
					reflect.ValueOf(db),
				}
				m.Func.Call(args)
			})
		}

	}
}
