### DBI

[![GoDoc](https://godoc.org/github.com/jlabath/dbi?status.svg)](https://godoc.org/github.com/jlabath/dbi/v3)
[![Travis](https://www.travis-ci.org/jlabath/dbi.svg?branch=master)](https://www.travis-ci.org/jlabath/dbi)   

Opinionated database/sql wrapper.  

DBI is an attempt to track my personal likes and dislikes after many years of using various ORM libraries.  
 
Likes:
* Define your model, set attributes and save the model, which is then behind the scenes, persisted to DB.
* Get an ID from somehwere and simply get the model from DB, then change it save it back again in few simple steps.
* Get results of a query as a slice of models.
* Never again misspel or forget a column name.

Dislikes:
* Custom language for writing queries - SQL does this already, and much better than all the various frameworks that tried to wrap that syntax into their own version.
* Custom language for table schema definition - same as above plus the supported datatypes (and their options) vary widely by DB used.

DBI gives maximum control to the user to define how models are stored and retreived from DB, and how the table schema looks like.

At present Support for Postgres, MySQL and Sqlite.

Define your models and implement required methods to satisfy RowMarshaler/RowUnmarshaller

```golang
type Person struct {
	ID        int
	FirstName string
	LastName  string
}

//define table name
func (p *Person) DBName() string {
	return "person"
}

//serialize our struct
func (p *Person) DBRow() []dbi.Col {
	return []dbi.Col{
		dbi.NewCol("id", p.ID, &dbi.ColOpt{"SERIAL PRIMARY KEY", dbi.NoInsert | dbi.PrimaryKey}),
		dbi.NewCol("first", p.FirstName, nil),
		dbi.NewCol("last", p.LastName, nil),
	}
}

//scan into our struct from sql.Row or sql.Rows
func (p *Person) DBScan(scanner dbi.Scanner) error {
	return scanner.Scan(&p.ID, &p.FirstName, &p.LastName)
}
```
  
Usage then looks like this

```golang

db, err := dbi.New(sqlConn, Postgres())
p := &Person{
	FirstName: "John",
	LastName:  "Doe",
}

//create table
err := db.CreateTable(p, nil)

//insert
pk, err := db.Insert(p, nil)

//get
up := &Person{ID: pk.Val.(int)}
err = db.Get(up, nil)

//update
up.LastName = "Moe"
err = db.Update(up, nil)

//query
var persons []Person
err := db.Select(&persons, nil, "WHERE last = @last ORDER BY last", db.Named("last", "Moe"))
```
