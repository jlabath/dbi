package dbi

import (
	"database/sql"
	"encoding/json"
	"math/big"
)

type Company struct {
	ID     int64  `json:"id"`
	Name   string `json:"name"`
	Ticker string `json:"ticker"`
}

func (c *Company) DBName() string {
	return "company"
}

func (c *Company) DBRow() []Col {
	return []Col{
		Col{"ID", c.ID, &ColOpt{"INTEGER PRIMARY KEY", NoInsert | PrimaryKey}},
		Col{"Name", c.Name, nil},
		Col{"Ticker", c.Ticker, nil},
	}
}

func (c *Company) DBNew() DBScanner {
	var n Company
	return &n
}

func (c *Company) DBScan(scanner Scanner) error {
	return scanner.Scan(&c.ID, &c.Name, &c.Ticker)
}

type AnnualReport struct {
	ID        int64
	CompanyID int64
	Year      int
	Sales     *big.Int
	NetIncome *big.Int
}

func (ar *AnnualReport) DBName() string {
	return "annual_report"
}

func (ar *AnnualReport) DBRow() []Col {
	var (
		salesVal  string
		netIncVal []byte
	)
	if ar.Sales != nil {
		salesVal = ar.Sales.String()
	}
	if ar.NetIncome != nil {
		netIncVal, _ = json.Marshal(ar.NetIncome)
	}
	return []Col{
		Col{"id", ar.ID, &ColOpt{"INTEGER PRIMARY KEY", NoInsert | PrimaryKey}},
		Col{"company_id", ar.CompanyID, nil},
		Col{"year", ar.Year, nil},
		Col{"sales", salesVal, nil},                         //store as varchar(255)
		Col{"net_income", netIncVal, &ColOpt{Type: "BLOB"}}, //store in DB as []byte
	}
}

func (ar *AnnualReport) DBScan(scanner Scanner) error {
	var (
		salesVal  sql.NullString
		netIncBuf []byte
	)
	if err := scanner.Scan(&ar.ID, &ar.CompanyID, &ar.Year, &salesVal, &netIncBuf); err != nil {
		return err
	}
	if salesVal.Valid {
		ar.Sales = big.NewInt(0)
		ar.Sales.SetString(salesVal.String, 10)
	}
	if len(netIncBuf) > 0 {
		ar.NetIncome = big.NewInt(0)
		if err := json.Unmarshal(netIncBuf, ar.NetIncome); err != nil {
			return err
		}
	}
	return nil
}

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
func (p *Person) DBRow() []Col {
	return []Col{
		Col{"id", p.ID, &ColOpt{"INTEGER PRIMARY KEY", NoInsert | PrimaryKey}},
		Col{"first", p.FirstName, nil},
		Col{"last", p.LastName, nil},
	}
}

//scan into our struct from sql.Row or sql.Rows
func (p *Person) DBScan(scanner Scanner) error {
	return scanner.Scan(&p.ID, &p.FirstName, &p.LastName)
}
