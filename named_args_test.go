package dbi

import (
	"testing"
)

func TestNamedArgsParsing(t *testing.T) {

	var tests = []struct {
		in  string
		out string
		ph  placeHolderFunc
	}{
		{
			`SELECT ID,Name,Ticker
 FROM company WHERE Ticker != @ticker
 AND ID != @id AND ID > @id ORDER BY ID`,
			`SELECT ID,Name,Ticker
 FROM company WHERE Ticker != ?
 AND ID != ? AND ID > ? ORDER BY ID`,
			defaultPlaceHolder(),
		},
		{
			`SELECT ID,Name,Ticker
 FROM company WHERE Ticker != @ticker
 AND ID != @id AND ID > @id ORDER BY ID`,
			`SELECT ID,Name,Ticker
 FROM company WHERE Ticker != $1
 AND ID != $2 AND ID > $3 ORDER BY ID`,
			pgPlaceHolder(),
		},
	}

	for _, test := range tests {
		query, argKeys, err := produceQuery('@', test.ph, test.in)
		if err != nil {
			t.Error(err)
		}
		if query != test.out {
			t.Errorf("Expected\n%s\nbut got\n%s", test.out, query)
		}
		if len(argKeys) != 3 {
			t.Fatalf("Expected 3 items returned in argKeys")
		}
		if argKeys[0] != "ticker" || argKeys[1] != "id" || argKeys[1] != argKeys[2] {
			t.Errorf("Expected [ticker id id] but got %v", argKeys)
		}
	}
}
