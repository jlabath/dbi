package dbi

import "context"

//StmtContext for advanced settings during query execution
//this will be modified via the StmtOption functions
type StmtContext struct {
	newFunc func() RowUnmarshaler
	context context.Context
}

//StmtOption is configuration function to configure QueryContext before executing the query
type StmtOption func(qc *StmtContext) error

//WithNewFunc returns a QueryOption that will modify the QueryContext with
//function that will be called to initialize the target type before
//the result from DB is scanned
func WithNewFunc(newFunc func() RowUnmarshaler) StmtOption {
	return func(qc *StmtContext) error {
		qc.newFunc = newFunc
		return nil
	}
}

//WithContext is configuration function to configure QueryContext with provided context
//which then gives caller the ability to cancel and timeout queries
func WithContext(ctx context.Context) StmtOption {
	return func(qc *StmtContext) error {
		qc.context = ctx
		return nil
	}
}

//Compose combines several options into one
//e.g. Compose(WithContext(ctx), WithNewFunc(myInitFunc))
func Compose(opts ...StmtOption) StmtOption {
	return func(qc *StmtContext) error {
		for _, opt := range opts {
			err := opt(qc)
			if err != nil {
				return nil
			}
		}
		return nil
	}
}
