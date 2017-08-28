package dbi

import "context"

//QueryContext for advanced settings during query execution
//this will be modified via the QueryOption functions
type QueryContext struct {
	newFunc func() RowUnmarshaler
	context context.Context
}

//QueryOption is configuration function to configure QueryContext before executing the query
type QueryOption func(qc *QueryContext) error

//NewFuncQO returns a QueryOption that will modify the QueryContext with
//function that will be called to initialize the target type before
//the result from DB is scanned
func NewFuncQO(newFunc func() RowUnmarshaler) QueryOption {
	return func(qc *QueryContext) error {
		qc.newFunc = newFunc
		return nil
	}
}

//WithContextQO is configuration function to configure QueryContext with provided context
//which then gives caller the ability to cancel and timeout queries
func WithContextQO(ctx context.Context) QueryOption {
	return func(qc *QueryContext) error {
		qc.context = ctx
		return nil
	}
}

//WithQO combines several options into one
func WithQO(opts ...QueryOption) QueryOption {
	return func(qc *QueryContext) error {
		for _, opt := range opts {
			err := opt(qc)
			if err != nil {
				return nil
			}
		}
		return nil
	}
}
