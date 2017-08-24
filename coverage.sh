#/bin/sh
rm coverage.prof
go test -coverprofile=coverage.prof -race -v -timeout 20s
if [ $? -ne 0 ]; then
    exit $?
fi
if [ -z "$1" ]; then
  go tool cover -func=coverage.prof
else
  go tool cover -html=coverage.prof
fi
