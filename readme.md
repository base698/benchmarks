## Sample run
```bash
$ go run benchmarks/benchmarks.go -test=psql -get -r 1000000
```

Valid tests are psql, redis, couchbase.  Set DATABASE_URL to use psql.  

## Example output
```
Testing psql using 1000000 requests using 20 threads.
Doing insert test...
7433.18 insert OPS (ops/second)
0 failures.
Doing read test...
10587.28 get OPS (ops/second)
2 failures.
```

## OR:  Build and use the binary
```bash
$ go get
$ go build benchmarks/benchmarks.go
$ ./benchmarks
```
