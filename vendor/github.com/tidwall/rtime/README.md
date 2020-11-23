# rtime

[![GoDoc](https://img.shields.io/badge/api-reference-blue.svg?style=flat-square)](https://godoc.org/github.com/tidwall/rtime)

Retrieve the current time from remote servers.

It works by requesting timestamps from twelve very popular hosts over https.
As soon as it gets at least three responses, it takes the two that have the
smallest difference in time. And from those two it picks the one that is
the oldest. Finally it ensures that the time is monotonic.

## Getting

```
go get -u github.com/tidwall/rtime
```

## Using

The only function is `rtime.Now()`.

```go
tm := rtime.Now()
if tm.IsZero() {
    panic("time could not be retrieved")
}
println(tm.String())
// output: 2020-03-29 10:27:00 -0700 MST
}
```

## Contact

Josh Baker [@tidwall](http://twitter.com/tidwall)

## License

Source code is available under the MIT [License](/LICENSE).