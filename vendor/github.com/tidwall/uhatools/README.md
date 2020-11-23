# uhatools

[![GoDoc](https://godoc.org/github.com/tidwall/uhatools?status.svg)](https://godoc.org/github.com/tidwall/uhatools)

Tools for managing [Uhaha](https://github.com/tidwall/uhaha) services.
Right now this only has the Uhaha client library.

## Open a Cluster

This will define a new cluster pool that will be used for establishing
connections to a Uhaha cluster.

```go
cl := uhatools.OpenCluster(uhatools.ClusterOptions{
    InitialServers: []string { 
        "127.0.0.1:11001", // Server 1
        "127.0.0.1:11002", // Server 2
        "127.0.0.1:11003", // Server 3
    },
})
```

Close the Cluster when you don't need it anymore

```go
cl.Close()
```

## Connect to cluster

Get a connection from the cluster pool. The connection will automatically track
the leadership changes. It's api is modeled after the
[redigo](https://github.com/gomodule/redigo) project.

```go
conn := cl.Get()
defer conn.Close() // Always close the connection when you're done

pong, err := uhatools.String(conn.Do("PING"))
if err != nil{
    return err
}
println(pong) // should be PONG
```

## License

`uhatools` source code is available under the MIT License.
