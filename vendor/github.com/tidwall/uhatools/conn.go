package uhatools

import (
	"crypto/tls"
	"errors"
	"io"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

var defaultLeadershipTimeout = time.Second * 20
var defaultConnectionTimeout = time.Second * 5
var defaultRetryTimeout = time.Millisecond * 250
var defaultPoolSize = 20

// ErrClosed is returned when a Uhaha connection has been closed.
var ErrClosed = errors.New("closed")

// ErrLeadershipTimeout is returned when a conenction to a Uhaha leader could
// not be established within the required time.
var ErrLeadershipTimeout = errors.New("leadership timeout")

// ErrConnectionTimeout is returned when a conenction to a Uhaha server could
// not be established within the required time.
var ErrConnectionTimeout = errors.New("connection timeout")

// Cluster represents a Uhaha connection cluster and pool
type Cluster struct {
	opts   ClusterOptions
	mu     sync.Mutex
	closed bool
	conns  []*Conn
}

// ClusterOptions are provide to OpenCluster.
type ClusterOptions struct {
	DialOptions             // The Dial Options for each connection
	InitialServers []string // The initial cluster server addresses
	PoolSize       int      // Max number of connection in pool. default: 15
}

// OpenCluster returns a new Uhaha Cluster connection pool
func OpenCluster(opts ClusterOptions) *Cluster {
	if opts.PoolSize == 0 {
		opts.PoolSize = defaultPoolSize
	}
	if opts.TLSConfig != nil {
		opts.TLSConfig = opts.TLSConfig.Clone()
	}
	if opts.LeadershipTimeout == 0 {
		opts.LeadershipTimeout = defaultLeadershipTimeout
	}
	if opts.ConnectionTimeout == 0 {
		opts.ConnectionTimeout = defaultConnectionTimeout
	}
	return &Cluster{opts: opts}
}

// Get a connection from the Cluster.
func (cl *Cluster) Get() *Conn {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if cl.closed {
		return &Conn{closed: true}
	}
	for len(cl.conns) > 0 {
		// borrow
		c := cl.conns[len(cl.conns)-1]
		cl.conns[len(cl.conns)-1] = nil
		cl.conns = cl.conns[:len(cl.conns)-1]
		c.closed = false
		pong, err := redis.String(c.conn.Do("PING"))
		if err == nil && pong == "PONG" {
			return c
		}
	}
	return &Conn{
		cluster: cl,
		servers: cl.opts.InitialServers,
		opts:    cl.opts.DialOptions,
	}
}

// Close the cluster and pooled connections
func (cl *Cluster) Close() error {
	cl.mu.Lock()
	if cl.closed {
		cl.mu.Unlock()
		return ErrClosed
	}
	conns := cl.conns
	cl.conns = nil
	cl.closed = true
	cl.mu.Unlock()
	for _, c := range conns {
		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
		}
	}
	return nil
}

// Conn represents a connection to a Uhaha cluster
type Conn struct {
	closed  bool
	cluster *Cluster
	conn    redis.Conn
	servers []string
	opts    DialOptions
}

// DialOptions ...
type DialOptions struct {
	Auth              string        // optional
	TLSConfig         *tls.Config   // optional
	ConnectionTimeout time.Duration // default: 5 seconds
	LeadershipTimeout time.Duration // default: 20 seconds
}

// Dial connects to the Uhaha cluster at the given TCP address using the
// specified options. The addr param can be a single value or a comma-delimited
// set of addresses, where the set represents the servers in the Uhaha cluster.
func Dial(addr string, opts *DialOptions) (*Conn, error) {
	var dialOpts DialOptions
	if opts != nil {
		dialOpts = *opts
	}
	if dialOpts.LeadershipTimeout == 0 {
		dialOpts.LeadershipTimeout = defaultLeadershipTimeout
	}
	if dialOpts.ConnectionTimeout == 0 {
		dialOpts.ConnectionTimeout = defaultConnectionTimeout
	}
	if dialOpts.TLSConfig != nil {
		dialOpts.TLSConfig = dialOpts.TLSConfig.Clone()
	}

	var lerr error
	addrs := strings.Split(addr, ",")
	ri := rand.Int() % len(addrs)
	for i := 0; i < len(addrs); i++ {
		addr := addrs[(ri+i)%len(addrs)]
		conn, servers, err := rawDial(addr, dialOpts.Auth, dialOpts.TLSConfig,
			false, dialOpts.ConnectionTimeout)
		if err != nil {
			lerr = err
			continue
		}
		c := &Conn{
			conn:    conn,
			servers: servers,
			opts:    dialOpts,
		}
		return c, nil
	}
	return nil, lerr
}

// Close a connection
func (c *Conn) Close() error {
	if c.closed {
		return ErrClosed
	}
	c.closed = true
	if c.conn == nil {
		return nil
	}
	if c.cluster != nil {
		c.cluster.mu.Lock()
		if !c.cluster.closed && len(c.cluster.conns) < c.cluster.opts.PoolSize {
			c.cluster.conns = append(c.cluster.conns, c)
			c.cluster.mu.Unlock()
			return nil
		}
		c.cluster.mu.Unlock()
	}
	c.conn.Close()
	c.conn = nil
	return nil
}

func rawDial(addr, auth string, tlscfg *tls.Config, requireLeader bool,
	connTimeout time.Duration) (conn redis.Conn, servers []string, err error,
) {
	var opts []redis.DialOption
	if tlscfg != nil {
		opts = append(opts, redis.DialUseTLS(true), redis.DialTLSConfig(tlscfg))
	}
	opts = append(opts, redis.DialConnectTimeout(connTimeout))
	conn, err = redis.Dial("tcp", addr, opts...)
	if err != nil {
		return nil, nil, err
	}
	// do some handshaking stuff
	if err := func() (err error) {
		// authenticate
		if auth != "" {
			ok, err := redis.String(conn.Do("AUTH", auth))
			if err != nil {
				return err
			}
			if ok != "OK" {
				return errors.New("expected 'OK'")
			}
		}
		// talk to the server
		pong, err := redis.String(conn.Do("PING"))
		if err != nil {
			return err
		}
		if pong != "PONG" {
			return errors.New("expected 'PONG'")
		}
		// get server list
		vv, err := redis.Values(conn.Do("raft", "server", "list"))
		if err != nil {
			return err
		}
		if len(vv) == 0 {
			return errors.New("no servers found")
		}
		for _, v := range vv {
			m, err := redis.StringMap(v, nil)
			if err != nil {
				return err
			}
			servers = append(servers, m["address"])
		}
		if requireLeader {
			m, err := redis.StringMap(conn.Do("raft", "info", "state"))
			if err != nil {
				return err
			}
			if m["state"] != "Leader" {
				leader, err := redis.String(conn.Do("raft", "leader"))
				if err != nil || leader == "" {
					return errors.New("ERR node is not the leader")
				}
				return errors.New("TRY " + leader)
			}
		}
		return nil
	}(); err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, servers, nil
}

// Do exectes a Uhaha command on the server and returns the reply or an error.
func (c *Conn) Do(commandName string, args ...interface{}) (reply interface{},
	err error,
) {
	if c.closed {
		return nil, ErrClosed
	}
	var tryLeader string
	var requireLeader bool
	leaderStart := time.Now()
retryCommand:
	if time.Since(leaderStart) > c.opts.LeadershipTimeout {
		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
		}
		return nil, ErrLeadershipTimeout
	}
	if c.conn == nil {
		connectionStart := time.Now()
	tryNewServer:
		if time.Since(connectionStart) > c.opts.ConnectionTimeout {
			if c.conn != nil {
				c.conn.Close()
				c.conn = nil
			}
			return nil, ErrConnectionTimeout
		}
		// open a new network connection
		var addr string
		if tryLeader != "" {
			// use the request leader
			addr, tryLeader = tryLeader, ""
		} else {
			// choose a random server
			if len(c.servers) == 0 {
				return nil, errors.New("no servers provided")
			}
			addr = c.servers[rand.Int()%len(c.servers)]
		}
		if addr == "" {
			return nil, errors.New("no server address")
		}
		rconn, servers, err := rawDial(addr, c.opts.Auth, c.opts.TLSConfig,
			requireLeader, c.opts.ConnectionTimeout)
		if err != nil {
			if isNetworkError(err) {
				// just try a new server
				time.Sleep(defaultRetryTimeout)
				goto tryNewServer
			}
			if isLeadershipError(err) {
				// requires a leader, try again
				if strings.HasPrefix(err.Error(), "TRY ") {
					requireLeader = true
					tryLeader = err.Error()[4:]
				} else if strings.HasPrefix(err.Error(), "MOVED ") {
					parts := strings.Split(err.Error(), " ")
					if len(parts) == 3 {
						requireLeader = true
						tryLeader = parts[2]
					}
				}
				time.Sleep(defaultRetryTimeout)
				goto retryCommand
			}
			return nil, err
		}
		c.conn = rconn
		c.servers = servers
	}
	reply, err = c.conn.Do(commandName, args...)
	if err != nil {
		if isNetworkError(err) || isLeadershipError(err) {
			// reset the connection and try a new server
			c.conn.Close()
			c.conn = nil
			if strings.HasPrefix(err.Error(), "TRY ") {
				requireLeader = true
				tryLeader = err.Error()[4:]
			} else if strings.HasPrefix(err.Error(), "MOVED ") {
				parts := strings.Split(err.Error(), " ")
				if len(parts) == 3 {
					requireLeader = true
					tryLeader = parts[2]
				}
			}
			time.Sleep(defaultRetryTimeout)
			goto retryCommand
		}
		return nil, err
	}
	return reply, nil
}

func isNetworkError(err error) bool {
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}
	if _, ok := err.(net.Error); ok {
		return true
	}
	return false
}

func isLeadershipError(err error) bool {
	errmsg := err.Error()
	switch {
	case strings.HasPrefix(errmsg, "MOVED "):
		return true
	case strings.HasPrefix(errmsg, "CLUSTERDOWN "):
		return true
	case strings.HasPrefix(errmsg, "TRYAGAIN "):
		return true
	case strings.HasPrefix(errmsg, "TRY "):
		return true
	case errmsg == "ERR node is not the leader":
		return true
	case errmsg == "ERR leadership lost while committing log":
		return true
	case errmsg == "ERR leadership transfer in progress":
		return true
	}
	return false
}

// ErrNil indicates that a reply value is nil.
var ErrNil = redis.ErrNil

// Int is a helper that converts a command reply to an integer. If err is not
// equal to nil, then Int returns 0, err. Otherwise, Int converts the
// reply to an int as follows:
//
//  Reply type    Result
//  integer       int(reply), nil
//  bulk string   parsed reply, nil
//  nil           0, ErrNil
//  other         0, error
func Int(reply interface{}, err error) (int, error) {
	return redis.Int(reply, err)
}

// Int64 is a helper that converts a command reply to 64 bit integer. If err is
// not equal to nil, then Int64 returns 0, err. Otherwise, Int64 converts the
// reply to an int64 as follows:
//
//  Reply type    Result
//  integer       reply, nil
//  bulk string   parsed reply, nil
//  nil           0, ErrNil
//  other         0, error
func Int64(reply interface{}, err error) (int64, error) {
	return redis.Int64(reply, err)
}

// Uint64 is a helper that converts a command reply to 64 bit unsigned integer.
// If err is not equal to nil, then Uint64 returns 0, err. Otherwise, Uint64
// converts the reply to an uint64 as follows:
//
//  Reply type    Result
//  +integer      reply, nil
//  bulk string   parsed reply, nil
//  nil           0, ErrNil
//  other         0, error
func Uint64(reply interface{}, err error) (uint64, error) {
	return redis.Uint64(reply, err)
}

// Float64 is a helper that converts a command reply to 64 bit float. If err is
// not equal to nil, then Float64 returns 0, err. Otherwise, Float64 converts
// the reply to an int as follows:
//
//  Reply type    Result
//  bulk string   parsed reply, nil
//  nil           0, ErrNil
//  other         0, error
func Float64(reply interface{}, err error) (float64, error) {
	return redis.Float64(reply, err)
}

// String is a helper that converts a command reply to a string. If err is not
// equal to nil, then String returns "", err. Otherwise String converts the
// reply to a string as follows:
//
//  Reply type      Result
//  bulk string     string(reply), nil
//  simple string   reply, nil
//  nil             "",  ErrNil
//  other           "",  error
func String(reply interface{}, err error) (string, error) {
	return redis.String(reply, err)
}

// Bytes is a helper that converts a command reply to a slice of bytes. If err
// is not equal to nil, then Bytes returns nil, err. Otherwise Bytes converts
// the reply to a slice of bytes as follows:
//
//  Reply type      Result
//  bulk string     reply, nil
//  simple string   []byte(reply), nil
//  nil             nil, ErrNil
//  other           nil, error
func Bytes(reply interface{}, err error) ([]byte, error) {
	return redis.Bytes(reply, err)
}

// Bool is a helper that converts a command reply to a boolean. If err is not
// equal to nil, then Bool returns false, err. Otherwise Bool converts the
// reply to boolean as follows:
//
//  Reply type      Result
//  integer         value != 0, nil
//  bulk string     strconv.ParseBool(reply)
//  nil             false, ErrNil
//  other           false, error
func Bool(reply interface{}, err error) (bool, error) {
	return redis.Bool(reply, err)
}

// Values is a helper that converts an array command reply to a []interface{}.
// If err is not equal to nil, then Values returns nil, err. Otherwise, Values
// converts the reply as follows:
//
//  Reply type      Result
//  array           reply, nil
//  nil             nil, ErrNil
//  other           nil, error
func Values(reply interface{}, err error) ([]interface{}, error) {
	return redis.Values(reply, err)
}

// Float64s is a helper that converts an array command reply to a []float64. If
// err is not equal to nil, then Float64s returns nil, err. Nil array items are
// converted to 0 in the output slice. Floats64 returns an error if an array
// item is not a bulk string or nil.
func Float64s(reply interface{}, err error) ([]float64, error) {
	return redis.Float64s(reply, err)
}

// Strings is a helper that converts an array command reply to a []string. If
// err is not equal to nil, then Strings returns nil, err. Nil array items are
// converted to "" in the output slice. Strings returns an error if an array
// item is not a bulk string or nil.
func Strings(reply interface{}, err error) ([]string, error) {
	return redis.Strings(reply, err)
}

// ByteSlices is a helper that converts an array command reply to a [][]byte.
// If err is not equal to nil, then ByteSlices returns nil, err. Nil array
// items are stay nil. ByteSlices returns an error if an array item is not a
// bulk string or nil.
func ByteSlices(reply interface{}, err error) ([][]byte, error) {
	return redis.ByteSlices(reply, err)
}

// Int64s is a helper that converts an array command reply to a []int64.
// If err is not equal to nil, then Int64s returns nil, err. Nil array
// items are stay nil. Int64s returns an error if an array item is not a
// bulk string or nil.
func Int64s(reply interface{}, err error) ([]int64, error) {
	return redis.Int64s(reply, err)
}

// Ints is a helper that converts an array command reply to a []in.
// If err is not equal to nil, then Ints returns nil, err. Nil array
// items are stay nil. Ints returns an error if an array item is not a
// bulk string or nil.
func Ints(reply interface{}, err error) ([]int, error) {
	return redis.Ints(reply, err)
}

// StringMap is a helper that converts an array of strings (alternating key,
// value) into a map[string]string.
// Requires an even number of values in result.
func StringMap(reply interface{}, err error) (map[string]string, error) {
	return redis.StringMap(reply, err)
}

// IntMap is a helper that converts an array of strings (alternating key, value)
// into a map[string]int.
func IntMap(reply interface{}, err error) (map[string]int, error) {
	return redis.IntMap(reply, err)
}

// Int64Map is a helper that converts an array of strings (alternating key,
// value) into a map[string]int64. The HGETALL commands return replies in this
// format.
// Requires an even number of values in result.
func Int64Map(reply interface{}, err error) (map[string]int64, error) {
	return redis.Int64Map(reply, err)
}

// Uint64s is a helper that converts an array command reply to a []uint64.
// If err is not equal to nil, then Uint64s returns nil, err. Nil array
// items are stay nil. Uint64s returns an error if an array item is not a
// bulk string or nil.
func Uint64s(reply interface{}, err error) ([]uint64, error) {
	return redis.Uint64s(reply, err)
}

// Uint64Map is a helper that converts an array of strings (alternating key,
// value) into a map[string]uint64.
// Requires an even number of values in result.
func Uint64Map(reply interface{}, err error) (map[string]uint64, error) {
	return redis.Uint64Map(reply, err)
}

// ValueMap is a helper that converts an array of values (alternating key,
// value) into a map[string]interface{}.
// Requires an even number of values in result.
func ValueMap(reply interface{}, err error) (map[string]interface{}, error) {
	values, err := redis.Values(reply, err)
	if err != nil {
		return nil, err
	}
	if len(values)%2 != 0 {
		return nil, errors.New("redigo: ValueMap expects even number of " +
			"values result")
	}
	m := make(map[string]interface{}, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		key, ok := values[i].([]byte)
		if !ok {
			return nil, errors.New("redigo: ValueMap key not a bulk " +
				"string value")
		}
		m[string(key)] = values[i+1]
	}
	return m, nil
}
