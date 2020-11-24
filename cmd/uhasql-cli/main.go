package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/peterh/liner"
	"github.com/tidwall/uhatools"
)

var scriptMultilineMode bool
var scriptLines string
var scriptLinesPrefix string

func main() {
	var host string
	var port int
	var auth string
	var cacert string
	var tlsinsecure bool
	flag.StringVar(&host, "h", "127.0.0.1", "host")
	flag.IntVar(&port, "p", 11001, "port")
	flag.StringVar(&auth, "a", "", "auth")
	flag.BoolVar(&tlsinsecure, "tlsinsecure", false,
		"Use insecure TLS connection")
	flag.StringVar(&cacert, "cacert", "", "")
	flag.Parse()
	var tlscfg *tls.Config
	if tlsinsecure {
		tlscfg = &tls.Config{
			InsecureSkipVerify: true,
		}
	} else if cacert != "" {
		var serverName string
		tlscfg = &tls.Config{
			InsecureSkipVerify: true,
			VerifyConnection: func(cs tls.ConnectionState) error {
				if len(cs.PeerCertificates) > 0 {
					if len(cs.PeerCertificates[0].DNSNames) > 0 {
						serverName = cs.PeerCertificates[0].DNSNames[0]
						return nil
					}
				}
				return errors.New(
					"tls: cannot verify because no IP SANs could be determined")
			},
		}
		conn, err := tls.Dial("tcp", fmt.Sprintf("%s:%d", host, port), tlscfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			return
		}
		conn.Close()

		data, err := ioutil.ReadFile(cacert)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			return
		}
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(data)
		tlscfg = &tls.Config{
			ServerName: serverName,
			RootCAs:    pool,
		}
	}
	conn, err := uhatools.Dial(fmt.Sprintf("%s:%d", host, port),
		&uhatools.DialOptions{
			Auth:      auth,
			TLSConfig: tlscfg,
		})
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		return
	}
	defer conn.Close()

	pong, err := uhatools.String(conn.Do("ping"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		return
	}
	if pong != "PONG" {
		fmt.Fprintf(os.Stderr, "bad connection")
		return
	}

	line := liner.NewLiner()
	line.SetCtrlCAborts(true)

	var histPath string
	if udir, err := os.UserHomeDir(); err == nil {
		histPath = filepath.Join(udir, ".uhasql_history")
		if f, err := os.Open(histPath); err == nil {
			line.ReadHistory(f)
			f.Close()
		}
	}
	defer func() {
		if histPath != "" {
			if f, err := os.Create(histPath); err == nil {
				line.WriteHistory(f)
				f.Close()
			}
		}
		line.Close()
	}()

	var sql string
	for {
		var str string
		var err error
		if sql == "" && !scriptMultilineMode {
			str, err = line.Prompt("uhasql> ")
		} else {
			str, err = line.Prompt("   ...> ")
		}
		if err != nil {
			return
		}
		line.AppendHistory(str)

		if scriptMultilineMode {
			if str == "```" {
				doProcSetCommand(conn, scriptLines)
			} else {
				scriptLines += str + "\n"
			}
		} else {
			str = strings.TrimSpace(str)
			if sql == "" && strings.HasPrefix(str, ".") {
				// do sys command
				if doSysCommand(conn, str) {
					return
				}
			} else if (strings.IndexByte(str, ' ') != -1 &&
				strings.ToLower(str[:strings.IndexByte(str, ' ')]) == "proc") ||
				(len(str) == 4 && strings.ToLower(str) == "proc") {
				// do proc command
				doProcCommand(conn, str)
			} else {
				// do sql command
				sql = strings.TrimSpace(sql + "\n" + str)
				if strings.HasSuffix(sql, ";") {
					sql = sql[:len(sql)-1]
					v, err := conn.Do("$any", sql)
					sql = ""
					if err != nil {
						fmt.Fprintf(os.Stderr, "Error: %s\n", cleanErr(err))
						continue
					}
					writeResultSets(v)
				}
			}
		}
	}
}

func writeResultSets(v interface{}) {
	rss, ok := v.([]interface{})
	if !ok {
		if v == nil {
			return
		}
		fmt.Printf("%s\n", v)
		return
	}
	for i := 0; i < len(rss); i++ {
		writeResultSet(rss[i], i == len(rss)-1)
	}
}

func writeResultSet(v interface{}, last bool) {
	vv, ok := v.([]interface{})
	if !ok {
		if v == nil {
			return
		}
		fmt.Printf("%s\n", v)
		return
	}
	rs := make([][]string, len(vv))
	var colsz []int
	var nlines int
	for i := 0; i < len(vv); i++ {
		cols, _ := uhatools.Strings(vv[i], nil)
		rs[i] = cols
		for j := 0; j < len(cols); j++ {
			n := len(cols[j])
			if j == len(colsz) {
				colsz = append(colsz, n)
			} else if n > colsz[j] {
				colsz[j] = n
			}
		}
	}
	for i := 0; i < len(rs); i++ {
		cols := rs[i]
		if len(cols) == 0 {
			continue
		}
		for j := 0; j < len(cols); j++ {
			sz := colsz[j]
			col := (cols[j] + strings.Repeat(" ", sz))[:sz]
			fmt.Printf("%s  ", col)
		}
		fmt.Printf("\n")
		nlines++
		if i == 0 {
			for j := 0; j < len(cols); j++ {
				sz := colsz[j]
				fmt.Printf("%s  ", strings.Repeat("-", sz))
			}
			fmt.Printf("\n")
			nlines++
		}
	}
	if !last {
		fmt.Printf("\n")
	}
}

func cleanErr(err error) error {
	if err == nil {
		return nil
	}
	errmsg := err.Error()
	if strings.HasPrefix(errmsg, "ERR ") {
		return errors.New(errmsg[4:])
	}
	return err
}

func doSysCommand(conn *uhatools.Conn, cmd string) (exit bool) {
	args := strings.Split(cmd, " ")
	for i := 0; i < len(args); i++ {
		if args[i] == "" {
			args = append(args[:i], args[i+1:]...)
			i--
			continue
		}
	}
	switch strings.ToLower(args[0]) {
	case ".help":
		fmt.Printf(".exit                        Exit the process\n")
		fmt.Printf(".help                        Show this screen\n")
		fmt.Printf(".version                     Show the UhaSQL version\n")
	case ".exit":
		return true
	case ".version":
		vers, err := uhatools.String(conn.Do("version"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %s\n", cleanErr(err))
		} else {
			fmt.Printf("%s\n", vers)
		}
	default:
		fmt.Fprintf(os.Stderr,
			"Error: unknown command or invalid arguments:  "+
				"\"%s\". Enter \".help\" for help\n", args[0][1:])
	}
	return false
}

func doProcSetCommand(conn *uhatools.Conn, cmd string) {
	scriptMultilineMode = false
	cmd = scriptLinesPrefix + strconv.Quote(cmd[len(scriptLinesPrefix):])
	args, err := readArgs(cmd)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s", err)
		return
	}
	var vargs []interface{}
	for _, arg := range args {
		vargs = append(vargs, arg)
	}
	_, err = conn.Do(args[0], vargs[1:]...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", cleanErr(err))
		return
	}
}

func doProcCommand(conn *uhatools.Conn, cmd string) {
	lcmd := strings.ToLower(cmd)
	switch {
	case strings.HasPrefix(lcmd, "proc set "):
		args, err := readArgs(cmd)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %s", err)
			return
		}
		if len(args) != 4 || args[3] != "```" ||
			!strings.HasSuffix(cmd, " ```") {
			fmt.Fprintf(os.Stderr, "Error: invalid format\n")
			return
		}
		scriptLines = cmd[:len(cmd)-3] + " "
		scriptLinesPrefix = scriptLines
		scriptMultilineMode = true
	default:
		for cmd[len(cmd)-1] == ';' {
			cmd = cmd[:len(cmd)-1]
		}
		args, err := readArgs(cmd)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %s", err)
			return
		}
		var vargs []interface{}
		for _, arg := range args {
			vargs = append(vargs, arg)
		}
		resp, err := conn.Do(args[0], vargs[1:]...)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %s\n", cleanErr(err))
			return
		}
		switch strings.ToLower(args[1]) {
		case "list":
			names, err := uhatools.Strings(resp, nil)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %s\n", cleanErr(err))
				return
			}
			fmt.Printf("proc\n")
			n := 4
			for _, name := range names {
				if len(name) > n {
					n = len(name)
				}
			}
			fmt.Printf("%s\n", strings.Repeat("-", n))
			for _, name := range names {
				fmt.Printf("%s\n", name)
			}
		case "get":
			if resp == nil {
				return
			}
			script, err := uhatools.String(resp, nil)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %s\n", cleanErr(err))
				return
			}
			fmt.Printf("%s\n", script)
		case "delete", "del":
			_, err := uhatools.String(resp, nil)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %s\n", cleanErr(err))
				return
			}
		case "exec":
			writeResultSets([]interface{}{resp})
		default:
			fmt.Printf("%v\n", resp)
		}

	}

	// fmt.Printf("%v %v\n", args, err)

}

var errUnbalancedQuotes = errors.New("unbalanced quotes")

func readArgs(packet string) ([]string, error) {
	var args []string
	// just a plain text command
	for i := 0; i < len(packet); i++ {
		if packet[i] == '\n' || i == len(packet)-1 {
			var line string
			if i == len(packet)-1 {
				line = packet
			} else if i > 0 && packet[i-1] == '\r' {
				line = packet[:i-1]
			} else {
				line = packet[:i]
			}
			var quote bool
			var quotech byte
			var escape bool
		outer:
			for {
				nline := make([]byte, 0, len(line))
				for i := 0; i < len(line); i++ {
					c := line[i]
					if !quote {
						if c == ' ' {
							if len(nline) > 0 {
								args = append(args, string(nline))
							}
							line = line[i+1:]
							continue outer
						}
						if c == '"' || c == '\'' {
							if i != 0 {
								return nil, errUnbalancedQuotes
							}
							quotech = c
							quote = true
							line = line[i+1:]
							continue outer
						}
					} else {
						if escape {
							escape = false
							switch c {
							case 'n':
								c = '\n'
							case 'r':
								c = '\r'
							case 't':
								c = '\t'
							}
						} else if c == quotech {
							quote = false
							quotech = 0
							args = append(args, string(nline))
							line = line[i+1:]
							if len(line) > 0 && line[0] != ' ' {
								return nil, errUnbalancedQuotes
							}
							continue outer
						} else if c == '\\' {
							escape = true
							continue
						}
					}
					nline = append(nline, c)
				}
				if quote {
					return nil, errUnbalancedQuotes
				}
				if len(line) > 0 {
					args = append(args, line)
				}
				break
			}
			return args, nil
		}
	}
	return args, nil
}
