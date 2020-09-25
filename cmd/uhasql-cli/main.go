package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/peterh/liner"
	"github.com/tidwall/uhatools"
)

func main() {
	var host string
	var port int
	var auth string
	flag.StringVar(&host, "h", "127.0.0.1", "host")
	flag.IntVar(&port, "p", 11001, "port")
	flag.StringVar(&auth, "a", "", "auth")
	flag.Parse()
	conn, err := uhatools.Dial(fmt.Sprintf("%s:%d", host, port),
		&uhatools.DialOptions{
			Auth: auth,
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
		if sql == "" {
			str, err = line.Prompt("uhasql> ")
		} else {
			str, err = line.Prompt("   ...> ")
		}
		if err != nil {
			return
		}
		line.AppendHistory(str)
		str = strings.TrimSpace(str)
		if sql == "" && strings.HasPrefix(str, ".") {
			// do sys command
			doSysCommand(conn, str)
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

func writeResultSets(v interface{}) {
	rss, _ := v.([]interface{})
	for i := 0; i < len(rss); i++ {
		writeResultSet(rss[i])
	}
}

func writeResultSet(v interface{}) {
	vv, _ := v.([]interface{})
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
	if nlines > 0 {
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

func doSysCommand(conn *uhatools.Conn, cmd string) {
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
		fmt.Printf(".help                        Show this screen\n")
	default:
		fmt.Fprintf(os.Stderr,
			"Error: unknown command or invalid arguments:  "+
				"\"%s\". Enter \".help\" for help\n", args[0][1:])
	}
}
