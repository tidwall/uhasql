package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/tidwall/gjson"
	"github.com/tidwall/redcon"
	"github.com/tidwall/uhaha"
)

// #cgo LDFLAGS: -Lsqlite -lsqlite
// #include "uhaha.c"
// extern int64_t uhaha_seed;
// extern int64_t uhaha_ts;
// // hello jelloasfasdfasdf
import "C"

var dbmu sync.Mutex
var disableCheckpoints bool
var dbPath string

type client struct {
	tx   bool
	sqls []string
	ros  []bool
	sv   *sqlValidator
}

func (c *client) reset() {
	c.tx = false
	c.sqls = []string{}
	c.ros = []bool{}
}

func main() {
	var conf uhaha.Config
	conf.Name = "uhasql"
	conf.Version = "0.1.0"
	conf.DataDirReady = func(dir string) {
		os.RemoveAll(filepath.Join(dir, "db"))
		os.Mkdir(filepath.Join(dir, "db"), 0777)
		dbPath = filepath.Join(dir, "db", "sqlite.db")
		cs := C.CString(dbPath)
		C.db_open(cs)
		C.free(unsafe.Pointer(cs))
	}
	conf.Tick = tick
	conf.Snapshot = snapshot
	conf.Restore = restore

	conf.ConnOpened = func(addr string) (context interface{}, accept bool) {
		c := new(client)
		c.sv = newSQLValidator()
		c.reset()
		return c, true
	}
	conf.ConnClosed = func(context interface{}, addr string) {
		c := context.(*client)
		c.sv.release()
	}

	stmts := []string{
		"ALTER", "ANALYZE", "ATTACH", "CREATE", "DELETE", "DETACH", "DROP",
		"EXPLAIN", "INDEXED", "INSERT", "ON", "REINDEX", "REPLACE", "SELECT",
		"UPDATE", "UPSERT", "WITH",
	}
	for _, stmt := range stmts {
		conf.AddPassiveCommand(stmt, cmdSQL)
	}
	conf.AddPassiveCommand("BEGIN", cmdBEGIN)
	conf.AddPassiveCommand("END", cmdEND)
	conf.AddPassiveCommand("COMMIT", cmdEND)
	conf.AddPassiveCommand("ROLLBACK", cmdEND)
	conf.AddWriteCommand("$EXEC", cmdEXEC)
	conf.AddReadCommand("$QUERY", cmdQUERY)
	uhaha.Main(conf)
}

func tick(m uhaha.Machine) {
	var info uhaha.RawMachineInfo
	uhaha.ReadRawMachineInfo(m, &info)
	C.uhaha_seed = C.int64_t(info.Seed)
	C.uhaha_ts = C.int64_t(info.TS)
	if !disableCheckpoints {
		C.db_checkpoint()
	}
}

func cmdSQL(m uhaha.Machine, args []string) (interface{}, error) {
	c := m.Context().(*client)
	sql := strings.Join(args, " ")
	ro, err := c.sv.validate(sql)
	if err != nil {
		return nil, err
	}
	if c.tx {
		c.sqls = append(c.sqls, sql)
		c.ros = append(c.ros, ro)
		return redcon.SimpleString("QUEUED"), nil
	}
	data, _ := json.Marshal(sql)
	if ro {
		args = []string{"$QUERY", string(data)}

	} else {
		args = []string{"$EXEC", string(data)}
	}
	fmt.Printf("%v\n", args)
	return uhaha.FilterArgs(args), nil
}

func cmdEXEC(m uhaha.Machine, args []string) (interface{}, error) {
	// Take special care to keep the the machine random and time state
	// updated for write commands.
	var info uhaha.RawMachineInfo
	uhaha.ReadRawMachineInfo(m, &info)
	defer func() {
		info.TS = int64(C.uhaha_ts)
		info.Seed = int64(C.uhaha_seed)
		uhaha.WriteRawMachineInfo(m, &info)
	}()
	return exec(args[1], true)
}

func cmdQUERY(m uhaha.Machine, args []string) (interface{}, error) {
	return exec(args[1], false)
}

func cmdBEGIN(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, uhaha.ErrWrongNumArgs
	}
	c := m.Context().(*client)
	if c.tx {
		return nil, errors.New("nested transactions are not supported")
	}
	c.tx = true
	return redcon.SimpleString("OK"), nil
}

func cmdEND(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, uhaha.ErrWrongNumArgs
	}
	c := m.Context().(*client)
	if !c.tx {
		return nil, errors.New("transaction not started")
	}
	if strings.ToLower(args[0]) == "rollback" {
		c.reset()
		return redcon.SimpleString("OK"), nil
	}
	data, _ := json.Marshal(c.sqls)
	ro := true
	for i := 0; i < len(c.ros); i++ {
		if !c.ros[i] {
			ro = false
			break
		}
	}
	c.reset()
	if ro {
		return uhaha.FilterArgs([]string{"$QUERY", string(data)}), nil
	}
	return uhaha.FilterArgs([]string{"$EXEC", string(data)}), nil
}

func parseExecRes(res string) ([][]interface{}, error) {
	var rows [][]interface{}
	for {
		if len(res) == 0 {
			break
		}
		var row []interface{}
		for {
			if len(res) == 0 {
				break
			}
			idx := strings.IndexByte(res, '.')
			if idx == -1 {
				if len(res) == 0 {
					return nil, errors.New("invalid response A")
				}
				if res[0] == '\n' {
					res = res[1:]
					break
				}
				return nil, errors.New("invalid response AA")
			}
			str := res[:idx]
			res = res[idx+1:]
			nbytes, err := strconv.Atoi(str)
			if err != nil {
				return nil, errors.New("invalid response B")
			}
			if len(res) < nbytes {
				return nil, errors.New("invalid response C")
			}
			if nbytes == 0 {
				row = append(row, nil)
			} else {
				row = append(row, res[:nbytes-1])
			}
			res = res[nbytes:]
			if len(res) == 0 {
				return nil, errors.New("invalid response D")
			}
			if res[0] == '\n' {
				res = res[1:]
				break
			}
			if res[0] != '|' {
				return nil, errors.New("invalid response E")
			}
			res = res[1:]
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func dbexec(sql string) (rows [][]interface{}, err error) {
	csql := C.CString(sql)
	errmsg := C.GoString(C.db_exec(csql))
	C.free(unsafe.Pointer(csql))
	if errmsg != "" {
		return nil, errors.New(errmsg)
	}
	return parseExecRes(C.GoStringN(C.result, C.result_len))
}

func exec(sqlJSON string, write bool) (interface{}, error) {
	var sqls []string
	var tx bool
	val := gjson.Parse(sqlJSON)
	if val.Type == gjson.String {
		sqls = append(sqls, val.String())
		tx = false
	} else {
		val.ForEach(func(_, val gjson.Result) bool {
			sqls = append(sqls, val.String())
			return true
		})
		tx = true
	}
	res := make([]interface{}, len(sqls))
	if !write {
		// read commands need to take care to reset the machine state back to
		// where it started.
		dbmu.Lock()
		ts := C.uhaha_ts
		seed := C.uhaha_seed
		defer func() {
			C.uhaha_ts = ts
			C.uhaha_seed = seed
			dbmu.Unlock()
		}()
	}
	if tx {
		if _, err := dbexec("begin"); err != nil {
			return nil, err
		}
	}
	for i, sql := range sqls {
		rows, err := dbexec(sql)
		if err != nil {
			if !tx {
				return nil, err
			}
			if _, err := dbexec("rollback"); err != nil {
				return nil, err
			}
			res[i] = err
			i++
			for ; i < len(sqls); i++ {
				res[i] = errors.New("transaction rolledback")
			}
			return res, nil
		}
		res[i] = rows
	}
	if tx {
		if _, err := dbexec("commit"); err != nil {
			return nil, err
		}
		return res, nil
	}
	return res[0], nil
}

type snap struct{}

func (s *snap) Done(path string) {
	disableCheckpoints = false
}

func (s *snap) Persist(wr io.Writer) error {
	f, err := os.Open(dbPath)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(wr, f)
	return err
}

func snapshot(_ interface{}) (uhaha.Snapshot, error) {
	disableCheckpoints = true
	C.db_checkpoint()
	return &snap{}, nil
}

func restore(rd io.Reader) (interface{}, error) {
	C.db_close()
	if err := os.RemoveAll(filepath.Dir(dbPath)); err != nil {
		return nil, err
	}
	if err := os.Mkdir(filepath.Dir(dbPath), 0777); err != nil {
		return nil, err
	}
	f, err := os.Create(dbPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if _, err := io.Copy(f, rd); err != nil {
		return nil, err
	}
	cs := C.CString(dbPath)
	C.db_open(cs)
	C.free(unsafe.Pointer(cs))
	return nil, nil
}

func must(v interface{}, err error) interface{} {
	if err != nil {
		panic(err)
	}
	return v
}

type sqlValidator struct {
	db *C.sqlite3
}

// newSQLValidator is used to validate sqlite statements prior to sending to a
// sqlite database instance. The validator must be released when no longer in
// use.
func newSQLValidator() *sqlValidator {
	sv := new(sqlValidator)
	cpath := C.CString(":memory:")
	rc := C.sqlite3_open(cpath, &sv.db)
	C.free(unsafe.Pointer(cpath))
	if rc != C.SQLITE_OK {
		panic(errors.New(C.GoString(C.sqlite3_errmsg(sv.db))))
	}
	return sv
}

func (sv *sqlValidator) release() {
	rc := C.sqlite3_close(sv.db)
	if rc != C.SQLITE_OK {
		panic(errors.New(C.GoString(C.sqlite3_errmsg(sv.db))))
	}
	sv.db = nil
}

func (sv *sqlValidator) validate(sql string) (readonly bool, err error) {
	sql = strings.TrimSpace(sql)
	var stmt *C.sqlite3_stmt
	var tail *C.char
	csql := C.CString(sql)
	rc := C.sqlite3_prepare_v2(sv.db, csql, C.int(len(sql)), &stmt, &tail)
	C.free(unsafe.Pointer(csql))
	if tail != nil && strings.TrimSpace(C.GoString(tail)) != "" {
		return false, errors.New("too much input")
	}
	if rc != C.SQLITE_OK {
		errmsg := C.GoString(C.sqlite3_errmsg(sv.db))
		var ok bool
		if !ok && strings.HasPrefix(errmsg, "no such table: ") {
			ok = true
		}
		if !ok {
			return false, errors.New(errmsg)
		}
	} else {
		readonly = C.sqlite3_stmt_readonly(stmt) != 0
	}
	rc = C.sqlite3_finalize(stmt)
	if rc != C.SQLITE_OK {
		return false, errors.New(C.GoString(C.sqlite3_errmsg(sv.db)))
	}
	if !readonly {
		keyword := sql
		idx := strings.IndexAny(sql, " \t\n\v\f\r")
		if idx != -1 {
			keyword = sql[:idx]
		}
		if strings.ToLower(keyword) == "select" {
			readonly = true
		}
	}
	return readonly, nil
}
