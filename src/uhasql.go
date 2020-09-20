package main

import (
	"encoding/json"
	"errors"
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
// #include "sqlite/sqlite.h"
// #include <stdint.h>
// #include <stdlib.h>
// extern int64_t uhaha_seed;
// extern int64_t uhaha_ts;
import "C"

var dbmu sync.Mutex
var dbPath string
var db *sqlDatabase

var errTooMuchInput = errors.New("too much input")

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
		db = must(openSQLDatabase(dbPath)).(*sqlDatabase)
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
	conf.AddWriteCommand("$EXEC", cmdEXEC)
	conf.AddReadCommand("$QUERY", cmdQUERY)
	conf.AddCatchallCommand(cmdANY)
	uhaha.Main(conf)
}

func tick(m uhaha.Machine) {
	var info uhaha.RawMachineInfo
	uhaha.ReadRawMachineInfo(m, &info)
	C.uhaha_seed = C.int64_t(info.Seed)
	C.uhaha_ts = C.int64_t(info.TS)
}

func cmdANY(m uhaha.Machine, args []string) (interface{}, error) {
	// PASSIVE
	sql := strings.TrimSpace(strings.Join(args, " "))
	keyword := sqlKeyword(sql)
	remain := strings.TrimSpace(sql[len(keyword):])
	if len(remain) == 0 || remain == ";" {
		args = []string{keyword}
	} else {
		args = []string{keyword, remain}
	}
	switch keyword {
	case "alter", "analyze", "attach", "create", "delete", "detach", "drop",
		"explain", "indexed", "insert", "on", "reindex", "replace", "select",
		"update", "upsert", "with":
		return cmdSQL(m, args)
	case "begin":
		return cmdBEGIN(m, args)
	case "end", "commit", "rollback":
		return cmdEND(m, args)
	}
	return nil, uhaha.ErrUnknownCommand
}

func cmdSQL(m uhaha.Machine, args []string) (interface{}, error) {
	// PASSIVE
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
	return uhaha.FilterArgs(args), nil
}

func cmdBEGIN(m uhaha.Machine, args []string) (interface{}, error) {
	// PASSIVE
	if len(args) != 1 {
		return nil, errTooMuchInput
	}
	c := m.Context().(*client)
	if c.tx {
		return nil, errors.New("nested transactions are not supported")
	}
	c.tx = true
	return redcon.SimpleString("OK"), nil
}

func cmdEND(m uhaha.Machine, args []string) (interface{}, error) {
	// PASSIVE
	if len(args) != 1 {
		return nil, errTooMuchInput
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

func cmdEXEC(m uhaha.Machine, args []string) (interface{}, error) {
	// WRITE
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
	// READ
	return exec(args[1], false)
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
	dbmu.Lock()
	defer dbmu.Unlock()
	if !write {
		// read commands need to take care to reset the machine state back to
		// where it started.
		ts := C.uhaha_ts
		seed := C.uhaha_seed
		defer func() {
			C.uhaha_ts = ts
			C.uhaha_seed = seed
		}()
	}
	if tx {
		if err := db.exec("begin", nil); err != nil {
			return nil, err
		}
	}
	for i, sql := range sqls {
		var rows [][]string
		err := db.exec(sql, func(row []string) bool {
			rows = append(rows, row)
			return true
		})
		if err != nil {
			if !tx {
				return nil, err
			}
			if err := db.exec("rollback", nil); err != nil {
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
		if err := db.exec("commit", nil); err != nil {
			return nil, err
		}
		return res, nil
	}
	return res[0], nil
}

type snap struct{}

func (s *snap) Done(path string) {
	dbmu.Lock()
	defer dbmu.Unlock()
	must(nil, db.checkpoint())
	must(nil, db.autocheckpoint(1000))
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
	if err := db.autocheckpoint(0); err != nil {
		return nil, err
	}
	if err := db.checkpoint(); err != nil {
		return nil, err
	}
	return &snap{}, nil
}

func restore(rd io.Reader) (interface{}, error) {
	if err := db.close(); err != nil {
		return nil, err
	}
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
	db, err = openSQLDatabase(dbPath)
	return nil, err
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

func sqlKeyword(sql string) string {
	keyword := sql
	idx := strings.IndexAny(keyword, " \t\n\v\f\r;")
	if idx != -1 {
		keyword = keyword[:idx]
	}
	return strings.ToLower(keyword)
}

// validate a sql statement and determine if it's readonly. The readonly return
// value is only a hint but it won't have have false positives.
func (sv *sqlValidator) validate(sql string) (readonly bool, err error) {
	sql = strings.TrimSpace(sql)
	var stmt *C.sqlite3_stmt
	var tail *C.char
	csql := C.CString(sql)
	rc := C.sqlite3_prepare_v2(sv.db, csql, C.int(len(sql)), &stmt, &tail)
	C.free(unsafe.Pointer(csql))
	defer func() {
		C.sqlite3_finalize(stmt)
	}()
	if tail != nil && strings.TrimSpace(C.GoString(tail)) != "" {
		return false, errTooMuchInput
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
	if !readonly {
		if sqlKeyword(sql) == "select" {
			readonly = true
		}
	}
	return readonly, nil
}

type sqlDatabase struct {
	db *C.sqlite3
}

func (db *sqlDatabase) close() error {
	if db.db == nil {
		return errors.New("database closed")
	}
	C.sqlite3_close(db.db)
	db.db = nil
	return nil
}

func openSQLDatabase(path string) (*sqlDatabase, error) {
	db := new(sqlDatabase)
	cstr := C.CString(path)
	rc := C.sqlite3_open(cstr, &db.db)
	C.free(unsafe.Pointer(cstr))
	if rc != C.SQLITE_OK {
		return nil, errors.New(C.GoString(C.sqlite3_errmsg(db.db)))
	}
	if err := db.exec("PRAGMA journal_mode=WAL", nil); err != nil {
		db.close()
		return nil, err
	}
	return db, nil
}

func (db *sqlDatabase) exec(sql string, iter func(row []string) bool) error {
	if db.db == nil {
		return errors.New("database closed")
	}
	var stmt *C.sqlite3_stmt
	csql := C.CString(sql)
	rc := C.sqlite3_prepare_v2(db.db, csql, C.int(len(sql)), &stmt, nil)
	C.free(unsafe.Pointer(csql))
	if rc != C.SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errmsg(db.db)))
	}
	ncols := int(C.sqlite3_column_count(stmt))
	row := make([]string, ncols)
	for i := 0; i < ncols; i++ {
		row[i] = C.GoString(C.sqlite3_column_name(stmt, C.int(i)))
	}
	if iter == nil || iter(row) {
		for {
			rc := C.sqlite3_step(stmt)
			if rc == C.SQLITE_DONE {
				break
			}
			if rc == C.SQLITE_ROW {
				row := make([]string, ncols)
				for i := 0; i < ncols; i++ {
					text := C.sqlite3_column_text(stmt, C.int(i))
					row[i] = C.GoString((*C.char)(unsafe.Pointer(text)))
				}
				if iter != nil && !iter(row) {
					break
				}
			}
		}
	}
	rc = C.sqlite3_finalize(stmt)
	if rc != C.SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errmsg(db.db)))
	}
	return nil
}

func (db *sqlDatabase) checkpoint() error {
	rc := C.sqlite3_wal_checkpoint_v2(db.db, nil,
		C.SQLITE_CHECKPOINT_TRUNCATE, nil, nil)
	if rc != C.SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errmsg(db.db)))
	}
	return nil
}

func (db *sqlDatabase) autocheckpoint(n int) error {
	rc := C.sqlite3_wal_autocheckpoint(db.db, C.int(n))
	if rc != C.SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errmsg(db.db)))
	}
	return nil
}
