package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"unsafe"

	"github.com/tidwall/gjson"
	"github.com/tidwall/uhaha"
)

// #cgo LDFLAGS: -L../../sqlite -lsqlite -ldl
// #include "../../sqlite/sqlite.h"
// #include <stdint.h>
// #include <stdlib.h>
// extern int64_t uhaha_seed;
// extern int64_t uhaha_ts;
// void uhaha_begin_reader();
// void uhaha_end_reader();
import "C"

var buildVersion string
var buildGitSHA string

var dbmu sync.RWMutex
var dbPath string
var wdb *sqlDatabase

var errTooMuchInput = errors.New("too much input")

func main() {
	var conf uhaha.Config
	conf.Name = "uhasql-server"
	conf.Version = strings.Replace(buildVersion, "v", "", -1)
	conf.GitSHA = buildGitSHA
	conf.DataDirReady = func(dir string) {
		os.RemoveAll(filepath.Join(dir, "db"))
		os.Mkdir(filepath.Join(dir, "db"), 0777)
		dbPath = filepath.Join(dir, "db", "sqlite.db")
		wdb = must(openSQLDatabase(dbPath, false)).(*sqlDatabase)
	}
	conf.Tick = tick
	conf.Snapshot = snapshot
	conf.Restore = restore

	// Do not call $EXEC, $QUERY, or $ANY directly.
	conf.AddWriteCommand("$EXEC", cmdEXEC)
	conf.AddReadCommand("$QUERY", cmdQUERY)
	conf.AddPassiveCommand("$ANY", cmdANY)
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
	if strings.ToLower(args[0]) == "$any" {
		args = args[1:]
		if len(args) == 0 {
			return nil, uhaha.ErrWrongNumArgs
		}
	}
	sql := strings.TrimSpace(strings.Join(args, " "))

	readonly := true
	var txbegan bool
	var txended bool
	var err error
	stmts := []string{}
	sqlForEachStatement(sql, func(sql string) bool {
		cmd := sqlCommand(sql)
		switch cmd {
		case "alter", "analyze", "create", "delete", "drop", "insert",
			"reindex", "replace", "update", "upsert":
			// write command
			readonly = false
		case "explain", "select":
			// readonly command
		case "begin":
			if len(sql) != len(cmd) {
				err = errTooMuchInput
				return false
			}
			if txbegan {
				err = errors.New("nested transactions are not supported")
				return false
			}
			if len(stmts) > 0 {
				err = errors.New("\"begin\" must be the first statement")
				return false
			}
			txbegan = true
			return true
		case "end":
			if !txbegan {
				err = errors.New("\"end\" missing \"begin\"")
				return false
			}
			if len(sql) != len(cmd) {
				err = errTooMuchInput
				return false
			}
			txended = true
			return true
		default:
			err = fmt.Errorf("near \"%s\": syntax error", cmd)
			return false
		}
		if txended {
			err = errors.New("\"end\" must be the last statement")
			return false
		}
		stmts = append(stmts, sql)
		return true
	})
	if err != nil {
		return nil, err
	}
	if txbegan && !txended {
		return nil, errors.New("\"begin\" without \"end\"")
	}
	if len(stmts) == 0 {
		if txbegan {
			return [][]string{[]string{}, []string{}}, nil
		}
		return []string{}, nil
	}
	vals := map[string]interface{}{
		"tx":    txbegan,
		"stmts": stmts,
	}
	data, _ := json.Marshal(vals)
	if readonly {
		args = []string{"$QUERY", string(data)}
	} else {
		args = []string{"$EXEC", string(data)}
	}
	return uhaha.FilterArgs(args), nil
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
	return exec(args[1], false)
}

func cmdQUERY(m uhaha.Machine, args []string) (interface{}, error) {
	// READ
	return exec(args[1], true)
}

func exec(sqlJSON string, readonly bool) (interface{}, error) {
	var sqls []string
	tx := gjson.Get(sqlJSON, "tx").Bool()
	var res []interface{}
	gjson.Get(sqlJSON, "stmts").ForEach(func(_, val gjson.Result) bool {
		sqls = append(sqls, val.String())
		return true
	})
	var db *sqlDatabase
	if readonly {
		var err error
		db, err = takeReaderDB()
		if err != nil {
			return nil, err
		}
		defer releaseReaderDB(db)

		dbmu.RLock()
		C.uhaha_begin_reader()
		defer func() {
			C.uhaha_end_reader()
			dbmu.RUnlock()
		}()
	} else {
		dbmu.Lock()
		defer dbmu.Unlock()
		db = wdb
	}

	if len(sqls) > 1 {
		if err := db.exec("begin", nil); err != nil {
			return nil, err
		}
	}
	if tx {
		res = append(res, [][]string{[]string{}})
	}
	for _, sql := range sqls {
		var rows [][]string
		err := db.exec(sql, func(row []string) bool {
			rows = append(rows, row)
			return true
		})
		if err != nil {
			if len(sqls) > 1 {
				if err := db.exec("rollback", nil); err != nil {
					return nil, err
				}
			}
			return nil, err
		}
		res = append(res, rows)
	}
	if tx {
		res = append(res, [][]string{[]string{}})
	}
	if len(sqls) > 1 {
		if err := db.exec("end", nil); err != nil {
			return nil, err
		}
	}
	return res, nil
}

type snap struct{}

func (s *snap) Done(path string) {
	dbmu.Lock()
	defer dbmu.Unlock()
	must(nil, wdb.checkpoint())
	must(nil, wdb.autocheckpoint(1000))
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
	dbmu.Lock()
	defer dbmu.Unlock()
	if err := wdb.autocheckpoint(0); err != nil {
		return nil, err
	}
	if err := wdb.checkpoint(); err != nil {
		return nil, err
	}
	return &snap{}, nil
}

func restore(rd io.Reader) (interface{}, error) {
	dbmu.Lock()
	defer dbmu.Unlock()
	if err := wdb.close(); err != nil {
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
	wdb, err = openSQLDatabase(dbPath, false)
	return nil, err
}

func must(v interface{}, err error) interface{} {
	if err != nil {
		panic(err)
	}
	return v
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

func openSQLDatabase(path string, readonly bool) (*sqlDatabase, error) {
	db := new(sqlDatabase)
	cstr := C.CString(path)
	var rc C.int
	if readonly {
		rc = C.sqlite3_open_v2(cstr, &db.db, C.SQLITE_OPEN_READONLY, nil)
	} else {
		rc = C.sqlite3_open(cstr, &db.db)
	}
	C.free(unsafe.Pointer(cstr))
	if rc != C.SQLITE_OK {
		return nil, errors.New(C.GoString(C.sqlite3_errmsg(db.db)))
	}
	if !readonly {
		if err := db.exec("PRAGMA auto_vacuum=FULL", nil); err != nil {
			db.close()
			return nil, err
		}
		if err := db.exec("PRAGMA journal_mode=WAL", nil); err != nil {
			db.close()
			return nil, err
		}
		if err := db.exec("PRAGMA synchronous=off", nil); err != nil {
			db.close()
			return nil, err
		}
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

	var ferr error
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
				continue
			}
			// failed
			ferr = errors.New(C.GoString(C.sqlite3_errmsg(db.db)))
			rc = C.sqlite3_finalize(stmt)
			if rc != C.SQLITE_OK {
				return errors.New(C.GoString(C.sqlite3_errmsg(db.db)))
			}
			break

		}
	}
	rc = C.sqlite3_finalize(stmt)
	if rc != C.SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errmsg(db.db)))
	}
	return ferr
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

const rdbMaxPool = 50

var rdbsMu sync.Mutex
var rdbs []*sqlDatabase

func takeReaderDB() (*sqlDatabase, error) {
	rdbsMu.Lock()
	if len(rdbs) > 1 {
		db := rdbs[len(rdbs)-1]
		rdbs = rdbs[:len(rdbs)-1]
		rdbsMu.Unlock()
		return db, nil
	}
	rdbsMu.Unlock()
	return openSQLDatabase(dbPath, true)
}

func releaseReaderDB(db *sqlDatabase) {
	rdbsMu.Lock()
	if len(rdbs) < rdbMaxPool {
		rdbs = append(rdbs, db)
		rdbsMu.Unlock()
	} else {
		rdbsMu.Unlock()
		db.close()
	}
}

// sqlForEachStatement iterates over each sql statement in a block of semicolon
// seperated statements. Comments are removed. Returns complete=false if the
// input sql ended too soon.
func sqlForEachStatement(sql string, iter func(sql string) bool) bool {
	i, s, complete := 0, 0, true
	for ; i < len(sql); i++ {
		switch sql[i] {
		case '/':
			e := i
			if i+1 != len(sql) && sql[i+1] == '*' {
				i++
				complete = false
				for ; i < len(sql); i++ {
					if sql[i] == '*' {
						if i+1 != len(sql) && sql[i+1] == '/' {
							i++
							sql = sql[s:e] + sql[i+1:]
							i, s = 0, 0
							complete = true
							break
						}
					}
				}
			}
		case '-':
			e := i
			if i+1 != len(sql) && sql[i+1] == '-' {
				i++
				for ; i < len(sql); i++ {
					if sql[i] == '\n' {
						sql = sql[s:e] + sql[i+1:]
						i, s = 0, 0
						break
					}
				}
			}
		case '\'', '"', '`':
			q := sql[i]
			i++
			complete = false
			for ; i < len(sql); i++ {
				if sql[i] == q {
					if i+1 != len(sql) && sql[i+1] == q {
						i++
						continue
					}
					complete = true
					break
				}
			}
		case '[':
			i++
			complete = false
			for ; i < len(sql); i++ {
				if sql[i] == ']' {
					complete = true
					break
				}
			}
		case ';':
			part := strings.TrimSpace(sql[s:i])
			if len(part) > 0 {
				if !iter(part) {
					return false
				}
			}
			i++
			s = i
		}
	}
	if i > len(sql) {
		i = len(sql)
	}
	part := strings.TrimSpace(sql[s:i])
	if len(part) > 0 {
		if !iter(part) {
			return false
		}
	}
	return complete
}

// sqlCommand returns the sql statement command in all lowercase characters.
func sqlCommand(sql string) string {
	for i := 0; i < len(sql); i++ {
		alpha := (sql[i] >= 'A' && sql[i] <= 'Z') ||
			(sql[i] >= 'a' && sql[i] <= 'z')
		if !alpha {
			return strings.ToLower(sql[:i])
		}
	}
	return strings.ToLower(sql)
}
