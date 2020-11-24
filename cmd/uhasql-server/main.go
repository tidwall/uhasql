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

	"github.com/robertkrimen/otto"

	"github.com/tidwall/gjson"
	"github.com/tidwall/redcon"
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
	conf.AddIntermediateCommand("$ANY", cmdANY)
	conf.AddWriteCommand("PROC", cmdPROC)
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
		default:
			err = fmt.Errorf("near \"%s\": syntax error", cmd)
			return false
		}
		stmts = append(stmts, sql)
		return true
	})
	if err != nil {
		return nil, err
	}
	if len(stmts) == 0 {
		return []string{}, nil
	}
	data, _ := json.Marshal(stmts)
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
	return sqlExec(args[1], false)
}

func cmdQUERY(m uhaha.Machine, args []string) (interface{}, error) {
	// READ
	return sqlExec(args[1], true)
}

func sqlExec(sqlJSON string, readonly bool) (interface{}, error) {
	var sqls []string
	var res []interface{}
	gjson.Parse(sqlJSON).ForEach(func(_, val gjson.Result) bool {
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
		if err := db.ensureProcSpace(); err != nil {
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

func (db *sqlDatabase) ensureProcSpace() error {
	err := db.exec(`
		CREATE TABLE IF NOT EXISTS __proc__ (
			name       TEXT PRIMARY KEY,
			script     TEXT
		);
	`, nil)
	return err
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

// PROC EXEC name args       -- executes a proc
// PROC SET name script      -- sets a proc
// PROC GET name             -- gets a proc
// PROC DEL name             -- deletes a proc
// PROC LIST                 -- returns the names of all procs
func cmdPROC(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, errors.New("wrong number of arguments, try PROC HELP")
	}
	switch strings.ToLower(args[1]) {
	case "exec":
		return cmdPROCEXEC(m, args)
	case "set":
		return cmdPROCSET(m, args)
	case "get":
		return cmdPROCGET(m, args)
	case "del", "delete":
		return cmdPROCDEL(m, args)
	case "help":
		return cmdPROCHELP(m, args)
	case "list":
		return cmdPROCLIST(m, args)
	default:
		return nil, fmt.Errorf("unknown proc command '%s %s', try PROC HELP",
			args[0], args[1],
		)
	}
}

func cmdPROCEXEC(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, errors.New("wrong number of arguments, try PROC HELP")
	}
	name := args[2]
	var vargs []string
	var script string
	if name == "__inline__" {
		if len(args) < 4 {
			return nil, errors.New("wrong number of arguments, try PROC HELP")
		}
		script = args[3]
		vargs = args[4:]
	} else {
		vargs = args[3:]
	}
	_ = vargs

	dbmu.Lock()
	defer dbmu.Unlock()
	var commit bool
	if err := wdb.exec("begin", nil); err != nil {
		return nil, err
	}
	defer func() {
		if commit {
			wdb.exec("end", nil)
		} else {
			wdb.exec("rollback", nil)
		}
	}()

	if name != "__inline__" {
		var count int
		err := wdb.exec(`select script from __proc__
	                 where name = '`+strings.Replace(name, "'", "''", -1)+`'`,
			func(rows []string) bool {
				if count == 1 {
					script = rows[0]
				}
				count++
				return true
			})
		if err != nil {
			return nil, err
		}
		if count != 2 {
			return nil, errors.New("proc not found")
		}
	}
	var result otto.Value
	err := func() (err error) {
		defer func() {
			if err == nil {
				if v := recover(); v != nil {
					err = fmt.Errorf("%v", v)
				}
			}
			if err != nil {
				if strings.Contains(err.Error(), "(anonymous):") {
					err = errors.New(strings.Replace(err.Error(),
						"(anonymous):", "proc.js:", 1))
				}
			}
		}()
		vm := otto.New()
		vm.Set("exec", execFn)
		data, _ := json.Marshal(vargs)
		vm.Eval("this.arguments = " + string(data))
		result, err = vm.Run(script)
		return err
	}()
	if err != nil {
		return nil, err
	}
	val, err := result.Export()
	if err != nil {
		return nil, err
	}
	commit = true
	return val, nil
}

func execFn(call otto.FunctionCall) otto.Value {
	if !call.Argument(0).IsDefined() {
		panic("exec: statement not provided")
	}
	if !call.Argument(0).IsString() {
		panic("exec: statement not a string")
	}
	var rows [][]string
	err := wdb.exec(call.Argument(0).String(), func(row []string) bool {
		rows = append(rows, row)
		return true
	})
	if err != nil {
		panic("exec: " + err.Error())
	}
	val, err := call.Otto.ToValue(rows)
	if err != nil {
		panic("exec: " + err.Error())
	}
	return val
}

func cmdPROCSET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 4 {
		return nil, errors.New("wrong number of arguments, try PROC HELP")
	}

	vm := otto.New()
	_, err := vm.Compile("proc.js", args[3])
	if err != nil {
		return nil, err
	}

	name := strings.Replace(args[2], "'", "''", -1)
	script := strings.Replace(args[3], "'", "''", -1)

	dbmu.Lock()
	defer dbmu.Unlock()

	err = wdb.exec(`INSERT INTO __proc__ (name, script)
					VALUES ('`+name+`', '`+script+`')
					ON CONFLICT(name)
					DO UPDATE SET script=excluded.script;`, nil)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func cmdPROCGET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, errors.New("wrong number of arguments, try PROC HELP")
	}
	dbmu.Lock()
	defer dbmu.Unlock()
	name := strings.Replace(args[2], "'", "''", -1)
	var count int
	var script string
	err := wdb.exec(`select script from __proc__ where name = '`+name+`'`,
		func(rows []string) bool {
			if count == 1 {
				script = rows[0]
			}
			count++
			return true
		})
	if err != nil {
		return nil, err
	}
	if count != 2 {
		return nil, nil
	}
	return script, nil
}

func cmdPROCDEL(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, errors.New("wrong number of arguments, try PROC HELP")
	}
	dbmu.Lock()
	defer dbmu.Unlock()
	name := strings.Replace(args[2], "'", "''", -1)
	err := wdb.exec(`delete from __proc__ where name = '`+name+`'`, nil)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func cmdPROCLIST(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, errors.New("wrong number of arguments, try PROC HELP")
	}
	dbmu.Lock()
	defer dbmu.Unlock()
	db := wdb
	var list []string
	err := db.exec("select name from __proc__ order by name",
		func(row []string) bool {
			list = append(list, row[0])
			return true
		})
	if err != nil {
		return nil, err
	}
	return list[1:], nil
}

func cmdPROCHELP(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, errors.New("wrong number of arguments, try PROC HELP")
	}
	return []string{
		"PROC EXEC name [arg ...]",
		"PROC SET name script",
		"PROC GET name",
		"PROC DEL name",
		"PROC LIST",
	}, nil
}
