#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "sqlite/sqlite.h"

const char *parser_db_check_stmt(sqlite3 *db, const char *sql, 
    const char **tail, int *readonly
) {
    *readonly = 0;
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, tail);
    if (rc != SQLITE_OK) {
        return sqlite3_errmsg(db);
    }
    *readonly = sqlite3_stmt_readonly(stmt) ? 1 : 0;
    rc = sqlite3_finalize(stmt);
    if (rc != SQLITE_OK) {
        return sqlite3_errmsg(db);
    }
    return NULL;
}

static sqlite3 *wdb = NULL;
// char *errmsg = NULL;
void db_open(const char *path) {
    int rc = sqlite3_open(path, &wdb);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "sqlite3_open: %s\n", sqlite3_errmsg(wdb));
        exit(1);
    }
    rc = sqlite3_exec(wdb, "PRAGMA journal_mode=WAL", NULL, 0, 0);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "sqlite3_open: %s\n", sqlite3_errmsg(wdb));
        exit(1);
    }
    rc = sqlite3_exec(wdb, "PRAGMA wal_autocheckpoint=WAL", NULL, 0, 0);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "sqlite3_open: %s\n", sqlite3_errmsg(wdb));
        exit(1);
    }
}

void db_close() {
    int rc = sqlite3_close(wdb);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "sqlite3_close: %s\n", sqlite3_errmsg(wdb));
        exit(1);
    }
}

char *result = NULL;
int result_len = 0;
int result_cap = 0;

static void result_ensure(int nbytes) {
    if (result_cap-result_len < nbytes) {
        int cap = result_cap ? result_cap : 1;
        while (cap-result_len < nbytes) {
            cap *= 2;
        }
        result = realloc(result, cap);
        if (!result) {
            fprintf(stderr, "out of memory\n");
            exit(1);
        }
        result_cap = cap;
    }
}

static void result_append_char(char ch) {
    result_ensure(1);
    result[result_len++] = ch;
}


static void result_append_int(int x) {
    result_ensure(20);
    sprintf(result+result_len, "%d", x);
    result_len += strlen(result+result_len);
}

static void result_append_str(const char *str) {
    if (!str) {
        result_append_char('0');
        result_append_char('.');
    } else {
        int slen = strlen(str);
        result_append_int(slen+1);
        result_append_char('.');
        result_ensure(slen);
        memcpy(result+result_len, str, slen);
        result_len += slen;
        result_append_char('!');
    }
}

const char *db_exec(const char *sql) {
    result_len = 0;
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(wdb, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        return sqlite3_errmsg(wdb);
    }
    int ncols = sqlite3_column_count(stmt);
    for (int i = 0; i < ncols; i++) {
        if (i > 0) {
            result_append_char('|');
        }
        result_append_str(sqlite3_column_name(stmt, i));
    }
    result_append_char('\n');
    while (1) {
        rc = sqlite3_step(stmt);
        if (rc == SQLITE_DONE) {
            break;
        }
        if (rc == SQLITE_ROW) {
            for (int i = 0; i < ncols; i++) {
                if (i > 0) {
                    result_append_char('|');
                }
                result_append_str((char*)sqlite3_column_text(stmt, i));
            }
            result_append_char('\n');
        }
    }
    rc = sqlite3_finalize(stmt);
    if (rc != SQLITE_OK) {
        return sqlite3_errmsg(wdb);
    }
    return NULL;
}


void db_checkpoint() {
    sqlite3_wal_checkpoint_v2(wdb, NULL,SQLITE_CHECKPOINT_TRUNCATE, NULL, NULL);
}