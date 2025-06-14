//go:build ignore
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "cdb.h"

int main() {
    printf("C tests started\n");

    int errCode;
    int hasErr;
    char* errMessage;
    char* filename = ":memory:";
    errCode = cdb_new_db(filename);
    assert(errCode == 0);

    char* createTableSql = "CREATE TABLE IF NOT EXISTS foo (id INTEGER PRIMARY KEY, name TEXT);";
    char* createPrepareErr;
    int createPrepareId;
    errCode = cdb_prepare(&createPrepareId, filename, createTableSql, &createPrepareErr);
    assert(errCode == 0);
    errCode = cdb_execute(createPrepareId);
    assert(errCode == 0);
    errCode = cdb_result_err(createPrepareId, &hasErr, &errMessage);
    assert(errCode == 0);
    assert(hasErr == 0);

    char* insertSql = "INSERT INTO foo (id, name) VALUES (?, ?);";
    int insertPrepareId;
    char* insertPrepareErr;
    errCode = cdb_prepare(&insertPrepareId, filename, insertSql, &insertPrepareErr);
    assert(errCode == 0);
    char* insertText = "asdf";
    errCode = cdb_bind_int(insertPrepareId, 1);
    assert(errCode == 0);
    errCode = cdb_bind_string(insertPrepareId, insertText);
    assert(errCode == 0);
    errCode = cdb_execute(insertPrepareId);
    assert(errCode == 0);
    errCode = cdb_result_err(insertPrepareId, &hasErr, &errMessage);
    assert(errCode == 0);
    assert(hasErr == 0);

    char* selectSql = "SELECT * FROM foo;";
    int selectPrepareId;
    char* selectPrepareErr;
    errCode = cdb_prepare(&selectPrepareId, filename, selectSql, &selectPrepareErr);
    assert(errCode == 0);
    errCode = cdb_execute(selectPrepareId);
    assert(errCode == 0);
    errCode = cdb_result_err(selectPrepareId, &hasErr, &errMessage);
    assert(errCode == 0);
    assert(hasErr == 0);
    int idResultType;
    errCode = cdb_result_col_type(selectPrepareId, 0, &idResultType);
    assert(errCode == 0);
    assert(idResultType == 1);
    int nameResultType;
    errCode = cdb_result_col_type(selectPrepareId, 1, &nameResultType);
    assert(errCode == 0);
    assert(nameResultType == 3);
    int hasRow;
    errCode = cdb_result_row(selectPrepareId, &hasRow);
    assert(errCode == 0);
    assert(hasRow != 0);
    int colCount;
    errCode = cdb_result_col_count(selectPrepareId, &colCount);
    assert(errCode == 0);
    assert(colCount == 2);
    char* idColName;
    errCode = cdb_result_col_name(selectPrepareId, 0, &idColName);
    assert(errCode == 0);
    assert(strcmp(idColName, "id") == 0);
    char* nameColName;
    errCode = cdb_result_col_name(selectPrepareId, 1, &nameColName);
    assert(errCode == 0);
    assert(strcmp(nameColName, "name") == 0);
    int rowId;
    errCode = cdb_result_col_int(selectPrepareId, 0, &rowId);
    assert(errCode == 0);
    printf("rowId %d\n", rowId);
    char* name;
    errCode = cdb_result_col_string(selectPrepareId, 1, &name);
    assert(errCode == 0);
    printf("name %s\n", name);

    char* paramSelectSql = "SELECT ? FROM foo;";
    int paramSelectPrepareId;
    char* paramPrepareErr;
    errCode = cdb_prepare(&paramSelectPrepareId, filename, paramSelectSql, &paramPrepareErr);
    assert(errCode == 0);
    errCode = cdb_bind_int(paramSelectPrepareId, 12);
    assert(errCode == 0);
    errCode = cdb_execute(paramSelectPrepareId);
    assert(errCode == 0);
    int paramSelectType;
    errCode = cdb_result_col_type(paramSelectPrepareId, 0, &paramSelectType);
    assert(errCode == 0);
    assert(paramSelectType == 1);

    printf("C tests finished successfully\n");
    return 0;
}
