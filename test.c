#include <stdio.h>
#include "cdb.h"

int main() {
    printf("C tests started\n");

    int errCode;
    int hasErr;
    char* errMessage;
    char* filename = ":memory:";
    errCode = cdb_new_db(filename);
    if (errCode != 0) {
        printf("failed to open db\n");
        return 1;
    }
    char* createTableSql = "CREATE TABLE IF NOT EXISTS foo (id INTEGER PRIMARY KEY, name TEXT);";
    int createPrepareId;
    cdb_prepare(&createPrepareId, filename, createTableSql);
    cdb_execute(createPrepareId);
    cdb_result_err(createPrepareId, &hasErr, &errMessage);
    if (hasErr != 0) {
        printf("failed to create table\n");
        printf("err: %s\n", errMessage);
        return 1;
    }

    char* insertSql = "INSERT INTO foo (name) VALUES (?);";
    int insertPrepareId;
    cdb_prepare(&insertPrepareId, filename, insertSql);
    char* insertText = "asdf";
    cdb_bind_string(insertPrepareId, insertText);
    cdb_execute(insertPrepareId);
    cdb_result_err(insertPrepareId, &hasErr, &errMessage);
    if (hasErr != 0) {
        printf("failed to insert into table\n");
        printf("err: %s\n", errMessage);
        return 1;
    }

    char* selectSql = "SELECT * FROM foo;";
    int selectPrepareId;
    cdb_prepare(&selectPrepareId, filename, selectSql);
    cdb_execute(selectPrepareId);
    cdb_result_err(selectPrepareId, &hasErr, &errMessage);
    if (hasErr != 0) {
        printf("failed to select from table\n");
        printf("err: %s\n", errMessage);
        return 1;
    }
    int hasRow;
    cdb_result_row(selectPrepareId, &hasRow);
    if (hasRow == 0) {
        printf("expected select result to have row\n");
        return 1;
    }
    int rowId;
    cdb_result_col_int(selectPrepareId, 0, &rowId);
    printf("rowId %d\n", rowId);
    char* name;
    cdb_result_col_string(selectPrepareId, 1, &name);
    printf("name %s\n", name);

    printf("C tests finished successfully\n");
    return 0;
}
