#include <stdio.h>
#include "cdb.h"

int main() {
    printf("C tests started\n");

    char* filename = ":memory:";
    cdb_new_db(filename);
    char* createTableSql = "CREATE TABLE IF NOT EXISTS foo (id INTEGER PRIMARY KEY, name TEXT);";
    int createPrepareId = cdb_prepare(filename, createTableSql);
    cdb_execute(createPrepareId);
    int errCode = cdb_result_err(createPrepareId);
    if (errCode != 0) {
        printf("failed to create table\n");
        return 1;
    }

    char* insertSql = "INSERT INTO foo (name) VALUES (?);";
    int insertPrepareId = cdb_prepare(filename, insertSql);
    char* insertText = "asdf";
    cdb_bind_string(insertPrepareId, insertText);
    cdb_execute(insertPrepareId);
    int insertErrCode = cdb_result_err(insertPrepareId);
    if (insertErrCode != 0) {
        printf("failed to insert into table\n");
        return 1;
    }

    char* selectSql = "SELECT * FROM foo;";
    int selectPrepareId = cdb_prepare(filename, selectSql);
    cdb_execute(selectPrepareId);
    int selectErrCode = cdb_result_err(selectPrepareId);
    if (selectErrCode != 0) {
        printf("failed to select from table\n");
        return 1;
    }
    cdb_result_row(selectPrepareId);
    int rowId = cdb_result_col_int(selectPrepareId, 0);
    printf("rowId %d\n", rowId);
    char* name = cdb_result_col_string(selectPrepareId, 1);
    printf("name %s\n", name);

    printf("C tests finished successfully");
    return 0;
}
