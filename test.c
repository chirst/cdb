#include <stdio.h>
#include "cdb.h"

int main() {
    printf("C tests started\n");

    int errCode;
    char* filename = ":memory:";
    errCode = cdb_new_db(filename);
    if (errCode != 0) {
        printf("failed to open db\n");
        return 1;
    }
    char* createTableSql = "CREATE TABLE IF NOT EXISTS foo (id INTEGER PRIMARY KEY, name TEXT);";
    int createPrepareId = cdb_prepare(filename, createTableSql);
    cdb_execute(createPrepareId);
    errCode = cdb_result_err(createPrepareId);
    if (errCode != 0) {
        printf("failed to create table\n");
        return 1;
    }

    char* insertSql = "INSERT INTO foo (name) VALUES (?);";

    int insertPrepareId = cdb_prepare(filename, insertSql);
    char* insertText = "asdf";
    cdb_bind_string(insertPrepareId, insertText);
    cdb_execute(insertPrepareId);
    errCode = cdb_result_err(insertPrepareId);
    if (errCode != 0) {
        printf("failed to insert into table\n");
        return 1;
    }

    char* selectSql = "SELECT * FROM foo;";
    int selectPrepareId = cdb_prepare(filename, selectSql);
    cdb_execute(selectPrepareId);
    errCode = cdb_result_err(selectPrepareId);
    if (errCode != 0) {
        printf("failed to select from table\n");
        return 1;
    }
    cdb_result_row(selectPrepareId);
    int rowId = cdb_result_col_int(selectPrepareId, 0);
    printf("rowId %d\n", rowId);
    char* name = cdb_result_col_string(selectPrepareId, 1);
    printf("name %s\n", name);

    printf("C tests finished successfully\n");
    return 0;
}
