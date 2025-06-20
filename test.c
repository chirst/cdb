//go:build ignore
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "cdb.h"

// test.c contains tests for the C interface of CDB.

// printInfo prints the message in blue with a new line.
void printInfo(char* message) {
    printf("\x1b[34m%s\033[0m\n", message);
}

// printSuccess prints the message in green with a new line.
void printSuccess(char* message) {
    printf("\033[0;32m%s\033[0m\n", message);
}

void openInMemoryDatabase() {
    int errCode = cdb_new_db(":memory:");
    assert(errCode == 0);
}

void testCreate() {
    // Prepare
    int prepareId = 0;
    char* prepareErr = "";
    int errCode = cdb_prepare(
        &prepareId,
        ":memory:",
         "CREATE TABLE IF NOT EXISTS foo (id INTEGER PRIMARY KEY, name TEXT);",
        &prepareErr
    );
    assert(prepareId != 0);
    assert(strcmp(prepareErr, "") == 0);
    assert(errCode == 0);

    // Execute
    errCode = cdb_execute(prepareId);
    assert(errCode == 0);

    // Check execute result for error
    char* errMessage = "";
    int hasErr;
    errCode = cdb_result_err(prepareId, &hasErr, &errMessage);
    assert(errCode == 0);
    assert(hasErr == 0);
    assert(strcmp(errMessage, "") == 0);
}

void testInsert() {
    // Prepare
    int prepareId = 0;
    char* prepareErr = "";
    int errCode = cdb_prepare(
        &prepareId,
        ":memory:",
         "INSERT INTO foo (id, name) VALUES (?, ?);",
        &prepareErr
    );
    assert(strcmp(prepareErr, "") == 0);
    assert(prepareId != 0);
    assert(errCode == 0);

    // Bind params
    errCode = cdb_bind_int(prepareId, 1);
    assert(errCode == 0);
    errCode = cdb_bind_string(prepareId, "asdf");
    assert(errCode == 0);

    // Execute
    errCode = cdb_execute(prepareId);
    assert(errCode == 0);

    // Check result for errors
    int hasErr = 0;
    char* errMessage = "";
    errCode = cdb_result_err(prepareId, &hasErr, &errMessage);
    assert(hasErr == 0);
    assert(strcmp(errMessage, "") == 0);
    assert(errCode == 0);
}

void testSelect() {
    // Prepare
    int prepareId = 0;
    char* prepareErr = "";
    int errCode = cdb_prepare(
        &prepareId,
        ":memory:",
         "SELECT * FROM foo;",
        &prepareErr
    );
    assert(errCode == 0);
    assert(prepareId != 0);
    assert(strcmp(prepareErr, "") == 0);

    // Execute
    errCode = cdb_execute(prepareId);
    assert(errCode == 0);

    // Check result for errors
    int hasErr;
    char* errMessage = "";
    errCode = cdb_result_err(prepareId, &hasErr, &errMessage);
    assert(errCode == 0);
    assert(hasErr == 0);
    assert(strcmp(errMessage, "") == 0);

    // Check id result column type
    int idResultType;
    errCode = cdb_result_col_type(prepareId, 0, &idResultType);
    assert(errCode == 0);
    assert(idResultType == 1);

    // Check name result column type
    int nameResultType;
    errCode = cdb_result_col_type(prepareId, 1, &nameResultType);
    assert(errCode == 0);
    assert(nameResultType == 3);

    // Move to first row
    int hasRow;
    errCode = cdb_result_row(prepareId, &hasRow);
    assert(errCode == 0);
    assert(hasRow != 0);

    // Check count of result columns
    int colCount;
    errCode = cdb_result_col_count(prepareId, &colCount);
    assert(errCode == 0);
    assert(colCount == 2);

    // Check name of id column
    char* idColName = "";
    errCode = cdb_result_col_name(prepareId, 0, &idColName);
    assert(errCode == 0);
    assert(strcmp(idColName, "id") == 0);

    // Check name of name column
    char* nameColName = "";
    errCode = cdb_result_col_name(prepareId, 1, &nameColName);
    assert(errCode == 0);
    assert(strcmp(nameColName, "name") == 0);

    // Check value of id column
    int rowId = 0;
    errCode = cdb_result_col_int(prepareId, 0, &rowId);
    assert(errCode == 0);
    assert(rowId == 1);

    // Check value of name column
    char* name = "";
    errCode = cdb_result_col_string(prepareId, 1, &name);
    assert(errCode == 0);
    assert(strcmp(name, "asdf") == 0);

    // Advance to next row and see there is none
    errCode = cdb_result_row(prepareId, &hasRow);
    assert(errCode == 0);
    assert(hasRow == 0);
}

// testParameterizedResultColumn is to test the type of a lone variable is
// resolved to the bound type and not type variable. In this case, the bound
// type an integer being an integer.
void testParameterizedResultColumn() {
    // Prepare
    int prepareId = 0;
    char* prepareErr = "";
    int errCode = cdb_prepare(
        &prepareId,
        ":memory:",
         "SELECT ? FROM foo;",
        &prepareErr
    );
    assert(errCode == 0);
    assert(prepareId != 0);
    assert(strcmp(prepareErr, "") == 0);

    // Bind int
    errCode = cdb_bind_int(prepareId, 12);
    assert(errCode == 0);

    // Execute
    errCode = cdb_execute(prepareId);
    assert(errCode == 0);

    // Assert result column type
    int resultType = 0;
    errCode = cdb_result_col_type(prepareId, 0, &resultType);
    assert(errCode == 0);
    assert(resultType == 1);
}

int main() {
    printInfo("C tests started");

    openInMemoryDatabase();
    testCreate();
    testInsert();
    testSelect();
    testParameterizedResultColumn();

    printSuccess("C tests finished successfully");
    return 0;
}
