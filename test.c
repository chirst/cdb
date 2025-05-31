#include <stdio.h>
#include "cdb.h"

int main() {
    printf("yo\n");

    char* filename = ":memory:";
    cdb_new_db(filename);

    printf("yo2\n");
    return 0;
}
