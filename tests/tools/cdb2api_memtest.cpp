/*
   Copyright 2015, 2025 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

/*
 * cdb2api memory and functionality tests
 * Adapted from Bloomberg internal memtest.cpp for open-source comdb2
 */

#include <cdb2api.h>
#include <cdb2api_hndl.h>
#include <cdb2api_test.h>
#include <signal.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <stdio.h>
#include <libgen.h>

#define CDB2TEST_TBL "memtest_t1"

static char *g_dbname = NULL;
static char *g_cluster = NULL;

/* Helper to get socket fd from handle for disconnect tests */
static int get_sb_fd(cdb2_hndl_tp *hndl)
{
    if (hndl && hndl->sb) {
        return sbuf2fileno(hndl->sb);
    }
    return -1;
}

static void disconnect_cdb2h(cdb2_hndl_tp *hndl)
{
    int fd = get_sb_fd(hndl);
    if (fd >= 0) {
        shutdown(fd, 2);
    }
}

static void bzero_hostinfo(cdb2_hndl_tp *hndl)
{
    disconnect_cdb2h(hndl);
    bzero(hndl->hosts, sizeof(hndl->hosts));
    bzero(hndl->ports, sizeof(hndl->ports));
    bzero(hndl->hosts_connected, sizeof(hndl->hosts_connected));
}

static cdb2_hndl_tp *open_and_run(const char *dbname, const char *type, const char *query)
{
    int rc;
    cdb2_hndl_tp *cdb2h = NULL;
    rc = cdb2_open(&cdb2h, dbname, type, 0);
    if (rc) {
        printf("Can't open db:%s cluster:%s %s\n", dbname, type, cdb2_errstr(cdb2h));
        cdb2_close(cdb2h);
        return NULL;
    }
    rc = cdb2_run_statement(cdb2h, query);
    if (rc) {
        printf("cdb2_run_statement failed rc:%d err:%s stmt:%s\n", rc, cdb2_errstr(cdb2h), query);
        cdb2_close(cdb2h);
        return NULL;
    }
    return cdb2h;
}

/* Test basic select */
static int test1(char *dbname, char *type)
{
    int rc;
    char query[] = "select 1";
    cdb2_hndl_tp *cdb2h = open_and_run(dbname, type, query);
    if (cdb2h == NULL) {
        return -1;
    }
    int ncols = cdb2_numcolumns(cdb2h);
    if (ncols != 1) {
        printf("Invalid number of columns %d should be %d.\n", ncols, 1);
        cdb2_close(cdb2h);
        return -1;
    }
    rc = cdb2_next_record(cdb2h);
    if (rc != CDB2_OK) {
        printf("cdb2_next_record failed rc:%d\n", rc);
        cdb2_close(cdb2h);
        return -1;
    }
    void *val = cdb2_column_value(cdb2h, 0);
    if (1 != *(long long *)val) {
        printf("Invalid column value %lld should be %d.\n", *(long long *)val, 1);
        cdb2_close(cdb2h);
        return -1;
    }
    rc = cdb2_close(cdb2h);
    return rc;
}

/* Test datetime type */
static int test2(char *dbname, char *type)
{
    int rc;
    char query[] = "select now()";
    cdb2_hndl_tp *cdb2h = open_and_run(dbname, type, query);
    if (cdb2h == NULL) {
        return -1;
    }
    int ncols = cdb2_numcolumns(cdb2h);
    if (ncols != 1) {
        printf("Invalid number of columns %d should be %d.\n", ncols, 1);
        cdb2_close(cdb2h);
        return -1;
    }
    rc = cdb2_next_record(cdb2h);
    if (rc != CDB2_OK) {
        printf("cdb2_next_record failed rc:%d\n", rc);
        cdb2_close(cdb2h);
        return -1;
    }
    int col_type = cdb2_column_type(cdb2h, 0);
    if (col_type != CDB2_DATETIME) {
        printf("Invalid column type %d should be %d.\n", col_type, CDB2_DATETIME);
        cdb2_close(cdb2h);
        return -1;
    }
    rc = cdb2_close(cdb2h);
    return rc;
}

/* Failure in invalid set transaction command */
static int test3(char *dbname, char *type)
{
    int rc;
    char query[] = "set transaction dfhgdkjfghkfjdgh";
    cdb2_hndl_tp *cdb2h = open_and_run(dbname, type, query);
    if (cdb2h == NULL) {
        return -1;
    }
    rc = cdb2_run_statement(cdb2h, "select 1");
    if (rc == 0) {
        cdb2_close(cdb2h);
        return -1;
    }
    rc = cdb2_close(cdb2h);
    return rc;
}

/* Test delete */
static int test4(char *dbname, char *type)
{
    int rc;
    char query[256];
    snprintf(query, sizeof(query), "delete from %s where 1", CDB2TEST_TBL);
    cdb2_hndl_tp *cdb2h = open_and_run(dbname, type, query);
    if (cdb2h == NULL) {
        return -1;
    }
    rc = cdb2_close(cdb2h);
    return rc;
}

/* Get effects test */
static int test5(char *dbname, char *type)
{
    int rc;
    char query[] = "begin";
    cdb2_hndl_tp *cdb2h = open_and_run(dbname, type, query);
    if (cdb2h == NULL) {
        return -1;
    }

    char insert_sql[256];
    snprintf(insert_sql, sizeof(insert_sql), "insert into %s values(1)", CDB2TEST_TBL);

    rc = cdb2_run_statement(cdb2h, insert_sql);
    if (rc != 0) {
        printf("insert failed rc:%d err:%s\n", rc, cdb2_errstr(cdb2h));
        cdb2_close(cdb2h);
        return -1;
    }
    rc = cdb2_run_statement(cdb2h, insert_sql);
    if (rc != 0) {
        printf("insert failed rc:%d err:%s\n", rc, cdb2_errstr(cdb2h));
        cdb2_close(cdb2h);
        return -1;
    }
    rc = cdb2_run_statement(cdb2h, "commit");
    if (rc != 0) {
        printf("commit failed rc:%d err:%s\n", rc, cdb2_errstr(cdb2h));
        cdb2_close(cdb2h);
        return -1;
    }
    cdb2_effects_tp effects;
    rc = cdb2_get_effects(cdb2h, &effects);
    if (rc != 0) {
        printf("get_effects failed rc:%d\n", rc);
        cdb2_close(cdb2h);
        return -1;
    }
    if (effects.num_inserted != 2) {
        printf("Expected 2 inserts, got %d\n", effects.num_inserted);
        cdb2_close(cdb2h);
        return -1;
    }

    char delete_sql[256];
    snprintf(delete_sql, sizeof(delete_sql), "delete from %s where 1", CDB2TEST_TBL);
    rc = cdb2_run_statement(cdb2h, delete_sql);
    rc = cdb2_get_effects(cdb2h, &effects);
    if (rc != 0) {
        printf("get_effects failed rc:%d\n", rc);
        cdb2_close(cdb2h);
        return -1;
    }
    if (effects.num_deleted != 2) {
        printf("Expected 2 deletes, got %d\n", effects.num_deleted);
        cdb2_close(cdb2h);
        return -1;
    }

    rc = cdb2_close(cdb2h);
    return rc;
}

/* Socket disconnect without a transaction - should reconnect */
static int test6(char *dbname, char *type)
{
    int rc;
    char insert_sql[256];
    snprintf(insert_sql, sizeof(insert_sql), "insert into %s values(1)", CDB2TEST_TBL);

    cdb2_hndl_tp *cdb2h = open_and_run(dbname, type, insert_sql);
    if (cdb2h == NULL) {
        return -1;
    }
    rc = cdb2_run_statement(cdb2h, insert_sql);
    if (rc != 0) {
        printf("Error: %s\n", cdb2_errstr(cdb2h));
        cdb2_close(cdb2h);
        return -1;
    }
    disconnect_cdb2h(cdb2h);
    /* It should reconnect */
    rc = cdb2_run_statement(cdb2h, insert_sql);
    if (rc != 0) {
        printf("Error: %s\n", cdb2_errstr(cdb2h));
        cdb2_close(cdb2h);
        return -1;
    }
    cdb2_effects_tp effects;
    rc = cdb2_get_effects(cdb2h, &effects);
    if (rc != 0) {
        printf("Error testcase 6\n");
        cdb2_close(cdb2h);
        return -1;
    }
    if (effects.num_inserted != 1) {
        printf("Error num inserted %d\n", effects.num_inserted);
        cdb2_close(cdb2h);
        return -1;
    }

    rc = cdb2_close(cdb2h);
    return rc;
}

/* Socket is disconnected along with invalid dbname - should fail */
static int test7(char *dbname, char *type)
{
    int rc;
    char insert_sql[256];
    snprintf(insert_sql, sizeof(insert_sql), "insert into %s values(1)", CDB2TEST_TBL);

    cdb2_hndl_tp *cdb2h = open_and_run(dbname, type, insert_sql);
    if (cdb2h == NULL) {
        return -1;
    }
    rc = cdb2_run_statement(cdb2h, insert_sql);
    if (rc != 0) {
        printf("Error: %s\n", cdb2_errstr(cdb2h));
        cdb2_close(cdb2h);
        return -1;
    }

    disconnect_cdb2h(cdb2h);
    bzero(cdb2h->dbname, 32);
    /* It should not reconnect */
    rc = cdb2_run_statement(cdb2h, insert_sql);
    cdb2_close(cdb2h);
    if (rc != 0) {
        /* Return 0 on failure (expected) */
        return 0;
    } else {
        printf("Error: This should not reconnect as there is no info\n");
        return -1;
    }
}

/* Simulate db cluster change - reconnect with fresh host info */
static int test8(char *dbname, char *type)
{
    int rc;
    char insert_sql[256];
    snprintf(insert_sql, sizeof(insert_sql), "insert into %s values(1)", CDB2TEST_TBL);

    cdb2_hndl_tp *cdb2h = open_and_run(dbname, type, insert_sql);
    if (cdb2h == NULL) {
        return -1;
    }
    rc = cdb2_run_statement(cdb2h, insert_sql);
    if (rc != 0) {
        printf("Error: %s\n", cdb2_errstr(cdb2h));
        cdb2_close(cdb2h);
        return -1;
    }
    bzero_hostinfo(cdb2h);
    /* It should requery for cluster and reconnect */
    rc = cdb2_run_statement(cdb2h, insert_sql);
    if (rc != 0) {
        printf("Error: %s\n", cdb2_errstr(cdb2h));
        cdb2_close(cdb2h);
        return -1;
    }
    cdb2_effects_tp effects;
    rc = cdb2_get_effects(cdb2h, &effects);
    if (rc != 0) {
        cdb2_close(cdb2h);
        return -1;
    }
    if (effects.num_inserted != 1) {
        cdb2_close(cdb2h);
        return -1;
    }

    rc = cdb2_close(cdb2h);
    return rc;
}

/* Socket disconnect inside a transaction - should fail */
static int test9(char *dbname, char *type)
{
    int rc;
    char query[] = "begin";
    cdb2_hndl_tp *cdb2h = open_and_run(dbname, type, query);
    if (cdb2h == NULL) {
        return -1;
    }

    char insert_sql[256];
    snprintf(insert_sql, sizeof(insert_sql), "insert into %s values(1)", CDB2TEST_TBL);

    rc = cdb2_run_statement(cdb2h, insert_sql);
    if (rc != 0) {
        printf("Error: %s\n", cdb2_errstr(cdb2h));
        cdb2_close(cdb2h);
        return -1;
    }
    disconnect_cdb2h(cdb2h);
    /* In transaction it should give failure */
    rc = cdb2_run_statement(cdb2h, insert_sql);
    if (rc == 0) {
        printf("Error: This should not succeed\n");
        cdb2_close(cdb2h);
        return -1;
    }
    rc = cdb2_close(cdb2h);
    return rc;
}

/* Test bind parameters */
static int test_bind(char *dbname, char *type)
{
    int rc;
    cdb2_hndl_tp *cdb2h = NULL;
    rc = cdb2_open(&cdb2h, dbname, type, 0);
    if (rc) {
        cdb2_close(cdb2h);
        return -1;
    }

    cdb2_bind_param(cdb2h, "a", CDB2_CSTRING, "foo", sizeof("foo"));
    rc = cdb2_run_statement(cdb2h, "SELECT @a");
    cdb2_clearbindings(cdb2h);
    if (rc) {
        cdb2_close(cdb2h);
        return -1;
    }
    rc = cdb2_bind_index(cdb2h, 0, CDB2_CSTRING, "foo", sizeof("foo"));
    if (!rc) {
        cdb2_close(cdb2h);
        return -1;
    }
    cdb2_bind_index(cdb2h, 1, CDB2_CSTRING, "foo", sizeof("foo"));
    rc = cdb2_run_statement(cdb2h, "SELECT ?");
    if (rc) {
        cdb2_close(cdb2h);
        return -1;
    }

    cdb2_close(cdb2h);
    return 0;
}

/* Test bind array */
static int test_bind_array(char *dbname, char *type)
{
    cdb2_hndl_tp *db = NULL;
    int rc = cdb2_open(&db, dbname, type, 0);

    if (rc) {
        fprintf(stderr, "%s: cdb2_open rc:%d err:%s\n", __func__, rc, cdb2_errstr(db));
        cdb2_close(db);
        return -1;
    }

    int32_t i32arr[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    int64_t i64arr[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    double dblarr[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    char txtarr[][10] = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"};
    struct {
        size_t len;
        void *data;
    } blbarr[] = {
        {0, 0},
    };
    cdb2_bind_array(db, "i32", CDB2_INTEGER, i32arr, 10, 4);
    cdb2_bind_array(db, "i64", CDB2_INTEGER, i64arr, 10, 8);
    cdb2_bind_array(db, "dbl", CDB2_REAL, dblarr, 10, 0);
    cdb2_bind_array(db, "txt", CDB2_CSTRING, txtarr, 10, 0);
    cdb2_bind_array(db, "blb", CDB2_BLOB, blbarr, 1, 0);
    cdb2_clearbindings(db);
    cdb2_close(db);
    return 0;
}

/* Test multiple columns */
static int test_many_columns(char *dbname, char *type)
{
    cdb2_hndl_tp *cdb2h = NULL;

    char ddl[65536];
    char *p = ddl;
    p += sprintf(p, "CREATE TABLE IF NOT EXISTS t_many_columns (");
    for (int i = 0; i < 100; ++i) {
        if (i > 0)
            p += sprintf(p, ", ");
        p += sprintf(p, "c%d INTEGER DEFAULT %d", i, i);
    }
    p += sprintf(p, ")");

    int rc = cdb2_open(&cdb2h, dbname, type, 0);
    if (rc != 0) {
        fprintf(stderr, "cdb2_open rc %d\n", rc);
        cdb2_close(cdb2h);
        return rc;
    }

    rc = cdb2_run_statement(cdb2h, "DROP TABLE IF EXISTS t_many_columns");
    if (rc != 0) {
        fprintf(stderr, "failed to drop table rc %d\n", rc);
        cdb2_close(cdb2h);
        return rc;
    }

    rc = cdb2_run_statement(cdb2h, ddl);
    if (rc != 0) {
        fprintf(stderr, "failed to create table cdb2_run_statement rc %d %s\n", rc, cdb2_errstr(cdb2h));
        cdb2_close(cdb2h);
        return rc;
    }

    rc = cdb2_run_statement(cdb2h, "INSERT INTO t_many_columns(c99) VALUES(99)");
    if (rc != 0) {
        fprintf(stderr, "failed to insert cdb2_run_statement rc %d\n", rc);
        cdb2_close(cdb2h);
        return rc;
    }

    rc = cdb2_run_statement(cdb2h, "SELECT * FROM t_many_columns");
    if (rc != 0) {
        fprintf(stderr, "failed to select cdb2_run_statement rc %d\n", rc);
        cdb2_close(cdb2h);
        return rc;
    }

    rc = cdb2_next_record(cdb2h);
    if (rc != 0) {
        fprintf(stderr, "cdb2_next_record rc %d\n", rc);
        cdb2_close(cdb2h);
        return rc;
    }

    int ncols = cdb2_numcolumns(cdb2h);
    if (ncols != 100) {
        fprintf(stderr, "got %d columns, expected 100\n", ncols);
        cdb2_close(cdb2h);
        return -1;
    }

    for (int i = 0; i != ncols; ++i) {
        long long val = *(long long *)cdb2_column_value(cdb2h, i);
        if (val != i) {
            fprintf(stderr, "got value %lld, expected %d\n", val, i);
            cdb2_close(cdb2h);
            return -1;
        }
    }

    rc = cdb2_next_record(cdb2h);
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "cdb2_next_record rc %d, expected CDB2_OK_DONE\n", rc);
        cdb2_close(cdb2h);
        return -1;
    }

    rc = cdb2_run_statement(cdb2h, "DROP TABLE t_many_columns");
    if (rc != 0) {
        fprintf(stderr, "failed to drop table rc %d\n", rc);
        cdb2_close(cdb2h);
        return rc;
    }

    while (cdb2_next_record(cdb2h) == CDB2_OK)
        ;
    cdb2_close(cdb2h);
    return 0;
}

/* Test sockpool enable/disable */
static int test_sockpool(char *dbname, char *type)
{
    int rc;
    cdb2_hndl_tp *cdb2h = NULL;

    cdb2_disable_sockpool();

    rc = cdb2_open(&cdb2h, dbname, type, 0);
    if (rc) {
        printf("cdb2_open failed rc %d\n", rc);
        cdb2_close(cdb2h);
        cdb2_enable_sockpool();
        return -1;
    }

    rc = cdb2_run_statement(cdb2h, "select 1");
    if (rc) {
        printf("run failed rc %d\n", rc);
        cdb2_close(cdb2h);
        cdb2_enable_sockpool();
        return -1;
    }

    rc = cdb2_next_record(cdb2h);
    if (rc != CDB2_OK) {
        printf("next failed rc %d\n", rc);
        cdb2_close(cdb2h);
        cdb2_enable_sockpool();
        return -1;
    }

    cdb2_close(cdb2h);
    cdb2_enable_sockpool();
    return 0;
}

/* Test host info retrieval */
static int test_host_info(char *dbname, char *type)
{
    int rc;
    cdb2_hndl_tp *cdb2h = NULL;

    rc = cdb2_open(&cdb2h, dbname, type, 0);
    if (rc) {
        printf("cdb2_open failed rc %d\n", rc);
        cdb2_close(cdb2h);
        return -1;
    }

    rc = cdb2_run_statement(cdb2h, "select comdb2_host()");
    if (rc) {
        printf("run failed rc %d\n", rc);
        cdb2_close(cdb2h);
        return -1;
    }

    rc = cdb2_next_record(cdb2h);
    if (rc != CDB2_OK) {
        printf("next failed rc %d\n", rc);
        cdb2_close(cdb2h);
        return -1;
    }

    const char *host = cdb2_host(cdb2h);
    if (host == NULL || strlen(host) == 0) {
        printf("cdb2_host returned empty\n");
        cdb2_close(cdb2h);
        return -1;
    }

    const char *dbname_ret = cdb2_dbname(cdb2h);
    if (dbname_ret == NULL || strcmp(dbname_ret, dbname) != 0) {
        printf("cdb2_dbname mismatch: got %s expected %s\n", dbname_ret, dbname);
        cdb2_close(cdb2h);
        return -1;
    }

    cdb2_close(cdb2h);
    return 0;
}

/* Test context push/pop */
static int test_context(char *dbname, char *type)
{
    int rc;
    cdb2_hndl_tp *cdb2h = NULL;

    rc = cdb2_open(&cdb2h, dbname, type, 0);
    if (rc) {
        cdb2_close(cdb2h);
        return -1;
    }

    rc = cdb2_push_context(cdb2h, "test context 1");
    if (rc != 0) {
        cdb2_close(cdb2h);
        return -1;
    }

    rc = cdb2_push_context(cdb2h, "test context 2");
    if (rc != 0) {
        cdb2_close(cdb2h);
        return -1;
    }

    rc = cdb2_run_statement(cdb2h, "select 1");
    if (rc != 0) {
        cdb2_close(cdb2h);
        return -1;
    }

    while (cdb2_next_record(cdb2h) == CDB2_OK)
        ;

    rc = cdb2_pop_context(cdb2h);
    if (rc != 0) {
        cdb2_close(cdb2h);
        return -1;
    }

    rc = cdb2_clear_contexts(cdb2h);
    if (rc != 0) {
        cdb2_close(cdb2h);
        return -1;
    }

    cdb2_close(cdb2h);
    return 0;
}

/* Test string escape */
static int test_string_escape(char *dbname, char *type)
{
    cdb2_hndl_tp *cdb2h = NULL;
    int rc = cdb2_open(&cdb2h, dbname, type, 0);
    if (rc) {
        cdb2_close(cdb2h);
        return -1;
    }

    char *escaped = cdb2_string_escape(cdb2h, "test's string");
    if (escaped == NULL) {
        cdb2_close(cdb2h);
        return -1;
    }
    if (strcmp(escaped, "'test''s string'") != 0) {
        printf("string escape failed: got %s\n", escaped);
        free(escaped);
        cdb2_close(cdb2h);
        return -1;
    }
    free(escaped);

    cdb2_close(cdb2h);
    return 0;
}

typedef int (*testfunc)(char *, char *);
struct test {
    testfunc func;
    const char *name;
    int skip;
};

#define R_TEST(a)  \
    {              \
        a, #a, 0   \
    }
#define S_TEST(a)  \
    {              \
        a, #a, 1   \
    }

static struct test tests[] = {
    R_TEST(test1),
    R_TEST(test2),
    R_TEST(test3),
    R_TEST(test4),
    R_TEST(test5),
    R_TEST(test6),
    R_TEST(test7),
    R_TEST(test8),
    R_TEST(test9),
    R_TEST(test_bind),
    R_TEST(test_bind_array),
    R_TEST(test_many_columns),
    R_TEST(test_sockpool),
    R_TEST(test_host_info),
    R_TEST(test_context),
    R_TEST(test_string_escape),
    {NULL, NULL, 0}};

#undef R_TEST
#undef S_TEST

static int setup_test_table(char *dbname, char *type)
{
    cdb2_hndl_tp *cdb2h = NULL;
    int rc = cdb2_open(&cdb2h, dbname, type, 0);
    if (rc) {
        printf("Can't open db:%s cluster:%s %s\n", dbname, type, cdb2_errstr(cdb2h));
        cdb2_close(cdb2h);
        return -1;
    }

    char create_sql[256];
    snprintf(create_sql, sizeof(create_sql),
             "CREATE TABLE IF NOT EXISTS %s (a INTEGER)", CDB2TEST_TBL);
    rc = cdb2_run_statement(cdb2h, create_sql);
    if (rc) {
        printf("Create table failed rc:%d err:%s\n", rc, cdb2_errstr(cdb2h));
        cdb2_close(cdb2h);
        return -1;
    }

    char truncate_sql[256];
    snprintf(truncate_sql, sizeof(truncate_sql), "TRUNCATE TABLE %s", CDB2TEST_TBL);
    rc = cdb2_run_statement(cdb2h, truncate_sql);
    if (rc) {
        printf("Truncate table failed rc:%d err:%s\n", rc, cdb2_errstr(cdb2h));
        cdb2_close(cdb2h);
        return -1;
    }

    cdb2_close(cdb2h);
    return 0;
}

int main(int argc, char *argv[])
{
    signal(SIGPIPE, SIG_IGN);

    if (argc < 2 || argc > 3) {
        printf("Usage: %s <dbname> [cluster]\n", basename(argv[0]));
        printf("  dbname  - Name of the database to test against\n");
        printf("  cluster - Cluster type (default: 'default')\n");
        return -1;
    }

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    g_dbname = argv[1];
    g_cluster = (argc >= 3) ? argv[2] : (char *)"default";

    printf("Running cdb2api memory tests against %s (cluster: %s)\n", g_dbname, g_cluster);

    /* Setup test table */
    if (setup_test_table(g_dbname, g_cluster) != 0) {
        printf("FAIL: Could not setup test table\n");
        return -1;
    }

    /* Run all tests */
    struct test *t = tests;
    int failed = 0;
    int passed = 0;
    int skipped = 0;

    while (t->func) {
        int rc;
        if (t->skip) {
            printf("SKIP: %s\n", t->name);
            skipped++;
        } else if ((rc = t->func(g_dbname, g_cluster)) != 0) {
            printf("FAIL: %s rc:%d\n", t->name, rc);
            failed++;
        } else {
            printf("PASS: %s\n", t->name);
            passed++;
        }
        ++t;
    }

    printf("\n");
    printf("Results: %d passed, %d failed, %d skipped\n", passed, failed, skipped);

    /* Clean up global state for valgrind */
    local_connection_cache_clear(1);

    if (failed > 0) {
        printf("FAILURE: %d test(s) failed\n", failed);
        return -1;
    }

    printf("SUCCESS: all test cases passed.\n");
    return 0;
}
