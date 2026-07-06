/*
   Copyright 2017 Bloomberg Finance L.P.

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
 * Unit Tests for cdb2api.h/c
 */

#undef NDEBUG

#include <assert.h>
#include <bb_oscompat.h>
#include <cdb2api.c>
#include <cdb2api_test.h>
#include <cdb2api_int.h>

void test_is_sql_read()
{
    assert(is_sql_read(NULL) == -1);
    assert(is_sql_read("select blah") == 1);
    assert(is_sql_read("  SELECT blah") == 1);
    assert(is_sql_read("explain blah") == 1);
    assert(is_sql_read("  EXPLAIN blah") == 1);
    assert(is_sql_read("with blah") == 1);
    assert(is_sql_read(" WITH blah") == 1);
    assert(is_sql_read("  get blah") == 1);
    assert(is_sql_read("GET blah") == 1);
    assert(is_sql_read("EXEC blah") == 1);
    assert(is_sql_read("  EXEC blah") == 1);
    assert(is_sql_read("  insert blah") == 0);
    assert(is_sql_read("INSERT blah") == 0);
    assert(is_sql_read("UPDATE blah") == 0);
    assert(is_sql_read(" update blah") == 0);
    assert(is_sql_read(" anything else ") == 0);
}

void test_do_init_once()
{
    assert(log_calls == 0);
    assert(strcmp(CDB2DBCONFIG_NOBBENV, "/opt/bb/etc/cdb2/config/comdb2db.cfg") == 0);
    setenv("CDB2_LOG_CALLS", "1", 1);
    setenv("CDB2_CONFIG_FILE", "abracadabra01234567891234567890123456789012345678912345678901234567890123456789123456789012345678901234567891234567890123456789012345678912345678901234567890123456789123456789012345678901234567891234567890123456789012345678912345678901234567890123456789123456789012345678901234567891234567890123456789012345678912345678901234567890123456789123456789012345678901234567891234567890123456789012345678912345678901234567890123456789123456789012345678901234567891234567890123456789012345678912345678901234567890123456789123456789012345678901234567891234567890123456789", 1);
    do_init_once(1);
    assert(log_calls == 1);
    assert(strcmp(CDB2DBCONFIG_NOBBENV, "abracadabra01234567891234567890123456789012345678912345678901234567890123456789123456789012345678901234567891234567890123456789012345678912345678901234567890123456789123456789012345678901234567891234567890123456789012345678912345678901234567890123456789123456789012345678901234567891234567890123456789012345678912345678901234567890123456789123456789012345678901234567891234567890123456789012345678912345678901234567890123456789123456789012345678901234567891234567890123456789012345678912345678901234567890123456") == 0);
    assert( _PID == getpid() );
    assert( _MACHINE_ID == gethostid() );
}


void test_cdb2_set_min_retries()
{
    cdb2_set_min_retries(20);
    assert(MIN_RETRIES == 20);

    cdb2_set_min_retries(-30);
    assert(MIN_RETRIES == 20);

    cdb2_set_min_retries(0);
    assert(MIN_RETRIES == 20);

    cdb2_set_min_retries(1);
    assert(MIN_RETRIES == 1);
}

void test_cdb2_set_max_retries()
{
    cdb2_set_max_retries(20);
    assert(MAX_RETRIES == 20);

    cdb2_set_max_retries(-30);
    assert(MAX_RETRIES == 20);

    cdb2_set_max_retries(0);
    assert(MAX_RETRIES == 20);

    cdb2_set_max_retries(1);
    assert(MAX_RETRIES == 1);
}

void test_cdb2_hndl_set_min_retries()
{
    cdb2_hndl_tp hndl;
    cdb2_hndl_set_min_retries(&hndl, 20);
    assert(hndl.min_retries == 20);

    cdb2_hndl_set_min_retries(&hndl, -30);
    assert(hndl.min_retries == 20);

    cdb2_hndl_set_min_retries(&hndl, 0);
    assert(hndl.min_retries == 20);

    cdb2_hndl_set_min_retries(&hndl, 1);
    assert(hndl.min_retries == 1);
}

void test_cdb2_hndl_set_max_retries()
{
    cdb2_hndl_tp hndl;
    cdb2_hndl_set_max_retries(&hndl, 20);
    assert(hndl.max_retries == 20);

    cdb2_hndl_set_max_retries(&hndl, -30);
    assert(hndl.max_retries == 20);

    cdb2_hndl_set_max_retries(&hndl, 0);
    assert(hndl.max_retries == 20);

    cdb2_hndl_set_max_retries(&hndl, 1);
    assert(hndl.max_retries == 1);
}


void test_cdb2_set_comdb2db_config()
{
    cdb2_set_comdb2db_config("anotherconfigfile");
    assert(strcmp(CDB2DBCONFIG_NOBBENV, "anotherconfigfile") == 0);

    cdb2_set_comdb2db_config(NULL);
}



void test_read_comdb2db_cfg()
{
    cdb2_hndl_tp hndl;
    char comdb2db_hosts[10][CDB2HOSTNAME_LEN];
    char db_hosts[10][CDB2HOSTNAME_LEN];
    COMDB2BUF *s = NULL;
    char *comdb2db_name = NULL;
    int num_hosts = 0;
    int comdb2db_num = 0;
    char *dbname = "mydb";
    int num_db_hosts = 0;
    int dbnum = 0;
    char shards[10][DBNAME_LEN];
    int num_shards = 0;
    int dbname_found;
    int comdb2db_found;

    const char *buf = 
"\
    \
    \
";


    read_comdb2db_cfg(&hndl, s, comdb2db_name,
                      buf, comdb2db_hosts,
                      &num_hosts, &comdb2db_num, dbname,
                      db_hosts, &num_db_hosts,
                      &dbnum, &dbname_found,
                      &comdb2db_found, shards, &num_shards);

    assert(num_hosts == 0);
    assert(comdb2db_num == 0);
    assert(num_db_hosts == 0);
    assert(dbnum == 0);
    assert(num_shards == 0);

    const char *buf2 = 
"\n\
  comdb2dbnm:a,b,c:d:e  \n\
  mydb:n1,n2,n3:n4:n5,n6    \n\
  partition mydb:mydb1,mydb2,mydb3,mydb4    \n\
  comdb2_config:default_type:testsuite  \n\
  comdb2_config:portmuxport=12345   \n\
  comdb2_config:allow_pmux_route:true   \
";

    read_comdb2db_cfg(&hndl, s, "comdb2dbnm",
                      buf2, comdb2db_hosts,
                      &num_hosts, &comdb2db_num, dbname,
                      db_hosts, &num_db_hosts,
                      &dbnum, &dbname_found,
                      &comdb2db_found, shards, &num_shards);

    assert(num_hosts == 5);
    assert(comdb2db_num == 0);
    assert(strcmp(comdb2db_hosts[0], "a") == 0);
    assert(strcmp(comdb2db_hosts[1], "b") == 0);
    assert(strcmp(comdb2db_hosts[2], "c") == 0);
    assert(strcmp(comdb2db_hosts[3], "d") == 0);
    assert(strcmp(comdb2db_hosts[4], "e") == 0);

    assert(num_db_hosts == 6);
    assert(strcmp(db_hosts[0], "n1") == 0);
    assert(strcmp(db_hosts[1], "n2") == 0);
    assert(strcmp(db_hosts[2], "n3") == 0);
    assert(strcmp(db_hosts[3], "n4") == 0);
    assert(strcmp(db_hosts[4], "n5") == 0);
    assert(strcmp(db_hosts[5], "n6") == 0);

    assert(dbnum == 0);
    assert(12345 == CDB2_PORTMUXPORT);

    assert(num_shards == 4);
    assert(strcmp(shards[0], "mydb1") == 0);
    assert(strcmp(shards[1], "mydb2") == 0);
    assert(strcmp(shards[2], "mydb3") == 0);
    assert(strcmp(shards[3], "mydb4") == 0);

    // test with buf3 which provokes buffer overflow in cdb2api
    // make sure cannot use mydb as shard name (will be ignored)
    num_hosts = 0;
    num_db_hosts = 0;
    num_shards = 0;
    const char *buf3 = "\
  comdb2dbnm:test_short_hostname,test_long_hostname_xf00fxf00fxf00fxf00fxf00fxf00fxf00fxf00fxf00f,test_overflow_hostname_extra_text_is_truncatedtest_overflow_hostname_extra_text_is_truncatedtest_overflow_hostname_extra_text_is_truncated   \n\
  mydb:test_short_hostname,test_long_hostname_xf00fxf00fxf00fxf00fxf00fxf00fxf00fxf00fxf00f,test_overflow_hostname_extra_text_is_truncatedtest_overflow_hostname_extra_text_is_truncatedtest_overflow_hostname_extra_text_is_truncated   \n\
  partition mydb:test_short_shard,mydb,test_long_shard_xf00fxf00fxf00f,test_overflow_shard_extra_text_is_truncated    \n\
  comdb2_config:default_type:test_overflow_when_assigning_the_default_type_extra_text_is_truncated   \n\
  comdb2_config:room:test_overflow_when_assigning_the_room_extra_text_is_truncated   \n\
  comdb2_config:comdb2dbname:test_overflow_when_assigning_the_dbname_extra_text_is_truncated   \n\
  comdb2_config:dnssuffix:test_overflow_when_assigning_the_dnssuffix_extra_text_is_truncatedtest_overflow_when_assigning_the_dnssuffix_extra_text_is_truncatedtest_overflow_when_assigning_the_dnssuffix_extra_text_is_truncatedtest_overflow_when_assigning_the_dnssuffix_extra_text_is_truncated  \n\
";
    read_comdb2db_cfg(NULL, s, "comdb2dbnm",
                      buf3, comdb2db_hosts,
                      &num_hosts, &comdb2db_num, dbname,
                      db_hosts, &num_db_hosts,
                      &dbnum, &dbname_found,
                      &comdb2db_found, shards, &num_shards);

    assert(num_db_hosts == 3);
    assert(num_hosts == 3);
    assert(strcmp(comdb2db_hosts[0], "test_short_hostname") == 0);
    assert(strcmp(comdb2db_hosts[1], "test_long_hostname_xf00fxf00fxf00fxf00fxf00fxf00fxf00fxf00fxf00f") == 0);
    assert(strcmp(comdb2db_hosts[2], "test_overflow_hostname_extra_text_is_truncatedtest_overflow_hostname_extra_text_is_truncatedtest_overflow_hostname_extra_text_i") == 0);
    assert(strcmp(db_hosts[0], "test_short_hostname") == 0);
    assert(strcmp(db_hosts[1], "test_long_hostname_xf00fxf00fxf00fxf00fxf00fxf00fxf00fxf00fxf00f") == 0);
    assert(strcmp(db_hosts[2], "test_overflow_hostname_extra_text_is_truncatedtest_overflow_hostname_extra_text_is_truncatedtest_overflow_hostname_extra_text_i") == 0);
    assert(strcmp(cdb2_default_cluster, "test_overflow_when_assigning_the_default_type_extra_text_is_tru") == 0);
    assert(strcmp(cdb2_machine_room, "test_overflow_w") == 0);
    assert(strcmp(cdb2_comdb2dbname, "test_overflow_when_assigning_th") == 0);
    assert(strcmp(cdb2_dnssuffix, "test_overflow_when_assigning_the_dnssuffix_extra_text_is_truncatedtest_overflow_when_assigning_the_dnssuffix_extra_text_is_truncatedtest_overflow_when_assigning_the_dnssuffix_extra_text_is_truncatedtest_overflow_when_assigning_the_dnssuffix_extra_text_is") == 0);

    assert(num_shards == 3);
    assert(strcmp(shards[0], "test_short_shard") == 0);
    assert(strcmp(shards[1], "test_long_shard_xf00fxf00fxf00f") == 0);
    assert(strcmp(shards[2], "test_overflow_shard_extra_text_is_truncated") == 0);
}


void test_get_config_file()
{
    char shortname[16];
    int rc = get_config_file("mydb", shortname, sizeof(shortname), 0);
    assert(rc == -1); //does not fit

    char filename[PATH_MAX];
    rc = get_config_file(NULL, filename, sizeof(filename), 0);
    /* NULL dbname is no longer permitted. */
    assert(rc == -1);

    setenv("COMDB2_ROOT", "myroot", 1);
    rc = get_config_file("mydb", filename, sizeof(filename), 0);
    assert(rc == 0);
    assert(strcmp(filename, "myroot/etc/cdb2/config.d/mydb.cfg") == 0);
}


void test_cdb2_string_escape()
{
    const char *emptyStr = "";
    char *testEmptyStr = cdb2_string_escape(NULL, emptyStr);
    assert(strcmp(testEmptyStr, "''") == 0);
    free(testEmptyStr);
    
    const char *simpleStr = "Hello world!";
    char *testSimpleStr = cdb2_string_escape(NULL, simpleStr);
    assert(strcmp(testSimpleStr, "'Hello world!'") == 0);
    free(testSimpleStr);
    
    const char* complexStr = "'As quirky joke, chefs won't pay devil magic zebra tax.''";
    char *testComplexStr = cdb2_string_escape(NULL, complexStr);
    assert(strcmp(testComplexStr, "'''As quirky joke, chefs won''t pay devil magic zebra tax.'''''") == 0);
    free(testComplexStr);
}


void test_result_cache_helpers()
{
    /* Test fnv1a_64 produces consistent hashes */
    uint64_t h1 = fnv1a_64("hello", 5);
    uint64_t h2 = fnv1a_64("hello", 5);
    uint64_t h3 = fnv1a_64("world", 5);
    assert(h1 == h2);
    assert(h1 != h3);

    /* Test cache key building */
    char *key = NULL;
    int key_len = 0;
    uint64_t hash = 0;
    int rc = result_cache_build_key("mydb", "SELECT 1", 0, NULL,
                                    &key, &key_len, &hash);
    assert(rc == 0);
    assert(key != NULL);
    assert(key_len == (int)(strlen("mydb") + 1 + strlen("SELECT 1") + 1));
    assert(hash != 0);

    /* Same inputs produce same key */
    char *key2 = NULL;
    int key_len2 = 0;
    uint64_t hash2 = 0;
    rc = result_cache_build_key("mydb", "SELECT 1", 0, NULL,
                                &key2, &key_len2, &hash2);
    assert(rc == 0);
    assert(hash == hash2);
    assert(key_len == key_len2);
    assert(memcmp(key, key2, key_len) == 0);

    /* Different inputs produce different key */
    char *key3 = NULL;
    int key_len3 = 0;
    uint64_t hash3 = 0;
    rc = result_cache_build_key("mydb", "SELECT 2", 0, NULL,
                                &key3, &key_len3, &hash3);
    assert(rc == 0);
    assert(hash != hash3);

    free(key);
    free(key2);
    free(key3);
}

void test_result_cache_insert_lookup()
{
    /* Enable cache */
    g_result_cache.max_entries = 64;
    g_result_cache.default_ttl_sec = 60;

    /* Build a cache entry */
    struct result_cache_entry *entry = calloc(1, sizeof(*entry));
    char *key = NULL;
    int key_len = 0;
    uint64_t hash = 0;
    result_cache_build_key("testdb", "SELECT x FROM t", 0, NULL,
                           &key, &key_len, &hash);
    entry->key_hash = hash;
    entry->cache_key = key;
    entry->cache_key_len = key_len;
    entry->dbname = strdup("testdb");
    entry->created_at = time(NULL);
    entry->ttl_sec = 60;
    entry->first_buf = NULL;
    entry->first_buf_len = 0;
    entry->rows = NULL;
    entry->n_rows = 0;

    /* Insert */
    result_cache_insert(entry);
    assert(g_result_cache.n_entries == 1);

    /* Lookup - should find it */
    struct result_cache_entry *found =
        result_cache_lookup(hash, key, key_len);
    assert(found == entry);

    /* Lookup with different key - should not find */
    char *key2 = NULL;
    int key_len2 = 0;
    uint64_t hash2 = 0;
    result_cache_build_key("testdb", "SELECT y FROM t", 0, NULL,
                           &key2, &key_len2, &hash2);
    found = result_cache_lookup(hash2, key2, key_len2);
    assert(found == NULL);
    free(key2);

    /* Invalidate by dbname */
    result_cache_invalidate_db("testdb");
    assert(g_result_cache.n_entries == 0);
    found = result_cache_lookup(hash, key, key_len);
    assert(found == NULL);
}

void test_result_cache_eviction()
{
    g_result_cache.max_entries = 2;
    g_result_cache.default_ttl_sec = 60;

    /* Insert 3 entries - should evict oldest */
    for (int i = 0; i < 3; i++) {
        char sql[32];
        snprintf(sql, sizeof(sql), "SELECT %d", i);
        char *key = NULL;
        int key_len = 0;
        uint64_t hash = 0;
        result_cache_build_key("db", sql, 0, NULL,
                               &key, &key_len, &hash);
        struct result_cache_entry *e = calloc(1, sizeof(*e));
        e->key_hash = hash;
        e->cache_key = key;
        e->cache_key_len = key_len;
        e->dbname = strdup("db");
        e->created_at = time(NULL) - (3 - i); /* older entries first */
        e->ttl_sec = 60;
        e->rows = NULL;
        e->n_rows = 0;
        e->first_buf = NULL;
        e->first_buf_len = 0;
        result_cache_insert(e);
    }

    /* Should have 2 entries (evicted the oldest) */
    assert(g_result_cache.n_entries == 2);

    /* The oldest (SELECT 0) should be gone */
    char *key = NULL;
    int key_len = 0;
    uint64_t hash = 0;
    result_cache_build_key("db", "SELECT 0", 0, NULL,
                           &key, &key_len, &hash);
    assert(result_cache_lookup(hash, key, key_len) == NULL);
    free(key);

    /* SELECT 2 should still be there */
    result_cache_build_key("db", "SELECT 2", 0, NULL,
                           &key, &key_len, &hash);
    assert(result_cache_lookup(hash, key, key_len) != NULL);
    free(key);

    /* Cleanup */
    result_cache_clear();
    assert(g_result_cache.n_entries == 0);
}

void test_result_cache_policy()
{
    cdb2_hndl_tp hndl;
    memset(&hndl, 0, sizeof(hndl));
    strcpy(hndl.dbname, "testdb");

    /* No policies - not eligible (unless global enabled with no policies) */
    g_result_cache.max_entries = 0;
    assert(result_cache_is_eligible(&hndl, "SELECT 1") == 0);

    /* Add a policy */
    cdb2_set_cache_policy(&hndl, "SELECT val FROM ref", 30);
    assert(result_cache_is_eligible(&hndl, "SELECT val FROM ref WHERE k=1") == 1);
    assert(result_cache_is_eligible(&hndl, "SELECT other FROM t") == 0);

    /* TTL from policy */
    assert(result_cache_get_ttl(&hndl, "SELECT val FROM ref WHERE k=1") == 30);
    assert(result_cache_get_ttl(&hndl, "SELECT other") == 60);

    /* Clear policies */
    cdb2_clear_cache_policies(&hndl);
    assert(hndl.cache_policies == NULL);
    assert(result_cache_is_eligible(&hndl, "SELECT val FROM ref") == 0);

    /* Global enable without per-handle policies */
    g_result_cache.max_entries = 64;
    assert(result_cache_is_eligible(&hndl, "SELECT anything") == 1);
    g_result_cache.max_entries = 0;
}

void test_result_cache_abort_capture()
{
    cdb2_hndl_tp hndl;
    memset(&hndl, 0, sizeof(hndl));

    /* Simulate in-progress capture */
    hndl.cache_capturing = 1;
    hndl.cache_capture_capacity = 2;
    hndl.cache_capture_n_rows = 2;
    hndl.cache_capture_rows = malloc(2 * sizeof(struct result_cache_row));
    struct result_cache_row *rows = hndl.cache_capture_rows;
    rows[0].buf = malloc(10);
    rows[0].len = 10;
    rows[1].buf = malloc(20);
    rows[1].len = 20;
    hndl.cache_capture_first_buf = malloc(5);
    hndl.cache_capture_first_buf_len = 5;
    hndl.cache_capture_key = strdup("somekey");
    hndl.cache_capture_key_len = 7;

    /* Abort should free everything */
    result_cache_abort_capture(&hndl);
    assert(hndl.cache_capturing == 0);
    assert(hndl.cache_capture_rows == NULL);
    assert(hndl.cache_capture_first_buf == NULL);
    assert(hndl.cache_capture_key == NULL);
    assert(hndl.cache_capture_n_rows == 0);
}

int main(int argc, char *argv[])
{
    int rc = 0;
    printf("starting unit test\n");
    assert(1 == 1);

    test_is_sql_read();
    test_do_init_once();

    test_cdb2_set_min_retries();
    test_cdb2_set_max_retries();

    test_cdb2_hndl_set_min_retries();
    test_cdb2_hndl_set_max_retries();

    test_cdb2_set_comdb2db_config();

    test_result_cache_helpers();
    test_result_cache_insert_lookup();
    test_result_cache_eviction();
    test_result_cache_policy();
    test_result_cache_abort_capture();

    test_read_comdb2db_cfg();
    test_get_config_file();

    test_cdb2_string_escape();

    printf("finished succesfully\n");
    return rc;
}
