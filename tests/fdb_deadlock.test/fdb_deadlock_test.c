/*
 * Test for FDB deadlock in __lock_wrlock_exclusive
 *
 * This test creates multiple threads that concurrently access foreign
 * database tables. The access pattern is designed to stress the lock
 * acquisition path in _add_table_and_stats_fdb which previously could
 * deadlock when multiple threads competed for fdbs.arr_lock and
 * fdb->h_rwlock.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <cdb2api.h>

#define NUM_THREADS 10
#define ITERATIONS_PER_THREAD 50
#define TIMEOUT_SECONDS 60

static char *local_dbname;
static char *local_config;
static char *remote_dbname;
static char *remote_config;
static int test_failed = 0;
static int threads_done = 0;
static pthread_mutex_t fail_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t print_mutex = PTHREAD_MUTEX_INITIALIZER;

#define thread_printf(fmt, ...) do { \
    pthread_mutex_lock(&print_mutex); \
    printf(fmt, ##__VA_ARGS__); \
    fflush(stdout); \
    pthread_mutex_unlock(&print_mutex); \
} while (0)

static void mark_failed(const char *msg) {
    pthread_mutex_lock(&fail_mutex);
    if (!test_failed) {
        thread_printf("TEST FAILED: %s\n", msg);
        test_failed = 1;
    }
    pthread_mutex_unlock(&fail_mutex);
}

/* Execute a query and consume all results */
static int exec_query(cdb2_hndl_tp *hndl, const char *query, int thread_id) {
    int rc;

    rc = cdb2_run_statement(hndl, query);
    if (rc != 0) {
        thread_printf("Thread %d: cdb2_run_statement failed for query '%s': %s (rc=%d)\n",
                     thread_id, query, cdb2_errstr(hndl), rc);
        return -1;
    }

    /* Consume all rows */
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
        /* Just iterate, don't need to check values */
    }

    if (rc != CDB2_OK_DONE) {
        thread_printf("Thread %d: cdb2_next_record failed: %s (rc=%d)\n",
                     thread_id, cdb2_errstr(hndl), rc);
        return -1;
    }

    return 0;
}

/* Worker thread function */
static void *worker_thread(void *arg) {
    int thread_id = *(int *)arg;
    cdb2_hndl_tp *hndl = NULL;
    int rc;
    char query[512];

    thread_printf("Thread %d: Starting\n", thread_id);

    /* Open connection to local database */
    rc = cdb2_open(&hndl, local_dbname, local_config, 0);
    if (rc != 0) {
        mark_failed("Failed to open database connection");
        thread_printf("Thread %d: cdb2_open failed: rc=%d\n", thread_id, rc);
        return NULL;
    }

    /* Run iterations of foreign database access */
    for (int i = 0; i < ITERATIONS_PER_THREAD && !test_failed; i++) {
        /* Mix of different query types to stress different code paths */

        /* 1. Simple SELECT from foreign table */
        snprintf(query, sizeof(query),
                "SELECT COUNT(*) FROM LOCAL_%s.t", remote_dbname);
        if (exec_query(hndl, query, thread_id) != 0) {
            mark_failed("SELECT COUNT(*) failed");
            break;
        }

        /* 2. SELECT with WHERE clause (forces table metadata access) */
        snprintf(query, sizeof(query),
                "SELECT name FROM LOCAL_%s.t WHERE id = %d",
                remote_dbname, (i % 1000) + 1);
        if (exec_query(hndl, query, thread_id) != 0) {
            mark_failed("SELECT with WHERE failed");
            break;
        }

        /* 3. JOIN between local and foreign table */
        snprintf(query, sizeof(query),
                "SELECT COUNT(*) FROM t JOIN LOCAL_%s.t AS remote ON t.id = remote.id",
                remote_dbname);
        if (exec_query(hndl, query, thread_id) != 0) {
            mark_failed("JOIN query failed");
            break;
        }

        /* 4. Access sqlite_stat1 (this triggers _add_table_and_stats_fdb) */
        snprintf(query, sizeof(query),
                "SELECT COUNT(*) FROM LOCAL_%s.sqlite_stat1",
                remote_dbname);
        if (exec_query(hndl, query, thread_id) != 0) {
            /* sqlite_stat1 might not exist, that's ok */
            thread_printf("Thread %d: sqlite_stat1 query failed (may not exist)\n", thread_id);
        }

        /* 5. Multiple foreign table accesses in one query */
        snprintf(query, sizeof(query),
                "SELECT COUNT(*) FROM LOCAL_%s.t t1, LOCAL_%s.t t2 WHERE t1.id = t2.id LIMIT 10",
                remote_dbname, remote_dbname);
        if (exec_query(hndl, query, thread_id) != 0) {
            mark_failed("Multiple table access failed");
            break;
        }

        if (i % 10 == 0) {
            thread_printf("Thread %d: Completed %d/%d iterations\n",
                         thread_id, i, ITERATIONS_PER_THREAD);
        }

        /* Small random delay to create contention */
        usleep((rand() % 10) * 1000);
    }

    cdb2_close(hndl);

    pthread_mutex_lock(&fail_mutex);
    threads_done++;
    pthread_mutex_unlock(&fail_mutex);

    thread_printf("Thread %d: Completed\n", thread_id);
    return NULL;
}

/* Watchdog thread to detect deadlocks */
static void *watchdog_thread(void *arg) {
    int timeout = TIMEOUT_SECONDS;

    while (timeout > 0) {
        sleep(1);
        timeout--;

        pthread_mutex_lock(&fail_mutex);
        if (test_failed) {
            pthread_mutex_unlock(&fail_mutex);
            return NULL;
        }
        if (threads_done >= NUM_THREADS) {
            pthread_mutex_unlock(&fail_mutex);
            return NULL;
        }
        pthread_mutex_unlock(&fail_mutex);
    }

    mark_failed("DEADLOCK DETECTED: Test timed out after 60 seconds");
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc != 5) {
        fprintf(stderr, "Usage: %s <local_dbname> <local_config> <remote_dbname> <remote_config>\n", argv[0]);
        return 1;
    }

    local_dbname = argv[1];
    local_config = argv[2];
    remote_dbname = argv[3];
    remote_config = argv[4];

    printf("FDB Deadlock Test\n");
    printf("=================\n");
    printf("Local DB: %s (config: %s)\n", local_dbname, local_config);
    printf("Remote DB: %s (config: %s)\n", remote_dbname, remote_config);
    printf("Threads: %d, Iterations per thread: %d\n", NUM_THREADS, ITERATIONS_PER_THREAD);
    printf("\n");

    srand(time(NULL));

    pthread_t threads[NUM_THREADS];
    pthread_t watchdog;
    int thread_ids[NUM_THREADS];

    /* Start watchdog */
    if (pthread_create(&watchdog, NULL, watchdog_thread, NULL) != 0) {
        fprintf(stderr, "Failed to create watchdog thread\n");
        return 1;
    }

    /* Create worker threads */
    for (int i = 0; i < NUM_THREADS; i++) {
        thread_ids[i] = i;
        if (pthread_create(&threads[i], NULL, worker_thread, &thread_ids[i]) != 0) {
            fprintf(stderr, "Failed to create thread %d\n", i);
            mark_failed("Failed to create threads");
            break;
        }
    }

    /* Wait for all threads to complete */
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    /* Signal watchdog to exit */
    pthread_join(watchdog, NULL);

    printf("\n");
    printf("Test Summary\n");
    printf("============\n");
    printf("Threads completed: %d/%d\n", threads_done, NUM_THREADS);

    if (test_failed) {
        printf("Result: FAILED\n");
        return 1;
    }

    if (threads_done != NUM_THREADS) {
        printf("Result: FAILED (not all threads completed)\n");
        return 1;
    }

    printf("Result: PASSED - No deadlock detected\n");
    return 0;
}
