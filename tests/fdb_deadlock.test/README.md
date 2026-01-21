# FDB Deadlock Test

## Purpose

This test validates the fix for a deadlock issue in `__lock_wrlock_exclusive` (db/fdb_fend.c:1568) that occurred when multiple threads concurrently accessed foreign database tables.

## Deadlock Scenario

The original deadlock occurred due to a lock ordering violation:

1. **Thread A**: Held `fdbs.arr_lock` (read mode) and blocked trying to acquire `fdb->h_rwlock` (write mode)
2. **Thread B**: Held `fdb->h_rwlock` and needed `fdbs.arr_lock`
3. Result: Circular dependency causing deadlock

## The Fix

The fix (implemented in db/fdb_fend.c) addresses this by:

1. Using `pthread_rwlock_trywrlock()` instead of blocking `Pthread_rwlock_wrlock()`
2. Immediately releasing `fdbs.arr_lock` if `fdb->h_rwlock` cannot be acquired
3. Adding proper user count synchronization with `users_mtx`
4. Adding sleep on retry to avoid busy-spinning

## Test Description

The test:

1. Creates a local database and a remote (foreign) database
2. Spawns 10 concurrent threads
3. Each thread performs 50 iterations of various foreign database queries:
   - Simple SELECT queries
   - WHERE clause queries (forces table metadata access)
   - JOINs between local and foreign tables
   - Access to sqlite_stat1 (triggers `_add_table_and_stats_fdb`)
   - Multiple foreign table accesses in one query

4. A watchdog thread monitors for deadlock (60 second timeout)
5. Test passes if all threads complete without deadlock

## Running the Test

From the tests/ directory:

```bash
make fdb_deadlock
```

Or from the test directory:

```bash
make setup
make fdb_deadlock
```

## Expected Behavior

- **With the fix**: Test completes in ~10-20 seconds with all threads finishing successfully
- **Without the fix**: Test would hang/timeout as threads deadlock trying to acquire locks

## Test Output

Successful run:
```
FDB Deadlock Test
=================
Local DB: testdb (config: /path/to/config)
Remote DB: testdb2 (config: /path/to/config)
Threads: 10, Iterations per thread: 50

Thread 0: Starting
Thread 1: Starting
...
Thread 0: Completed 10/50 iterations
...
Thread 0: Completed
Thread 1: Completed
...

Test Summary
============
Threads completed: 10/10
Result: PASSED - No deadlock detected
```

Failed run (deadlock):
```
...
Thread 5: Completed 30/50 iterations
(hangs here)
TEST FAILED: DEADLOCK DETECTED: Test timed out after 60 seconds

Test Summary
============
Threads completed: 5/10
Result: FAILED
```

## Related Code

- `db/fdb_fend.c:1568` - `__lock_wrlock_exclusive()` function
- `db/fdb_fend.c:954` - `_add_table_and_stats_fdb()` caller
- `db/fdb_fend.c:835` - Another caller in `new_fdb()`
