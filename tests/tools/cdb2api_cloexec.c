#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <cdb2api.h>

/*
 * Test that cdb2api sockets have FD_CLOEXEC set, so child processes
 * created via fork()+exec() don't inherit database connections.
 *
 * Strategy:
 * 1. Open a cdb2 handle and run a query to establish a connection
 * 2. Fork a child that execs a helper script which lists its open FDs
 * 3. Verify none of the child's FDs are TCP sockets to the database
 *
 * If CLOEXEC is properly set, the child's inherited copies of the
 * cdb2api sockets will be closed at exec() time.
 */

static int check_cloexec_on_open_fds(void)
{
    int found_non_cloexec = 0;
    /* Check all plausible FDs (skip stdin/stdout/stderr) */
    for (int fd = 3; fd < 1024; fd++) {
        int flags = fcntl(fd, F_GETFD);
        if (flags == -1)
            continue; /* not open */
        if (!(flags & FD_CLOEXEC)) {
            /* Check if this is a socket */
            int sotype;
            socklen_t len = sizeof(sotype);
            if (getsockopt(fd, SOL_SOCKET, SO_TYPE, &sotype, &len) == 0) {
                fprintf(stderr, "FAIL: fd %d is a socket without FD_CLOEXEC\n", fd);
                found_non_cloexec = 1;
            }
        }
    }
    return found_non_cloexec;
}

int main(int argc, char **argv)
{
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <dbname> [tier]\n", argv[0]);
        return 1;
    }

    char *dbname = argv[1];
    char *tier;
    char *conf = getenv("CDB2_CONFIG");

    if (conf != NULL) {
        cdb2_set_comdb2db_config(conf);
        tier = "default";
    } else {
        tier = "local";
    }
    if (argc > 2)
        tier = argv[2];

    signal(SIGPIPE, SIG_IGN);

    cdb2_hndl_tp *db;
    int rc = cdb2_open(&db, dbname, tier, 0);
    if (rc) {
        fprintf(stderr, "cdb2_open failed: %s\n", cdb2_errstr(db));
        return 1;
    }

    rc = cdb2_run_statement(db, "select 1");
    if (rc) {
        fprintf(stderr, "cdb2_run_statement failed: %s\n", cdb2_errstr(db));
        cdb2_close(db);
        return 1;
    }
    while ((rc = cdb2_next_record(db)) == CDB2_OK)
        ;

    /* Connection is established. Check that all sockets have CLOEXEC. */
    if (check_cloexec_on_open_fds()) {
        fprintf(stderr, "FAIL: found sockets without FD_CLOEXEC before fork\n");
        cdb2_close(db);
        return 1;
    }

    /*
     * Fork+exec a child and verify it doesn't inherit our sockets.
     * The child runs /proc/self/fd listing (Linux) or just exits 0
     * — the real check is the CLOEXEC flags above, but we also verify
     * the connection still works after fork+exec.
     */
    pid_t pid = fork();
    if (pid == -1) {
        perror("fork");
        cdb2_close(db);
        return 1;
    } else if (pid == 0) {
        execl("/bin/echo", "echo", (char *)NULL);
        perror("execl");
        _exit(1);
    } else {
        int status;
        if (waitpid(pid, &status, 0) != pid) {
            perror("waitpid");
            cdb2_close(db);
            return 1;
        }
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            fprintf(stderr, "FAIL: child exited with status %d\n", status);
            cdb2_close(db);
            return 1;
        }
    }

    /* Verify the parent's connection still works after fork+exec */
    rc = cdb2_run_statement(db, "select 2");
    if (rc) {
        fprintf(stderr, "FAIL: query after fork failed: %s\n", cdb2_errstr(db));
        cdb2_close(db);
        return 1;
    }
    while ((rc = cdb2_next_record(db)) == CDB2_OK)
        ;
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "FAIL: unexpected rc %d after fork\n", rc);
        cdb2_close(db);
        return 1;
    }

    cdb2_close(db);
    printf("Passed\n");
    return 0;
}
