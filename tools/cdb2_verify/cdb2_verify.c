/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "build/db_config.h"

#if 0
static const char copyright[] =
    "Copyright (c) 1996-2003\nSleepycat Software Inc.  All rights reserved.\n";
static const char revid[] =
    "$Id: db_verify.c,v 1.45 2003/08/13 19:57:09 ubell Exp $";
#endif

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#endif

#include "build/db_int.h"
#include <crc32c.h>
#include "sys_wrap.h"

int main __P((int, char *[]));
static int cdb2_verify_usage __P((void));
static int cdb2_verify_version_check __P((const char *));
extern int comdb2ma_init(size_t init_sz, size_t max_cap);

int
tool_cdb2_verify_main(argc, argv)
	int argc;
	char *argv[];
{
	extern char *optarg;
	extern int optind;
	const char *progname = "cdb2_verify";
	DB *dbp, *dbp1;
	DB_ENV *dbenv;
	u_int32_t cache;
	int ch, exitval, nflag, oflag, rflag, private;
	int quiet, resize, ret;
	char *home, passwd[1024];
	FILE *crypto;

	crc32c_init(0);
    comdb2ma_init(0, 0);
	if ((ret = cdb2_verify_version_check(progname)) != 0)
		return (ret);
	Pthread_key_create(&DBG_FREE_CURSOR, NULL);

	dbenv = NULL;
	dbp = NULL;
	cache = MEGABYTE;
	exitval = nflag = oflag = rflag = quiet = 0;
	home = NULL;
	memset(passwd, 0, sizeof(passwd));

	while ((ch = getopt(argc, argv, "h:NorP:qV")) != EOF)
		switch (ch) {
		case 'h':
			home = optarg;
			break;
		case 'N':
			nflag = 1;
			break;
		case 'P':
			crypto = fopen(optarg, "r");
			if (crypto == NULL) {
				fprintf(stderr, "%s fopen(s) %s errno:%d (%s)\n", __func__,
						optarg, errno, strerror(errno));
				exit(1);
			}
			if ((fgets(passwd, sizeof(passwd), crypto)) == NULL) {
				fprintf(stderr, "%s fgets returned NULL -- ferror:%d feof:%d errno:%d (%s)\n",
						__func__, ferror(crypto), feof(crypto), errno, strerror(errno));
				exit(1);
			}
			fclose(crypto);
			break;
		case 'o':
			oflag = 1;
			break;
		case 'r':
			rflag = 1;
			break;
		case 'q':
			quiet = 1;
			break;
		case 'V':
			printf("%s\n", db_version(NULL, NULL, NULL));
			return (EXIT_SUCCESS);
		case '?':
		default:
			return (cdb2_verify_usage());
		}
	argc -= optind;
	argv += optind;

	if (argc <= 0)
		return (cdb2_verify_usage());

	/* Handle possible interruptions. */
	__db_util_siginit();

	/*
	 * Create an environment object and initialize it for error
	 * reporting.
	 */
retry:	if ((ret = db_env_create(&dbenv, 0)) != 0) {
		fprintf(stderr,
		    "%s: db_env_create: %s\n", progname, db_strerror(ret));
		goto shutdown;
	}

	if (!quiet) {
		dbenv->set_errfile(dbenv, stderr);
		dbenv->set_errpfx(dbenv, progname);
	}

	if (nflag) {
		if ((ret = dbenv->set_flags(dbenv, DB_NOLOCKING, 1)) != 0) {
			dbenv->err(dbenv, ret, "set_flags: DB_NOLOCKING");
			goto shutdown;
		}
		if ((ret = dbenv->set_flags(dbenv, DB_NOPANIC, 1)) != 0) {
			dbenv->err(dbenv, ret, "set_flags: DB_NOPANIC");
			goto shutdown;
		}
	}

	if (strlen(passwd) > 0 &&
	    (ret = dbenv->set_encrypt(dbenv, passwd, DB_ENCRYPT_AES)) != 0) {
		dbenv->err(dbenv, ret, "set_passwd");
		goto shutdown;
	}

	/*
	 * Declare a reasonably large cache so that we don't fail when verifying
	 * large databases.
	 */
	if ((ret = dbenv->set_cachesize(dbenv, 0, cache, 1)) != 0) {
		dbenv->err(dbenv, ret, "set_cachesize");
		goto shutdown;
	}
	private = 1;
	int flags = DB_CREATE | DB_INIT_MPOOL | DB_PRIVATE | DB_USE_ENVIRON;
	if ((ret = dbenv->open(dbenv, home, flags, 0)) != 0) {
		dbenv->err(dbenv, ret, "open");
		goto shutdown;
	}

	for (; !__db_util_interrupted() && argv[0] != NULL; ++argv) {
		if ((ret = db_create(&dbp, dbenv, 0)) != 0) {
			dbenv->err(dbenv, ret, "%s: db_create", progname);
			goto shutdown;
		}

		/*
		 * We create a 2nd dbp to this database to get its pagesize
		 * because the dbp we're using for verify cannot be opened.
		 */
		if (private) {
			if ((ret = db_create(&dbp1, dbenv, 0)) != 0) {
				dbenv->err(
				    dbenv, ret, "%s: db_create", progname);
				goto shutdown;
			}

			if ((ret = dbp1->open(dbp1, NULL,
			    argv[0], NULL, DB_UNKNOWN, DB_RDONLY, 0)) != 0) {
				dbenv->err(dbenv, ret, "DB->open: %s", argv[0]);
				(void)dbp1->close(dbp1, 0);
				goto shutdown;
			}
			/*
			 * If we get here, we can check the cache/page.
			 * !!!
			 * If we have to retry with an env with a larger
			 * cache, we jump out of this loop.  However, we
			 * will still be working on the same argv when we
			 * get back into the for-loop.
			 */
			ret = __db_util_cache(dbenv, dbp1, &cache, &resize);
			(void)dbp1->close(dbp1, 0);
			if (ret != 0)
				goto shutdown;

			if (resize) {
				(void)dbp->close(dbp, 0);
				dbp = NULL;

				(void)dbenv->close(dbenv, 0);
				dbenv = NULL;
				goto retry;
			}
		}

        int verifyflags = 0;
        if (oflag)
            verifyflags |= DB_NOORDERCHK;
        if (rflag)
            verifyflags |= DB_RECCNTCHK;
		/* The verify method is a destructor. */
		ret = dbp->verify(dbp,
		    argv[0], NULL, NULL, verifyflags);
		dbp = NULL;
		if (ret != 0) {
			dbenv->err(dbenv, ret, "DB->verify: %s", argv[0]);
			goto shutdown;
		}
	}

	if (0) {
shutdown:	exitval = 1;
	}

	if (dbp != NULL && (ret = dbp->close(dbp, 0)) != 0) {
		exitval = 1;
		dbenv->err(dbenv, ret, "close");
	}
	if (dbenv != NULL && (ret = dbenv->close(dbenv, 0)) != 0) {
		exitval = 1;
		fprintf(stderr,
		    "%s: dbenv->close: %s\n", progname, db_strerror(ret));
	}

	/* Resend any caught signal. */
	__db_util_sigresend();

	return (exitval == 0 ? EXIT_SUCCESS : EXIT_FAILURE);
}

static int
cdb2_verify_usage()
{
	fprintf(stderr,
       "usage: cdb2_verify [-NorqV] [-h home] [-P /path/to/password] db_file ...\n"
        "  -h       this usage help message\n"
        "  -N       set db env to no locking\n"
        "  -o       order check\n"
        "  -r       record count check\n"
        "  -q       quiet mode\n"
        "  -V       print version information\n"
        );

	return (EXIT_FAILURE);
}

int
cdb2_verify_version_check(progname)
	const char *progname;
{
	int v_major, v_minor, v_patch;

	/* Make sure we're loaded with the right version of the DB library. */
	(void)db_version(&v_major, &v_minor, &v_patch);
	if (v_major != DB_VERSION_MAJOR || v_minor != DB_VERSION_MINOR) {
		fprintf(stderr,
	"%s: version %d.%d doesn't match library version %d.%d\n",
		    progname, DB_VERSION_MAJOR, DB_VERSION_MINOR,
		    v_major, v_minor);
		return (EXIT_FAILURE);
	}
	return (0);
}
