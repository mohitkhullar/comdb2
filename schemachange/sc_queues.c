/*
   Copyright 2015 Bloomberg Finance L.P.

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

#include "schemachange.h"
#include "sc_queues.h"
#include "translistener.h"
#include "sc_schema.h"
#include "logmsg.h"

extern int dbqueue_add_consumer(struct dbtable *db, int consumern,
                                const char *method, int noremove);

int consumer_change(const char *queuename, int consumern, const char *method)
{
    struct dbtable *db;
    int rc;

    db = getqueuebyname(queuename);
    if (!db) {
        logmsg(LOGMSG_ERROR, "no such queue '%s'\n", queuename);
        return -1;
    }

    /* Do the change.  If it works locally then assume that it will work
     * globally. */
    rc = dbqueuedb_add_consumer(db, consumern, method, 0);
    fix_consumers_with_bdblib(thedb);
    if (rc == 0) {
        rc = broadcast_add_consumer(queuename, consumern, method);
    }

    logmsg(LOGMSG_WARN, "consumer change %s-%d-%s %s\n", queuename, consumern,
           method, rc == 0 ? "SUCCESS" : "FAILED");

    if (rc == 0) {
        logmsg(LOGMSG_WARN, "**************************************\n");
        logmsg(LOGMSG_WARN, "* BE SURE TO FOLLOW UP BY MAKING THE *\n");
        logmsg(LOGMSG_WARN, "* APPROPRIATE CHANGE TO THE LRL FILE *\n");
        logmsg(LOGMSG_WARN, "* ON EACH CLUSTER NODE               *\n");
        logmsg(LOGMSG_WARN, "**************************************\n");
    }

    return rc;
}

int do_alter_queues_int(struct schema_change_type *sc)
{
    struct dbtable *db;
    int rc;
    db = getqueuebyname(sc->tablename);
    if (db == NULL) {
        /* create new queue */
        rc = add_queue_to_environment(sc->tablename, sc->avgitemsz,
                                      sc->pagesize);
        /* tell other nodes to follow suit */
        broadcast_add_new_queue(
            sc->tablename, sc->avgitemsz); // TODO Check the return value ??????
    } else {
        /* TODO - change item size in existing queue */
        logmsg(LOGMSG_ERROR,
               "do_queue_change: changing existing queues not supported yet\n");
        rc = -1;
    }

    return rc;
}

int static remove_from_qdbs(struct dbtable *db)
{
    for (int i = 0; i < thedb->num_qdbs; i++) {
        if (db == thedb->qdbs[i]) {

            /* Remove the queue from the hash. */
            hash_del(thedb->qdb_hash, db);

            /* Shift the rest down one slot. */
            --thedb->num_qdbs;
            for (int j = i; j < thedb->num_qdbs; ++j) {
                thedb->qdbs[j] = thedb->qdbs[j + 1];
            }
            return 0;
        }
    }
    return -1;
}

int add_queue_to_environment(char *table, int avgitemsz, int pagesize)
{
    struct dbtable *newdb;
    int bdberr;

    /* regardless of success, the fact that we are getting asked to do this is
     * enough to indicate that any backup taken during this period may be
     * suspect. */
    gbl_sc_commit_count++;

    if (pagesize <= 0) {
        pagesize = bdb_queue_best_pagesize(avgitemsz);
        logmsg(LOGMSG_WARN,
               "Using recommended pagesize %d for avg item size %d\n", pagesize,
               avgitemsz);
    }

    newdb = newqdb(thedb, table, avgitemsz, pagesize, 0);
    if (newdb == NULL) {
        logmsg(LOGMSG_ERROR, "add_queue_to_environment:newqdb failed\n");
        return SC_INTERNAL_ERROR;
    }

    if (newdb->dbenv->master == gbl_mynode) {
        /* I am master: create new db */
        newdb->handle =
            bdb_create_queue(newdb->tablename, thedb->basedir, avgitemsz,
                             pagesize, thedb->bdb_env, 0, &bdberr);
    } else {
        /* I am NOT master: open replicated db */
        newdb->handle =
            bdb_open_more_queue(newdb->tablename, thedb->basedir, avgitemsz,
                                pagesize, thedb->bdb_env, 0, NULL, &bdberr);
    }
    if (newdb->handle == NULL) {
        logmsg(LOGMSG_ERROR, "bdb_open:failed to open queue %s/%s, rcode %d\n",
               thedb->basedir, newdb->tablename, bdberr);
        return SC_BDB_ERROR;
    }
    thedb->qdbs =
        realloc(thedb->qdbs, (thedb->num_qdbs + 1) * sizeof(struct dbtable *));
    thedb->qdbs[thedb->num_qdbs++] = newdb;

    /* Add queue to the hash. */
    hash_add(thedb->qdb_hash, newdb);

    return SC_OK;
}

/* We are on on replicant, being called from scdone.  just create local
 * structures (db/consumer).
 * Lots of this code is in common with master, maybe call this from
 * perform_trigger_update()? */
int perform_trigger_update_replicant(const char *queue_name, scdone_t type)
{
    struct dbtable *db = NULL;
    int rc;
    void *tran = NULL;
    char *config;
    int ndests;
    int compr;
    char **dests;
    uint32_t lid = 0;
    extern uint32_t gbl_rep_lockid;
    int bdberr;

    /* Queue information should already be in llmeta. Fetch it and create
     * queue/consumer handles.  Use a transaction with gbl_rep_lockid to
     * querry (see comment in scdone_callback). */
    tran = bdb_tran_begin(thedb->bdb_env, NULL, &bdberr);
    if (tran == NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d can't begin transaction rc %d\n", __FILE__,
               __LINE__, bdberr);
        rc = bdberr;
        goto done;
    }

    bdb_get_tran_lockerid(tran, &lid);
    bdb_set_tran_lockerid(tran, gbl_rep_lockid);

    rc = bdb_lock_tablename_write(thedb->bdb_env, "comdb2_queues", tran);
    if (rc) {
        logmsg(LOGMSG_ERROR, "Error %d getting tablelock for comdb2_queues\n", rc);
        goto done;
    }


    /* TODO: assert we are holding the write-lock on the queue */
    if (type != llmeta_queue_drop) {
        rc = bdb_llmeta_get_queue(tran, (char *)queue_name, &config, &ndests,
                                  &dests, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "bdb_llmeta_get_queue %s rc %d bdberr %d\n",
                   queue_name, rc, bdberr);
            return rc;
        }
    }

    if (type == llmeta_queue_add) {
        /* Legacy schemachange mode - we could have restarted and opened files
         * already. Make this scdone a no-op. We trust that master did
         * necessary checks before adding this queue */
        if (getqueuebyname(queue_name) != NULL) {
            rc = 0;
            goto done;
        }
        rc = javasp_do_procedure_op(JAVASP_OP_LOAD, queue_name, NULL, config);
        if (rc) {
            /* TODO: fatal error? */
            logmsg(LOGMSG_ERROR, "%s: javasp_do_procedure_op returned rc %d\n",
                   __func__, rc);
            goto done;
        }

        db = newqdb(thedb, queue_name, 65536 /* TODO: pass from comdb2sc? */,
                    65536, 1);
        if (db == NULL) {
            logmsg(LOGMSG_ERROR, "can't allocate new queue table entry\n");
            rc = -1;
            goto done;
        }
        db->handle =
            bdb_open_more_queue(queue_name, thedb->basedir, 65536, 65536,
                                thedb->bdb_env, 1, tran, &bdberr);
        if (db->handle == NULL) {
            logmsg(LOGMSG_ERROR,
                   "bdb_open:failed to open queue %s/%s, rcode %d\n",
                   thedb->basedir, db->tablename, bdberr);
            rc = -1;
            goto done;
        }
        thedb->qdbs =
            realloc(thedb->qdbs, (thedb->num_qdbs + 1) * sizeof(struct dbtable *));
        thedb->qdbs[thedb->num_qdbs++] = db;

        /* Add queue to the hash. */
        hash_add(thedb->qdb_hash, db);

        /* TODO: needs locking */
        rc =
            dbqueuedb_add_consumer(db, 0, dests[0] /* TODO: multiple dests */, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR, "can't add consumer to queueu\n");
            rc = -1;
            goto done;
        }

        rc = bdb_queue_consumer(db->handle, 0, 1, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s: bdb_queue_consumer returned rc %d bdberr %d\n",
                   __func__, rc, bdberr);
            rc = -1;
            goto done;
        }
    } else if (type == llmeta_queue_alter) {
        db = getqueuebyname(queue_name);
        if (db == NULL) {
            logmsg(LOGMSG_ERROR, "%s: %s is not a valid trigger\n", __func__,
                   queue_name);
            rc = -1;
            goto done;
        }

        /* TODO: needs locking */
        rc =
            dbqueuedb_add_consumer(db, 0, dests[0] /* TODO: multiple dests */, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR, "can't add consumer to queue\n");
            rc = -1;
            goto done;
        }

        javasp_do_procedure_op(JAVASP_OP_RELOAD, queue_name, NULL, config);

        if (rc) {
            /* TODO: fatal error? */
            logmsg(LOGMSG_ERROR, "%s: javasp_do_procedure_op returned rc %d\n",
                   __func__, rc);
            rc = -1;
            goto done;
        }

    } else if (type == llmeta_queue_drop) {
        /* get us out of database list */
        db = getqueuebyname(queue_name);
        if (db == NULL) {
            logmsg(LOGMSG_ERROR, "unexpected: replicant can't find queue %s\n",
                   queue_name);
            rc = -1;
            goto done;
        }

        rc = bdb_queue_consumer(db->handle, 0, 0, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s: bdb_queue_consumer returned rc %d bdberr %d\n",
                   __func__, rc, bdberr);
            rc = -1;
            goto done;
        }

        javasp_do_procedure_op(JAVASP_OP_UNLOAD, queue_name, NULL, config);

        remove_from_qdbs(db);

        /* close */
        rc = bdb_close_only(db->handle, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: bdb_close_only rc %d bdberr %d\n",
                   __func__, rc, bdberr);
            rc = -1;
            goto done;
        }
    } else {
        logmsg(LOGMSG_ERROR, "unexpected trigger action %d\n", type);
        rc = -1;
        goto done;
    }

    compr = 0;
    if (type != llmeta_queue_drop) {
        if (get_db_queue_odh_tran(db, &db->odh, tran) != 0 || db->odh == 0) {
            db->odh = 0;
        } else {
            get_db_queue_compress_tran(db, &compr, tran);
        }
        bdb_set_queue_odh_options(db->handle, db->odh, compr);
    }

done:
    if (tran) {
        bdb_set_tran_lockerid(tran, lid);
        rc = bdb_tran_abort(thedb->bdb_env, tran, &bdberr);
        if (rc) {
            logmsg(LOGMSG_FATAL, "%s:%d failed to abort transaction\n",
                   __FILE__, __LINE__);
            exit(1);
        }
    }
    return rc;
}

static inline void set_empty_queue_options(struct schema_change_type *s)
{
    if (gbl_init_with_queue_odh == 0)
        gbl_init_with_queue_compr = 0;
    if (s->headers == -1)
        s->headers = gbl_init_with_queue_odh;
    if (s->compress == -1)
        s->compress = gbl_init_with_queue_compr;
    if (s->compress_blobs == -1)
        s->compress_blobs = 0;
    if (s->ip_updates == -1)
        s->ip_updates = 0;
    if (s->instant_sc == -1)
        s->instant_sc = 0;
}

extern int get_physical_transaction(bdb_state_type *bdb_state,
        tran_type *logical_tran, tran_type **outtran, int force_commit);

static int perform_trigger_update_int(struct schema_change_type *sc)
{
    char *config = sc->newcsc2;
    int same_tran = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SC_DONE_SAME_TRAN);
    

    /* we are on on master
     * 1) write config/destinations to llmeta
     * 2) create table in thedb->dbs
     * 3) stop/start threads for consumers, as needed
     * 4) send scdone, like any other sc
     */
    struct dbtable *db = NULL;
    tran_type *tran = NULL, *ltran = NULL;
    int rc = 0;
    int bdberr = 0;
    struct ireq iq;
    scdone_t scdone_type = llmeta_queue_add;
    SBUF2 *sb = sc->sb;

    set_empty_queue_options(sc);

    init_fake_ireq(thedb, &iq);
    iq.usedb = &thedb->static_table;

    if (same_tran) {
        rc = trans_start_logical_sc(&iq, &ltran);
        if (rc) {
            sbuf2printf(sb, "!Error %d creating logical transaction for %s.\n",
                    rc, sc->tablename);
            sbuf2printf(sb, "FAILED\n");
            goto done;
        }
        bdb_ltran_get_schema_lock(ltran);
        rc = get_physical_transaction(thedb->bdb_env, ltran, &tran, 0);
        if (rc != 0 || tran == NULL) {
            sbuf2printf(sb, "!Error %d creating physical transaction for %s.\n",
                    rc, sc->tablename);
            sbuf2printf(sb, "FAILED\n");
            goto done;
        }
    } else {
        rc = trans_start(&iq, NULL, (void *)&tran);
        if (rc) {
            sbuf2printf(sb, "!Error %d creating a transaction for %s.\n", rc,
                    sc->tablename);
            sbuf2printf(sb, "FAILED\n");
            goto done;
        }
    }

    rc = bdb_lock_tablename_write(thedb->bdb_env, "comdb2_queues", tran);
    if (rc) {
        sbuf2printf(sb, "!Error %d getting tablelock for %s.\n", rc,
                    sc->tablename);
        sbuf2printf(sb, "FAILED\n");
        goto done;
    }

    rc = bdb_lock_tablename_write(thedb->bdb_env, sc->tablename, tran);
    if (rc) {
        sbuf2printf(sb, "!Error %d getting tablelock for %s.\n", rc,
                    sc->tablename);
        sbuf2printf(sb, "FAILED\n");
        goto done;
    }

    db = get_dbtable_by_name(sc->tablename);
    if (db) {
        sbuf2printf(sb, "!Trigger name %s clashes with existing table.\n",
                    sc->tablename);
        sbuf2printf(sb, "FAILED\n");
        goto done;
    }
    db = getqueuebyname(sc->tablename);

    /* dropping/altering a queue that doesn't exist? */
    if ((sc->drop_table || sc->alteronly) && db == NULL) {
        sbuf2printf(sb, "!Trigger %s doesn't exist.\n", sc->tablename);
        sbuf2printf(sb, "FAILED\n");
        goto done;
    }
    /* adding a queue that already exists? */
    else if (sc->addonly && db != NULL) {
        sbuf2printf(sb, "!Trigger %s already exists.\n", sc->tablename);
        sbuf2printf(sb, "FAILED\n");
        goto done;
    }
    if (sc->addonly) {
        if (javasp_exists(sc->tablename)) {
            sbuf2printf(sb, "!Procedure %s already exists.\n", sc->tablename);
            sbuf2printf(sb, "FAILED\n");
            goto done;
        }
    }

    if ((rc = check_option_queue_coherency(sc, db)))
        goto done;

    /* TODO: other checks: procedure with this name must not exist either */

    char **dests;

    if (sc->addonly || sc->alteronly) {
        struct dest *d;

        dests = malloc(sizeof(char *) * sc->dests.count);
        if (dests == NULL) {
            sbuf2printf(sb, "!Can't allocate memory for destination list\n");
            logmsg(LOGMSG_ERROR,
                   "Can't allocate memory for destination list\n");
            goto done;
        }
        int i;
        for (i = 0, d = sc->dests.top; d; d = d->lnk.next, i++)
            dests[i] = d->dest;

        /* check first - backing things out gets difficult once we've done
         * things */
        if (dbqueuedb_check_consumer(dests[0])) {
            sbuf2printf(sb,
                        "!Can't load procedure - check config/destinations?\n");
            sbuf2printf(sb, "FAILED\n");
            rc = -1;
            goto done;
        }
    }

    /* For addding, there's no queue and no consumer/procedure, etc., so create
     * those first.  For
     * other methods, we need to manage the existing consumer first. */
    if (sc->addonly) {
        rc = bdb_llmeta_add_queue(thedb->bdb_env, tran, sc->tablename, config,
                                  sc->dests.count, dests, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: bdb_llmeta_add_queue returned %d\n",
                   __func__, rc);
            goto done;
        }

        /* create a procedure (needs to go away, badly) */
        rc =
            javasp_do_procedure_op(JAVASP_OP_LOAD, sc->tablename, NULL, config);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: javasp_do_procedure_op returned rc %d\n",
                   __func__, rc);
            sbuf2printf(sb,
                        "!Can't load procedure - check config/destinations?\n");
            sbuf2printf(sb, "FAILED\n");
            goto done;
        }

        scdone_type = llmeta_queue_add;

        db = newqdb(thedb, sc->tablename, 65536 /* TODO: pass from comdb2sc? */,
                    65536, 1);
        if (db == NULL) {
            logmsg(LOGMSG_ERROR, "%s: newqdb returned NULL\n", __func__);
            goto done;
        }

        /* I am master: create new db */
        db->handle =
            bdb_create_queue_tran(tran, db->tablename, thedb->basedir, 65536,
                                  65536, thedb->bdb_env, 1, &bdberr);
        if (db->handle == NULL) {
            logmsg(LOGMSG_ERROR,
                   "bdb_open:failed to open queue %s/%s, rcode %d\n",
                   thedb->basedir, db->tablename, bdberr);
            goto done;
        }

        if (sc->headers == 1) {
            if ((rc = put_db_queue_odh(db, tran, sc->headers)) != 0) {
                logmsg(LOGMSG_ERROR, "failed to set odh for queue, rcode %d\n",
                        rc);
                goto done;
            }

            if ((rc = put_db_queue_compress(db, tran, sc->compress)) != 0) {
                logmsg(LOGMSG_ERROR, "failed to set queue-compression, rcode "
                        "%d\n", rc);
                goto done;
            }
        }

        db->odh = sc->headers;
        bdb_set_queue_odh_options(db->handle, sc->headers, sc->compress);

        /* Add queue to the hash. */
        hash_add(thedb->qdb_hash, db);

        /* create a consumer for this guy */
        /* TODO: needs locking */
        rc = dbqueuedb_add_consumer(
            db, 0, dests[0] /* TODO: multiple destinations */, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: newqdb returned NULL\n", __func__);
            goto done;
        }

        rc = bdb_queue_consumer(db->handle, 0, 1, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s: bdb_queue_consumer returned rc %d bdberr %d\n",
                   __func__, rc, bdberr);
            goto done;
        }
    } else if (sc->alteronly) {
        rc = bdb_llmeta_alter_queue(thedb->bdb_env, tran, sc->tablename, config,
                                    sc->dests.count, dests, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: bdb_llmeta_alter_queue returned %d\n",
                   __func__, rc);
            goto done;
        }

        if (sc->headers == 1) {
            if ((rc = put_db_queue_odh(db, tran, sc->headers)) != 0) {
                logmsg(LOGMSG_ERROR, "failed to set odh for queue, rcode %d\n",
                        rc);
                goto done;
            }

            if ((rc = put_db_queue_compress(db, tran, sc->compress)) != 0) {
                logmsg(LOGMSG_ERROR, "failed to set queue-compress, rcode %d\n",
                        rc);
                goto done;
            }
        }

        db->odh = sc->headers;
        bdb_set_queue_odh_options(db->handle, sc->headers, sc->compress);

        scdone_type = llmeta_queue_alter;

        /* stop */
        dbqueuedb_stop_consumers(db);
        rc = javasp_do_procedure_op(JAVASP_OP_RELOAD, db->tablename, NULL,
                                    config);
        if (rc) {
            sbuf2printf(sb,
                        "!Can't load procedure - check config/destinations?\n");
            sbuf2printf(sb, "FAILED\n");
        }

        /* TODO: needs locking */
        rc = dbqueuedb_add_consumer(
            db, 0, dests[0] /* TODO: multiple destinations */, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: newqdb returned NULL\n", __func__);
            goto done;
        }

        /* start - see the ugh above. */
        dbqueuedb_restart_consumers(db);
    } else if (sc->drop_table) {
        /* get us out of llmeta */
        rc = bdb_llmeta_drop_queue(db->handle, tran, db->tablename, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: bdb_llmeta_drop_queue rc %d bdberr %d\n",
                   __func__, rc, bdberr);
            goto done;
        }

        scdone_type = llmeta_queue_drop;
        /* stop */
        dbqueuedb_stop_consumers(db);

        javasp_do_procedure_op(JAVASP_OP_UNLOAD, db->tablename, NULL, config);

        /* get us out of database list */
        remove_from_qdbs(db);

        /* close */
        rc = bdb_close_only_sc(db->handle, tran, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: bdb_close_only rc %d bdberr %d\n",
                   __func__, rc, bdberr);
            goto done;
        }
    }

    if (!same_tran) {
        rc = trans_commit(&iq, tran, gbl_mynode);
        tran = NULL;
        if (rc) {
            sbuf2printf(sb, "!Failed to commit transaction\n");
            goto done;
        }
    }


    /* log for replicants to do the same */
    if (!same_tran) {
        rc = bdb_llog_scdone(db->handle, scdone_type, 1, &bdberr);

        if (rc) {
            sbuf2printf(sb, "!Failed to broadcast queue %s\n",
                        sc->drop_table ? "drop" : "add");
            logmsg(LOGMSG_ERROR, "Failed to broadcast queue %s\n",
                   sc->drop_table ? "drop" : "add");
            /* Some replicant(s) timed-out, but master (us) committed successfully. There is no backing out of this */
            //goto done;
        }
    }

    /* TODO: This is fragile - all the actions for the queue should be in one
     * transaction, including the
     * scdone. This needs to be a separate transaction right now because the
     * file handle is stil open on the
     * replicant until the scdone, and we can't delete it until it's closed. */
    if (sc->drop_table) {
        if (!same_tran) {
            rc = trans_start(&iq, NULL, (void *)&tran);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: trans_start rc %d\n", __func__, rc);
                goto done;
            }
        }

        unsigned long long ver = 0;
        if (bdb_get_file_version_qdb(db->handle, tran, &ver, &bdberr) == 0) {
            sc_del_unused_files_tran(db, tran);
        } else {
            rc = bdb_del(db->handle, tran, &bdberr);
        }
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: bdb_close_only rc %d bdberr %d\n",
                   __func__, rc, bdberr);
            goto done;
        }

        if (!same_tran) {
            rc = trans_commit(&iq, tran, gbl_mynode);
            tran = NULL;
            if (rc) {
                sbuf2printf(sb, "!Failed to commit transaction\n");
                goto done;
            }
        }
    }

    if (same_tran) {
        rc = bdb_llog_scdone_tran(db->handle, scdone_type, tran, sc->tablename,
                                  &bdberr);
        if (rc) {
            sbuf2printf(sb, "!Failed write scdone , rc=%d\n", rc);
            goto done;
        }
        rc = trans_commit(&iq, ltran, gbl_mynode);
        tran = NULL;
        ltran = NULL;
        if (rc || bdberr != BDBERR_NOERROR) {
            sbuf2printf(sb, "!Failed to commit transaction, rc=%d\n", rc);
            goto done;
        }
    }

    if (sc->addonly && db && rc == 0) {
        thedb->qdbs =
            realloc(thedb->qdbs, (thedb->num_qdbs + 1) * sizeof(struct dbtable *));
        thedb->qdbs[thedb->num_qdbs++] = db;
    }

done:
    if (ltran) bdb_tran_abort(thedb->bdb_env, ltran, &bdberr);
    else if (tran) bdb_tran_abort(thedb->bdb_env, tran, &bdberr);

    dbqueuedb_admin(thedb, NULL);

    if (rc) {
        logmsg(LOGMSG_ERROR, "%s rc:%d\n", __func__, rc);
    }
    return !rc && !sc->finalize ? SC_COMMIT_PENDING : rc;
    // This function does not have the "finalize" behaviour but it needs to
    // return a proper return code
    // depending on where it is being executed from.
}

int perform_trigger_update(struct schema_change_type *sc)
{
    wrlock_schema_lk();
    javasp_do_procedure_op_pre();
    // At this point, no block-processor can operate on queues
    int rc = perform_trigger_update_int(sc);
    javasp_do_procedure_op_post();
    unlock_schema_lk();
    return rc;
}

// TODO -- what should this do? maybe log_scdone should be here
int finalize_trigger(struct schema_change_type *s)
{
    return 0;
}
