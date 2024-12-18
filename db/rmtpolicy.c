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

/* This module defines our remote policies i.e. who we accept writes
 * from and who we will cluster with.
 *
 * The policy is not to allow writes from a lower class of machine.
 *
 * $Id: rmtpolicy.c 92214 2014-04-17 17:08:33Z dhogea $
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <plbitlib.h>
#include <segstr.h>

#include "comdb2.h"
#include "intern_strings.h"
#include <clienthost.h>
#include "util.h"
#include "rtcpu.h"
#include "logmsg.h"

struct rmtpol {
    const char *descr;
    int explicit_allow_classes;
    int explicit_disallow_classes;
    int class_promotion; /* if 1, allowing class x implies that class x+1 also
                            allowed - used to implement "allow write from test"
                            implies "allow write from alpha". */
};

enum mach_class mach_classes[MAX_CPU] = {CLASS_UNKNOWN};

static struct rmtpol write_pol = {"write from", 0, 0, 1};
static struct rmtpol brd_pol = {"broadcast to", 0, 0, 0};
static struct rmtpol cluster_pol = {"cluster with", 0, 0, 0};

enum mach_class get_my_mach_class(void)
{
    if (gbl_machine_class)
        return mach_class_name2class(gbl_machine_class);
    return machine_my_class();
}

const char *get_my_mach_cluster(void)
{
    const char *mycluster;
    return (machine_my_cluster(&mycluster) == 0) ? mycluster : NULL;
}

const char *get_mach_cluster(const char *host)
{
    const char *cluster;
    return (machine_cluster(host, &cluster) == 0) ? cluster : NULL;
}

int get_cluster_machs(const char *cluster, int *count, const char ***machs)
{
    return machine_cluster_machs(cluster, count, machs);
}

const char *get_my_mach_class_str(void)
{
    return mach_class_class2name(get_my_mach_class());
}

enum mach_class get_mach_class(const char *host) { return machine_class(host); }

const char *get_mach_class_str(char *host)
{
    return mach_class_class2name(get_mach_class(host));
}

static int allow_action_from_remote(const char *host, const struct rmtpol *pol, struct hostpolicy *hpol)
{
    enum mach_class rmtclass;

    if (hpol->explicit_disallow)
        return 0;

    if (hpol->explicit_allow)
        return 1;

    rmtclass = get_mach_class(host);

    if (btst(&pol->explicit_disallow_classes, rmtclass))
        return 0;

    if (btst(&pol->explicit_allow_classes, rmtclass))
        return 1;

    if (pol->class_promotion && rmtclass > CLASS_UNKNOWN) {
        while ((--rmtclass) > 0) {
            if (btst(&pol->explicit_allow_classes, rmtclass))
                return 1;
        }
    }

    /* -1 => not sure */
    return -1;
}

int allow_write_from_remote(const char *host)
{
    int rc;
    struct interned_string *host_interned = intern_ptr(host);
    struct clienthost *c = retrieve_clienthost(host_interned);
    rc = allow_action_from_remote(host, &write_pol, &c->write_pol);
    if (rc == -1) {
        /* default logic: allow writes from same or higher classes. */
        if (get_mach_class(host) >= get_mach_class(gbl_myhostname))
            rc = 1;
        else
            rc = 0;
    }
    return rc;
}

int allow_cluster_from_remote(const char *host)
{
    int rc;
    struct interned_string *host_interned = intern_ptr(host);
    struct clienthost *c = retrieve_clienthost(host_interned);
    rc = allow_action_from_remote(host, &cluster_pol, &c->cluster_pol);
    if (rc == -1) {
        /* default logic: only cluster with like machines i.e. alpha with alpha,
         * beta with beta etc. */
        if (get_mach_class(host) == get_mach_class(gbl_myhostname))
            rc = 1;
        else
            rc = 0;
    }
    return rc;
}

int allow_broadcast_to_remote(const char *host)
{
    struct interned_string *host_interned = intern_ptr(host);
    struct clienthost *c = retrieve_clienthost(host_interned);
    int rc = allow_action_from_remote(host, &brd_pol, &c->brd_pol);
    if (rc == -1) {
        /* default logic: only broadcast to machines of the same or a lower
         * class.  we don't want alpha to broadcast to prod! */
        if (get_mach_class(host) <= get_mach_class(gbl_myhostname))
            rc = 1;
        else
            rc = 0;
    }
    return rc;
}

static int parse_mach_or_group(char *tok, int ltok, char **mach,
                               enum mach_class *cls)
{
    char *name = strndup(tok, ltok);
    *mach = NULL;

    *cls = mach_class_name2class(name);
    if (!*cls) {
        char *m;
        m = tokdup(tok, ltok);
        *mach = intern(m);
        free(m);
    }
    return 0;
}

/* Recognised commands:
 *
 * allow/disallow write/cluster/broadcast with/from/to
 *test/dev/alpha/beta/prod/# [on test/alpha/beta/prod]
 *
 * setclass # test/dev/alpha/beta/prod/reset
 */

struct hostpolicy *global_policy_to_host_policy(struct clienthost *c, const struct rmtpol *pol)
{
    if (pol == &write_pol) {
        return &c->write_pol;
    }
    if (pol == &cluster_pol) {
        return &c->cluster_pol;
    }
    if (pol == &brd_pol) {
        return &c->brd_pol;
    }
    return NULL;
}

int process_allow_command(char *line, int lline)
{
    char *tok;
    int ltok, st;
    int allow = 1;
    struct rmtpol *pol = NULL;
    struct hostpolicy *hostpol = NULL;
    enum mach_class cls = CLASS_UNKNOWN;
    struct clienthost *r = NULL;
    int ii, mark;
    char *new_mach = NULL;

    /* scan for end of line */
    for (mark = 0, ii = 0; ii < lline; ii++)
        if (line[ii] > ' ')
            mark = ii;
    lline = mark + 1;

    st = 0;

    tok = segtok(line, lline, &st, &ltok);
    if (ltok == 0)
        goto bad;
    if (tokcmp(tok, ltok, "allow") == 0)
        allow = 1;
    else if (tokcmp(tok, ltok, "disallow") == 0)
        allow = 0;
    else if (tokcmp(tok, ltok, "clrpol") == 0)
        allow = -1;
    else if (tokcmp(tok, ltok, "setclass") == 0) {
        enum mach_class new_cls = CLASS_UNKNOWN;
        /* set class of some machine */
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0)
            goto bad;
        if (parse_mach_or_group(tok, ltok, &new_mach, &cls) != 0)
            goto bad;

        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0)
            goto bad;
        if (tokcmp(tok, ltok, "reset") != 0 &&
            parse_mach_or_group(tok, ltok, &new_mach, &new_cls) != 0)
            goto bad;
        struct interned_string *mach_interned = intern_ptr(new_mach);
        struct clienthost *r = retrieve_clienthost(mach_interned);
        r->mach_class = new_cls;
        logmsg(LOGMSG_DEBUG, "processed <%*.*s>\n", lline, lline, line);
        return 0;
    } else
        goto bad;

    tok = segtok(line, lline, &st, &ltok);
    if (ltok == 0)
        goto bad;
    if (tokcmp(tok, ltok, "write") == 0)
        pol = &write_pol;
    else if (tokcmp(tok, ltok, "writes") == 0)
        pol = &write_pol;
    else if (tokcmp(tok, ltok, "cluster") == 0)
        pol = &cluster_pol;
    else if (tokcmp(tok, ltok, "broadcast") == 0)
        pol = &brd_pol;
    else
        goto bad;

    /* skip "from"/"with" */
    tok = segtok(line, lline, &st, &ltok);
    if (ltok == 0)
        goto bad;

    tok = segtok(line, lline, &st, &ltok);
    if (ltok == 0)
        goto bad;
    if (parse_mach_or_group(tok, ltok, &new_mach, &cls) != 0)
        goto bad;
    if (new_mach) {
        struct interned_string *new_mach_interned = intern_ptr(new_mach);
        r = retrieve_clienthost(new_mach_interned);
        hostpol = global_policy_to_host_policy(r, pol);
    }

    /* optional bit to only apply the update on a particular machine class */
    tok = segtok(line, lline, &st, &ltok);
    if (ltok != 0) {
        char *if_mach = 0;
        enum mach_class if_cls = CLASS_UNKNOWN;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0)
            goto bad;
        if (parse_mach_or_group(tok, ltok, &if_mach, &if_cls) != 0)
            goto bad;

        if (if_mach > 0 && if_mach != gbl_myhostname)
            goto ignore;
        if (if_cls != CLASS_UNKNOWN && if_cls != get_my_mach_class())
            goto ignore;
    }

    if (r != NULL) {
        if (allow == 1) {
            hostpol->explicit_allow = 1;
            hostpol->explicit_disallow = 0;
            logmsg(LOGMSG_USER, "allowing %s machine %s\n", pol->descr, new_mach);
        } else if (allow == 0) {
            hostpol->explicit_disallow = 1;
            hostpol->explicit_allow = 0;
            logmsg(LOGMSG_USER, "disallowing %s machine %s\n", pol->descr, new_mach);
        } else if (allow == -1) {
            hostpol->explicit_disallow = 0;
            hostpol->explicit_allow = 0;
            logmsg(LOGMSG_USER, "resetting policy for %s machine %s\n", pol->descr, new_mach);
        }
    } else if (cls != CLASS_UNKNOWN) {
        if (allow == 1) {
            bset(&pol->explicit_allow_classes, cls);
            bclr(&pol->explicit_disallow_classes, cls);
            logmsg(LOGMSG_USER, "allowing %s %s machines\n", pol->descr,
                   mach_class_class2name(cls));
        } else if (allow == 0) {
            bset(&pol->explicit_disallow_classes, cls);
            bclr(&pol->explicit_allow_classes, cls);
            logmsg(LOGMSG_USER, "disallowing %s %s machines\n", pol->descr,
                   mach_class_class2name(cls));
        } else if (allow == -1) {
            bclr(&pol->explicit_disallow_classes, cls);
            bclr(&pol->explicit_allow_classes, cls);
            logmsg(LOGMSG_USER, "resetting policy for %s %s machines\n",
                   pol->descr, mach_class_class2name(cls));
        }
    } else {
        goto bad;
    }

    logmsg(LOGMSG_DEBUG, "processed <%*.*s>\n", lline, lline, line);
    return 0;

ignore:
    logmsg(LOGMSG_WARN, "ignoring command <%*.*s>\n", lline, lline, line);
    return 0;
bad:
    logmsg(LOGMSG_ERROR, "bad command <%*.*s>\n", lline, lline, line);
    return -1;
}

extern LISTC_T(struct clienthost) clienthost_list;

void dump_policy_structure(const struct rmtpol *pol)
{
    logmsg(LOGMSG_USER, "Policy '%s'\n", pol->descr);
    struct clienthost *h;

    clienthost_lock();
    LISTC_FOR_EACH(&clienthost_list, h, lnk)
    {
        struct hostpolicy *p = global_policy_to_host_policy(h, pol);
        if (p->explicit_disallow) {
            logmsg(LOGMSG_USER, "  explicit_disallow mach %s\n", h->s->str);
        }
    }
    LISTC_FOR_EACH(&clienthost_list, h, lnk)
    {
        struct hostpolicy *p = global_policy_to_host_policy(h, pol);
        if (p->explicit_allow) {
            logmsg(LOGMSG_USER, "  explicit_allow mach %s\n", h->s->str);
        }
    }
    clienthost_unlock();

    int c = 0;
    while (++c <= 6) { // start from dev
        if (btst(&pol->explicit_disallow_classes, c))
            logmsg(LOGMSG_USER, "  explicit_disallow class %s\n",
                   mach_class_class2name(c));
        if (btst(&pol->explicit_allow_classes, c))
            logmsg(LOGMSG_USER, "  explicit_allow class %s\n",
                   mach_class_class2name(c));
    }
}

void dump_remote_policy()
{
    dump_policy_structure(&write_pol);
    dump_policy_structure(&brd_pol);
    dump_policy_structure(&cluster_pol);
}
