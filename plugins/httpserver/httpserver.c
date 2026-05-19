/*
   Copyright 2026 Bloomberg Finance L.P.

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

#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "comdb2.h"
#include "comdb2_appsock.h"
#include "comdb2_plugin.h"
#include "comdb2buf.h"
#include "logmsg.h"
#include "cdb2api.h"

#include "lua.h"
#include "lauxlib.h"
#include "lualib.h"

#include "httpserver.h"
#include "default_handler_lua.h"

extern char gbl_dbname[];
extern struct dbenv *thedb;

/* Configuration: path to optional Lua script override */
static char *httpserver_lua_script_path = NULL;

/*
 * HTTP request parsed from the appsock connection.
 */
struct http_request {
    char method[16];
    char path[1024];
    char query_string[1024];
    char body[8192];
    int body_len;
    int content_length;
};

/*
 * Parse the HTTP request line and headers from the appsock connection.
 * The first line has already been read into arg->cmdline by thd_appsock_int.
 */
static int parse_http_request(comdb2_appsock_arg_t *arg, struct http_request *req)
{
    memset(req, 0, sizeof(*req));

    /* Parse the request line from cmdline: "GET /path?query HTTP/1.1\r\n" */
    char *line = arg->cmdline;
    char *sp1 = strchr(line, ' ');
    if (!sp1)
        return -1;

    int method_len = sp1 - line;
    if (method_len >= (int)sizeof(req->method))
        method_len = sizeof(req->method) - 1;
    memcpy(req->method, line, method_len);
    req->method[method_len] = '\0';

    char *uri_start = sp1 + 1;
    char *sp2 = strchr(uri_start, ' ');
    if (!sp2)
        sp2 = uri_start + strlen(uri_start);

    /* Split URI into path and query string */
    char *qmark = memchr(uri_start, '?', sp2 - uri_start);
    if (qmark) {
        int path_len = qmark - uri_start;
        if (path_len >= (int)sizeof(req->path))
            path_len = sizeof(req->path) - 1;
        memcpy(req->path, uri_start, path_len);
        req->path[path_len] = '\0';

        int qs_len = sp2 - (qmark + 1);
        if (qs_len >= (int)sizeof(req->query_string))
            qs_len = sizeof(req->query_string) - 1;
        memcpy(req->query_string, qmark + 1, qs_len);
        req->query_string[qs_len] = '\0';
    } else {
        int path_len = sp2 - uri_start;
        if (path_len >= (int)sizeof(req->path))
            path_len = sizeof(req->path) - 1;
        memcpy(req->path, uri_start, path_len);
        req->path[path_len] = '\0';
    }

    /* Strip trailing \r\n from path if present */
    int plen = strlen(req->path);
    while (plen > 0 && (req->path[plen - 1] == '\r' || req->path[plen - 1] == '\n')) {
        req->path[--plen] = '\0';
    }

    /* Read headers until empty line */
    char hdr[2048];
    req->content_length = 0;
    while (1) {
        int rc = cdb2buf_gets(hdr, sizeof(hdr), arg->sb);
        if (rc <= 0)
            break;
        /* Strip trailing \r\n */
        while (rc > 0 && (hdr[rc - 1] == '\r' || hdr[rc - 1] == '\n')) {
            hdr[--rc] = '\0';
        }
        /* Empty line signals end of headers */
        if (rc == 0)
            break;
        /* Parse Content-Length */
        if (strncasecmp(hdr, "Content-Length:", 15) == 0) {
            req->content_length = atoi(hdr + 15);
        }
    }

    /* Read body if Content-Length > 0 */
    if (req->content_length > 0) {
        int to_read = req->content_length;
        if (to_read >= (int)sizeof(req->body))
            to_read = sizeof(req->body) - 1;
        int total = 0;
        while (total < to_read) {
            int rc = cdb2buf_gets(req->body + total, to_read - total + 1, arg->sb);
            if (rc <= 0)
                break;
            total += rc;
        }
        req->body[total] = '\0';
        req->body_len = total;
    }

    return 0;
}

/*
 * Send an HTTP response over the appsock connection.
 */
static void send_http_response(comdb2_appsock_arg_t *arg, int status, const char *content_type, const char *body,
                               int body_len)
{
    const char *status_text = "OK";
    if (status == 400)
        status_text = "Bad Request";
    else if (status == 404)
        status_text = "Not Found";
    else if (status == 500)
        status_text = "Internal Server Error";

    cdb2buf_printf(arg->sb, "HTTP/1.1 %d %s\r\n", status, status_text);
    cdb2buf_printf(arg->sb, "Content-Type: %s\r\n", content_type ? content_type : "text/html");
    cdb2buf_printf(arg->sb, "Content-Length: %d\r\n", body_len);
    cdb2buf_printf(arg->sb, "Connection: close\r\n");
    cdb2buf_printf(arg->sb, "\r\n");
    if (body && body_len > 0) {
        cdb2buf_write((char *)body, body_len, arg->sb);
    }
    cdb2buf_flush(arg->sb);
}

/* ---- Lua bindings ---- */

/*
 * Lua function: db_query(sql)
 * Executes SQL via cdb2api, returns {rows, columns}.
 * rows = array of arrays (each row is an array of string values)
 * columns = array of column names
 */
static int lua_db_query(lua_State *L)
{
    const char *sql = luaL_checkstring(L, 1);

    cdb2_hndl_tp *hndl = NULL;
    int rc = cdb2_open(&hndl, gbl_dbname, "local", 0);
    if (rc != 0) {
        return luaL_error(L, "cdb2_open failed: %s", cdb2_errstr(hndl));
    }

    rc = cdb2_run_statement(hndl, sql);
    if (rc != 0) {
        const char *err = cdb2_errstr(hndl);
        cdb2_close(hndl);
        return luaL_error(L, "query failed: %s", err);
    }

    /* Result table: rows */
    lua_newtable(L);
    int row_idx = 0;

    /* Column names table */
    lua_newtable(L);
    int cols_table_idx = lua_gettop(L);
    int cols_populated = 0;

    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
        int ncols = cdb2_numcolumns(hndl);

        /* Populate column names on first row */
        if (!cols_populated) {
            for (int i = 0; i < ncols; i++) {
                lua_pushstring(L, cdb2_column_name(hndl, i));
                lua_rawseti(L, cols_table_idx, i + 1);
            }
            cols_populated = 1;
        }

        /* Create row table */
        lua_newtable(L);
        for (int i = 0; i < ncols; i++) {
            void *val = cdb2_column_value(hndl, i);
            if (val == NULL) {
                lua_pushnil(L);
            } else {
                int ctype = cdb2_column_type(hndl, i);
                switch (ctype) {
                case CDB2_INTEGER:
                    lua_pushinteger(L, *(long long *)val);
                    break;
                case CDB2_REAL:
                    lua_pushnumber(L, *(double *)val);
                    break;
                case CDB2_CSTRING:
                    lua_pushstring(L, (const char *)val);
                    break;
                default: {
                    /* For blobs and other types, represent as string */
                    int sz = cdb2_column_size(hndl, i);
                    lua_pushlstring(L, (const char *)val, sz);
                    break;
                }
                }
            }
            lua_rawseti(L, -2, i + 1);
        }

        row_idx++;
        /* rows[row_idx] = row */
        lua_rawseti(L, -3, row_idx);
    }

    cdb2_close(hndl);

    /* Return rows table and columns table */
    /* Stack: rows_table, cols_table */
    /* We need to return rows first, columns second */
    /* Swap: push rows below cols */
    return 2; /* rows, columns */
}

/*
 * Lua function: db_name()
 * Returns the database name.
 */
static int lua_db_name(lua_State *L)
{
    lua_pushstring(L, gbl_dbname);
    return 1;
}

/*
 * Load Lua script source: from override file or built-in default.
 */
static const char *load_lua_script(int *len)
{
    if (httpserver_lua_script_path) {
        FILE *f = fopen(httpserver_lua_script_path, "r");
        if (f) {
            fseek(f, 0, SEEK_END);
            long sz = ftell(f);
            fseek(f, 0, SEEK_SET);
            char *buf = malloc(sz + 1);
            if (buf) {
                fread(buf, 1, sz, f);
                buf[sz] = '\0';
                *len = sz;
                fclose(f);
                return buf;
            }
            fclose(f);
        }
        logmsg(LOGMSG_WARN, "httpserver: could not load script '%s', using default\n", httpserver_lua_script_path);
    }

    *len = default_handler_lua_len;
    return (const char *)default_handler_lua;
}

/*
 * Execute the Lua handler script with the given HTTP request.
 * Returns the HTTP response fields via out parameters.
 */
static int execute_lua_handler(struct http_request *req, int *out_status, const char **out_content_type,
                               char **out_body, int *out_body_len)
{
    lua_State *L = luaL_newstate();
    if (!L)
        return -1;
    luaL_openlibs(L);

    /* Register C functions */
    lua_pushcfunction(L, lua_db_query);
    lua_setglobal(L, "db_query");

    lua_pushcfunction(L, lua_db_name);
    lua_setglobal(L, "db_name");

    /* Set request global table */
    lua_newtable(L);
    lua_pushstring(L, req->method);
    lua_setfield(L, -2, "method");
    lua_pushstring(L, req->path);
    lua_setfield(L, -2, "path");
    lua_pushstring(L, req->query_string);
    lua_setfield(L, -2, "query_string");
    lua_pushstring(L, req->body);
    lua_setfield(L, -2, "body");
    lua_setglobal(L, "request");

    /* Load and execute script */
    int script_len = 0;
    const char *script = load_lua_script(&script_len);
    int is_allocated = (script != (const char *)default_handler_lua);

    int rc = luaL_loadbuffer(L, script, script_len, "httphandler");
    if (is_allocated)
        free((void *)script);

    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "httpserver: lua load error: %s\n", lua_tostring(L, -1));
        lua_close(L);
        return -1;
    }

    rc = lua_pcall(L, 0, 0, 0);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "httpserver: lua exec error: %s\n", lua_tostring(L, -1));
        lua_close(L);
        return -1;
    }

    /* Call handle(request) */
    lua_getglobal(L, "handle");
    if (!lua_isfunction(L, -1)) {
        logmsg(LOGMSG_ERROR, "httpserver: no 'handle' function in lua script\n");
        lua_close(L);
        return -1;
    }

    lua_getglobal(L, "request");
    rc = lua_pcall(L, 1, 1, 0);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "httpserver: lua handle() error: %s\n", lua_tostring(L, -1));
        lua_close(L);
        return -1;
    }

    /* Read response table: {status, content_type, body} */
    if (!lua_istable(L, -1)) {
        logmsg(LOGMSG_ERROR, "httpserver: handle() did not return a table\n");
        lua_close(L);
        return -1;
    }

    lua_getfield(L, -1, "status");
    *out_status = lua_tointeger(L, -1);
    if (*out_status == 0)
        *out_status = 200;
    lua_pop(L, 1);

    lua_getfield(L, -1, "content_type");
    const char *ct = lua_tostring(L, -1);
    *out_content_type = ct ? strdup(ct) : strdup("text/html");
    lua_pop(L, 1);

    lua_getfield(L, -1, "body");
    size_t blen = 0;
    const char *b = lua_tolstring(L, -1, &blen);
    *out_body = b ? strndup(b, blen) : strdup("");
    *out_body_len = (int)blen;
    lua_pop(L, 1);

    lua_close(L);
    return 0;
}

/*
 * Common HTTP request handler for GET and POST.
 */
static int handle_http_request(comdb2_appsock_arg_t *arg)
{
    struct http_request req;
    if (parse_http_request(arg, &req) != 0) {
        send_http_response(arg, 400, "text/html", "<h1>400 Bad Request</h1>", 24);
        return APPSOCK_RETURN_OK;
    }

    int status = 500;
    const char *content_type = NULL;
    char *body = NULL;
    int body_len = 0;

    if (execute_lua_handler(&req, &status, &content_type, &body, &body_len) != 0) {
        send_http_response(arg, 500, "text/html", "<h1>500 Internal Server Error</h1>", 34);
        return APPSOCK_RETURN_OK;
    }

    send_http_response(arg, status, content_type, body, body_len);

    free((void *)content_type);
    free(body);

    return APPSOCK_RETURN_OK;
}

int handle_GET_request(comdb2_appsock_arg_t *arg)
{
    return handle_http_request(arg);
}

int handle_POST_request(comdb2_appsock_arg_t *arg)
{
    return handle_http_request(arg);
}

/* Plugin appsock descriptors */
comdb2_appsock_t httpserver_get_plugin = {
    "GET",              /* Name — matches first token of "GET /path HTTP/1.1" */
    "HTTP GET handler", /* Usage info */
    0,                  /* Execution count */
    0,                  /* Flags */
    handle_GET_request  /* Handler function */
};

comdb2_appsock_t httpserver_post_plugin = {
    "POST",              /* Name — matches first token of "POST /path HTTP/1.1" */
    "HTTP POST handler", /* Usage info */
    0,                   /* Execution count */
    0,                   /* Flags */
    handle_POST_request  /* Handler function */
};

static int httpserver_init(void *unused)
{
    logmsg(LOGMSG_USER, "httpserver: HTTP handler registered (GET, POST)\n");
    return 0;
}

static int httpserver_destroy(void)
{
    free(httpserver_lua_script_path);
    httpserver_lua_script_path = NULL;
    return 0;
}

/* LRL handler for httpserver configuration */
static int httpserver_lrl_handler(struct dbenv *dbenv, const char *line)
{
    if (strncasecmp(line, "httpserver_lua_script", 20) == 0) {
        const char *path = line + 20;
        while (*path && isspace((unsigned char)*path))
            path++;
        if (*path) {
            free(httpserver_lua_script_path);
            httpserver_lua_script_path = strdup(path);
            /* Strip trailing whitespace/newline */
            int len = strlen(httpserver_lua_script_path);
            while (len > 0 && isspace((unsigned char)httpserver_lua_script_path[len - 1])) {
                httpserver_lua_script_path[--len] = '\0';
            }
            logmsg(LOGMSG_INFO, "httpserver: lua script override set to '%s'\n", httpserver_lua_script_path);
        }
        return 0;
    }
    return 1; /* not handled */
}

#include "plugin.h"
