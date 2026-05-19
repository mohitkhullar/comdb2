function urldecode(s)
    return s:gsub("%%(%x%x)", function(h)
        return string.char(tonumber(h, 16))
    end)
end

function html_escape(s)
    if s == nil then return "" end
    s = tostring(s)
    s = s:gsub("&", "&amp;")
    s = s:gsub("<", "&lt;")
    s = s:gsub(">", "&gt;")
    s = s:gsub('"', "&quot;")
    return s
end

function page_header(title)
    return '<!DOCTYPE html><html><head><meta charset="utf-8">'
        .. '<title>' .. html_escape(title) .. '</title>'
        .. '<style>'
        .. 'body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;'
        .. 'margin:0;padding:20px 40px;background:#f8f9fa;color:#333}'
        .. 'h1{color:#1a73e8;border-bottom:2px solid #1a73e8;padding-bottom:8px}'
        .. 'a{color:#1a73e8;text-decoration:none}'
        .. 'a:hover{text-decoration:underline}'
        .. 'ul{list-style:none;padding:0}'
        .. 'ul li{padding:6px 12px;border-bottom:1px solid #e0e0e0;background:#fff}'
        .. 'ul li:first-child{border-radius:6px 6px 0 0}'
        .. 'ul li:last-child{border-radius:0 0 6px 6px;border-bottom:none}'
        .. 'table{border-collapse:collapse;width:100%;background:#fff;border-radius:6px;'
        .. 'overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.1)}'
        .. 'th{background:#1a73e8;color:#fff;padding:10px 12px;text-align:left;font-weight:500;cursor:pointer}'
        .. 'th a{color:#fff;text-decoration:none;display:block}'
        .. 'th a:hover{text-decoration:underline}'
        .. 'td{padding:8px 12px;border-bottom:1px solid #e8e8e8;max-width:400px;'
        .. 'overflow:hidden;text-overflow:ellipsis;white-space:nowrap}'
        .. 'tr:hover td{background:#f1f7fe}'
        .. '.nav{margin-bottom:16px;font-size:14px}'
        .. '.count{color:#666;font-size:14px;margin-bottom:12px}'
        .. '</style></head><body>'
end

function page_footer()
    return '</body></html>'
end

function parse_query_string(qs)
    local params = {}
    if qs == nil or qs == "" then return params end
    for kv in qs:gmatch("[^&]+") do
        local k, v = kv:match("^([^=]+)=?(.*)")
        if k then params[urldecode(k)] = urldecode(v) end
    end
    return params
end

function handle(request)
    local path = request.path or "/"
    local params = parse_query_string(request.query_string)

    if path == "/" or path == "" then
        return homepage()
    end

    local tbl = path:match("^/table/(.+)$")
    if tbl then
        return show_table(urldecode(tbl), params)
    end

    return {
        status = 404,
        content_type = "text/html",
        body = page_header("404 Not Found")
            .. '<h1>404 Not Found</h1><p>The requested page was not found.</p>'
            .. '<p><a href="/">Back to home</a></p>'
            .. page_footer()
    }
end

function homepage()
    local rows = db_query("SELECT name FROM comdb2_systables ORDER BY name")
    local dbname = db_name()
    local html = page_header(dbname .. " - System Tables")
        .. '<h1>' .. html_escape(dbname) .. '</h1>'
        .. '<div class="count">' .. #rows .. ' system tables</div><ul>'

    for _, row in ipairs(rows) do
        local name = row[1]
        html = html .. '<li><a href="/table/' .. html_escape(name) .. '">'
            .. html_escape(name) .. '</a></li>'
    end

    html = html .. '</ul>' .. page_footer()
    return {status = 200, content_type = "text/html", body = html}
end

function show_table(name, params)
    if not name:match("^[%w_]+$") then
        return {
            status = 400,
            content_type = "text/html",
            body = page_header("Bad Request")
                .. '<h1>Bad Request</h1><p>Invalid table name.</p>'
                .. '<p><a href="/">Back to home</a></p>'
                .. page_footer()
        }
    end

    local sort_col = params and params["sort"] or nil
    local sort_dir = params and params["dir"] or "asc"
    if sort_dir ~= "asc" and sort_dir ~= "desc" then sort_dir = "asc" end
    -- Validate sort column name to prevent injection
    if sort_col and not sort_col:match("^[%w_]+$") then sort_col = nil end

    local sql = "SELECT * FROM \"" .. name .. "\""
    if sort_col then
        sql = sql .. " ORDER BY \"" .. sort_col .. "\" " .. sort_dir
    end
    sql = sql .. " LIMIT 1000"

    local ok, rows, columns = pcall(function()
        return db_query(sql)
    end)

    if not ok then
        return {
            status = 500,
            content_type = "text/html",
            body = page_header("Error")
                .. '<h1>Error</h1><p>Failed to query table: '
                .. html_escape(tostring(rows)) .. '</p>'
                .. '<p><a href="/">Back to home</a></p>'
                .. page_footer()
        }
    end

    local cols = columns or {}
    local html = page_header(name .. " - " .. db_name())
        .. '<div class="nav"><a href="/">Home</a></div>'
        .. '<h1>' .. html_escape(name) .. '</h1>'
        .. '<div class="count">' .. #rows .. ' rows (limit 1000)</div>'

    if #rows == 0 then
        html = html .. '<p>Table is empty or returned no rows.</p>'
    else
        html = html .. '<table><thead><tr>'
        for _, col in ipairs(cols) do
            local next_dir = "asc"
            local arrow = ""
            if sort_col == col then
                if sort_dir == "asc" then
                    next_dir = "desc"
                    arrow = " &#9650;"
                else
                    next_dir = "asc"
                    arrow = " &#9660;"
                end
            end
            html = html .. '<th><a href="/table/' .. html_escape(name)
                .. '?sort=' .. html_escape(col)
                .. '&dir=' .. next_dir
                .. '">' .. html_escape(col) .. arrow .. '</a></th>'
        end
        html = html .. '</tr></thead><tbody>'

        for _, row in ipairs(rows) do
            html = html .. '<tr>'
            for _, val in ipairs(row) do
                html = html .. '<td>' .. html_escape(val) .. '</td>'
            end
            html = html .. '</tr>'
        end

        html = html .. '</tbody></table>'
    end

    html = html .. page_footer()
    return {status = 200, content_type = "text/html", body = html}
end
