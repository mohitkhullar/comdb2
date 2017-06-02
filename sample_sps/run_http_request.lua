local function main(arg)
    db:setmaxinstructions(100000)
    print(arg)
    local headers = {}
    for word in arg:gmatch("%S+") do table.insert(headers, word) end
    local name = string.sub(headers[1],2)
    print(name)
    if (name == "") then
      print("LOOP 1")  
      local resultset = db:exec("select name from sqlite_master where type='table'")
      local result = resultset:fetch()
      db:write("<!doctype html><html><body>")
      db:write("<br><form action=''> Change DB: <input type='text' name='d' value='mohitdb1'><br><input type='submit' value='Submit'> </form>")
      db:write("<table border='1'>")
      while result do
          local name = tostring(result.name);
          local table_entry = "<tr> <td>  <a href='" .. name .. "'> " .. name .. "</a></td></tr>"
          db:write(table_entry)
          result = resultset:fetch()
      end     
      db:write("</table>")
      db:write("<br><form action=''> Run Query: <input type='text' name='q' value='select 1'><br><input type='submit' value='Submit'> </form>")
      db:write("</body></html>")
   elseif (string.sub(name,1,1) == "@" and string.sub(name,2) ~= "") then
      print("LOOP 2")  
      local sql = "select * from " .. string.sub(name,2) .. " limit 100"
      print(sql)
      local resultset = db:exec(sql)
      local result = resultset:fetch()
      db:write("<html><body><br>")
      db:write("<a href=\"/\"> HOME </a> <br>")
      db:write("<table border='1'>")
      local table_entry = ""
      while result do
          table_entry = table_entry .. "<tr>"
          for val, row1 in pairs(result) do
            table_entry = table_entry .. "<td>" .. val .. " = ".. tostring(row1) .. "</td>"
          end 
          table_entry = table_entry .. "</tr>"
          result = resultset:fetch()
      end     
      db:write(table_entry)
      db:write("</table></body></html>")
   elseif (string.sub(name,1,3) == "?d=" and string.sub(name,4) ~= "") then
      print("LOOP 3")  
      local has_q = string.find(name, "&q=", 1, true)
      local has_dot = string.find(name, ".", 1, true)
      if (has_q == nil or has_dot) then
        local database = string.sub(name,4)
        if(has_dot)  then
          database = string.sub(name,4, has_q-1)
        end    
        local sql = "select name from " .. database .. ".sqlite_master where type='table'"
        local resultset = db:exec(sql)
        local result = resultset:fetch()
        db:write("<!doctype html><html><body>")
        db:write("<a href=\"/\"> HOME </a> <br>")
        db:write("<table border='1'>")
        while result do
            local name = tostring(result.name);
            local table_entry = "<tr> <td>  <a href='" .. database .. "." .. name .. "'> " .. name .. "</a></td></tr>"
            db:write(table_entry)
            result = resultset:fetch()
        end     
        db:write("</table>")
        local textbox = "<br><form action=''> Run Simple Query: DB<input type='text' name='d' value=" .. database .. "> Query:<input type='text' name='q' value='select 1'><br><input type='submit' value='Submit'> </form>"
        db:write(textbox)
        db:write("</body></html>")
       else 
        local database = string.sub(name,4,has_q-1)
        local sql = string.sub(name,has_q+3)
        sql = string.lower(sql)
        sql = string.gsub(sql, "%%20", " ")
        sql = string.gsub(sql, "+", " ")
        sql = string.gsub(sql, "%%2B", "+")
        sql = string.gsub(sql, "%%28", "(")
        sql = string.gsub(sql, "%%29", ")")        
        print(sql)
        local final_sql = sql
        local ending_sql = ""
        local words_str = ""
        local from = string.find(sql,"from")
        if (from) then
          final_sql = string.sub(sql, 1, from+4)
          local where = string.find(sql,"where")
          if (where == nil) then
             where = string.find(sql,"limit")
          end
          words_str = ""
          if (where) then
            words_str = string.sub(sql, from+4, where-1)
          else  
            words_str = string.sub(sql, from+4)
          end    
          local words = {}
          final_sql = final_sql ..  "    "
          for word in words_str:gmatch('([^,]+)') do 
              word = word:gsub(" ", "")
              final_sql = final_sql .. database .. "." .. word .. " , "
          end    
          final_sql = string.sub(final_sql, 1, -3)

          print(final_sql)

          if (where ~= nil) then
              final_sql = final_sql .. string.sub(sql, where)
          end    
        end      
        print(final_sql)
        local resultset = db:exec(final_sql)
        local result = resultset:fetch()
        db:write("<html><body><br>")
        db:write("<a href=\"/\"> HOME </a> <br>")
        db:write("<table border='1'>")
        local table_entry = ""
        while result do
            table_entry = table_entry .. "<tr>"
            for val, row in pairs(result) do
              table_entry = table_entry .. "<td>" .. val .. " = ".. row .. "</td>"
            end 
            table_entry = table_entry .. "</tr>"
            result = resultset:fetch()
        end     
        db:write(table_entry)
        db:write("</table></body></html>")   
      end  
   elseif (string.sub(name,1,3) == "?q=" and string.sub(name,4) ~= "") then
      print("LOOP 4")  
      local sql1 = string.sub(name,4)
      print(sql1)
      local sql = string.gsub(sql1, "%%20", " ")
      local sql = string.gsub(sql, "+", " ")
      local sql = string.gsub(sql, "%%2B", "+")
      local sql = string.gsub(sql, "%%28", "(")
      local sql = string.gsub(sql, "%%29", ")")
      print(sql)
      local resultset = db:exec(sql)
      local result = resultset:fetch()
      db:write("<html><body><br>")
      db:write("<a href=\"/\"> HOME </a> <br>")
      db:write("<table border='1'>")
      local table_entry = ""
      while result do
          table_entry = table_entry .. "<tr>"
          for val, row in pairs(result) do
            table_entry = table_entry .. "<td>" .. val .. " = ".. row .. "</td>"
          end 
          table_entry = table_entry .. "</tr>"
          result = resultset:fetch()
      end     
      db:write(table_entry)
      db:write("</table></body></html>")   
   elseif (string.sub(name,1,1) == "?" and string.sub(name,2) ~= "") then
      print("LOOP 5")  
      local sql1 = string.sub(name,2)
      print(sql1)
      local sql = string.gsub(sql1, "%%20", " ")
      local sql = string.gsub(sql, "+", " ")
      local sql = string.gsub(sql, "%%2B", "+")
      local sql = string.gsub(sql, "%%28", "(")
      local sql = string.gsub(sql, "%%29", ")")
      print(sql)
      local resultset = db:exec(sql)
      local result = resultset:fetch()
      db:write("<html><body><br>")
      db:write("<a href=\"/\"> HOME </a> <br>")
      db:write("<table border='1'>")
      local table_entry = ""
      while result do
          table_entry = table_entry .. "<tr>"
          for val, row in pairs(result) do
            table_entry = table_entry .. "<td>" .. val .. " = ".. row .. "</td>"
          end 
          table_entry = table_entry .. "</tr>"
          result = resultset:fetch()
      end     
      db:write(table_entry)
      db:write("</table></body></html>")         
   elseif (name == "favicon.ico") then
      print("LOOP 6") 
      local sql = "select icon from ico"
      local resultset = db:exec(sql)
      local result = resultset:fetch()
      db.write(result.icon)
   else    
      print("LOOP 7") 
      local dbname = ""
      local tablename = name
      local dot = string.find(name, ".", 1, true)
      local sql = ""
      if (dot ~= nil) then
        dbname = string.sub(name, 1, dot-1)
        tablename = string.sub(name, dot+1)
        sql = "select csc2 from " .. dbname .. ".sqlite_master where type='table' and tbl_name='" .. tablename .. "'"
      else    
         sql = "select csc2 from sqlite_master where type='table' and tbl_name='" .. name .. "'"
      end  
      local showdata = "<a href='/@" .. name .."'> SHOW DATA </a> <br>"
      local resultset = db:exec(sql)
      local result = resultset:fetch()
      db:write("<html><body><br>")
      db:write(showdata)
      db:write("<a href=\"/\"> HOME </a> <br><br><br> SCHEMA <br><br><br> <pre>")
      while result do
          local res = tostring(result.csc2);
          db:write(res)
          print(res)
          result = resultset:fetch()
      end     
      db:write("</pre></body></html>")
   end  
end
