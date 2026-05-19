include(${PROJECT_SOURCE_DIR}/cmake/plugin.cmake)
include(${PROJECT_SOURCE_DIR}/sqlite/definitions.cmake)
add_definitions(${SQLITE_FLAGS})

include_directories(
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_BINARY_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/bdb
  ${PROJECT_SOURCE_DIR}/cdb2api
  ${PROJECT_SOURCE_DIR}/crc32c
  ${PROJECT_SOURCE_DIR}/csc2
  ${PROJECT_SOURCE_DIR}/datetime
  ${PROJECT_SOURCE_DIR}/db
  ${PROJECT_SOURCE_DIR}/dfp/decNumber
  ${PROJECT_SOURCE_DIR}/dfp/dfpal
  ${PROJECT_SOURCE_DIR}/dlmalloc
  ${PROJECT_SOURCE_DIR}/lua
  ${PROJECT_BINARY_DIR}/lua
  ${PROJECT_SOURCE_DIR}/mem
  ${PROJECT_SOURCE_DIR}/net
  ${PROJECT_SOURCE_DIR}/protobuf
  ${PROJECT_SOURCE_DIR}/sqlite/src
  ${PROJECT_BINARY_DIR}/db
  ${PROJECT_BINARY_DIR}/mem
  ${PROJECT_BINARY_DIR}/protobuf
  ${PROJECT_BINARY_DIR}/sqlite
  ${PROJECT_BINARY_DIR}/plugins
  ${CMAKE_CURRENT_BINARY_DIR}
  ${OPENSSL_INCLUDE_DIR}
  ${PROTOBUF-C_INCLUDE_DIR}
)

# Embed default_handler.lua as a C byte array
file(READ ${CMAKE_CURRENT_SOURCE_DIR}/default_handler.lua DEFAULT_HANDLER_LUA_CONTENT HEX)
# Convert hex to comma-separated hex bytes
string(REGEX REPLACE "([0-9a-f][0-9a-f])" "0x\\1," DEFAULT_HANDLER_LUA_HEX "${DEFAULT_HANDLER_LUA_CONTENT}")
string(LENGTH "${DEFAULT_HANDLER_LUA_CONTENT}" DEFAULT_HANDLER_LUA_HEX_LEN)
math(EXPR DEFAULT_HANDLER_LUA_LEN "${DEFAULT_HANDLER_LUA_HEX_LEN} / 2")
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/default_handler_lua.h
  "/* WARNING: auto-generated from default_handler.lua, do not edit. */\n"
  "static const unsigned char default_handler_lua[] = {\n"
  "${DEFAULT_HANDLER_LUA_HEX}\n"
  "};\n"
  "static const int default_handler_lua_len = ${DEFAULT_HANDLER_LUA_LEN};\n"
)

set(HTTPSERVER_SRCS httpserver.c)
add_plugin(httpserver STATIC "${HTTPSERVER_SRCS}")
add_dependencies(httpserver sqlite)
