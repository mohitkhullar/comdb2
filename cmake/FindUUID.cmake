find_path(UUID_INCLUDE_DIR NAMES uuid/uuid.h)
find_library(UUID_LIBRARY NAMES uuid)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(UUID DEFAULT_MSG UUID_INCLUDE_DIR UUID_LIBRARY)
