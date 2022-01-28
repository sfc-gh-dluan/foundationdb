find_path(LIBURING_INCLUDE_DIR NAMES liburing.h PATH_SUFFIXES include )

find_package_handle_standard_args(LibUring
  REQUIRED_VARS LIBURING_INCLUDE_DIR
  FAIL_MESSAGE "Could not find Liburing header files")

if(LIBURING_FOUND)
add_library(LibUring INTERFACE)
target_include_directories(LibUring INTERFACE "${VALGRIND_INCLUDE_DIR}")
else()
message( FATAL_ERROR "LIBURING not found" )
endif()