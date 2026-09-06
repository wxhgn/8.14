#----------------------------------------------------------------
# Generated CMake target import file.
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "minilog::minilog" for configuration ""
set_property(TARGET minilog::minilog APPEND PROPERTY IMPORTED_CONFIGURATIONS NOCONFIG)
set_target_properties(minilog::minilog PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_NOCONFIG "CXX"
  IMPORTED_LOCATION_NOCONFIG "${_IMPORT_PREFIX}/lib/libminilog.a"
  )

list(APPEND _IMPORT_CHECK_TARGETS minilog::minilog )
list(APPEND _IMPORT_CHECK_FILES_FOR_minilog::minilog "${_IMPORT_PREFIX}/lib/libminilog.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
