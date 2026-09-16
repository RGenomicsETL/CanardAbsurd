cmake_minimum_required(VERSION 3.15)

project(CanardCoordinatorExtension LANGUAGES C)

set(EXTENSION_NAME canard_coordinator)
set(EXTENSION_SOURCES
    src/canard_coordinator.c
    src/canard_extension.c)
if(WIN32)
  list(APPEND EXTENSION_SOURCES src/canard_thread_windows.c)
else()
  list(APPEND EXTENSION_SOURCES src/canard_thread.c)
endif()

build_loadable_extension_capi(${EXTENSION_NAME} 1 2 0 "" ${EXTENSION_SOURCES})
set_target_properties(${EXTENSION_NAME}_loadable_extension PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS ON)

target_compile_features(${EXTENSION_NAME}_loadable_extension PRIVATE c_std_11)
target_include_directories(${EXTENSION_NAME}_loadable_extension PRIVATE src/include)

if(NOT WIN32)
  find_package(Threads REQUIRED)
  target_link_libraries(${EXTENSION_NAME}_loadable_extension PRIVATE Threads::Threads)
endif()

install(
  TARGETS ${EXTENSION_NAME}_loadable_extension
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION lib)
