cmake_minimum_required(VERSION 3.24)

project(pace-migration-utils)

if(CMAKE_EXPORT_COMPILE_COMMANDS)
  set(CMAKE_CXX_STANDARD_INCLUDE_DIRECTORIES ${CMAKE_CXX_IMPLICIT_INCLUDE_DIRECTORIES})
endif()

add_subdirectory(spdlog spdlog)
add_subdirectory(gtl gtl)
add_subdirectory(CLI11 CLI11)
find_package(PkgConfig REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(OpenMP REQUIRED)

pkg_check_modules(ACL REQUIRED libacl)

add_library(mdb liblmdb/midl.c liblmdb/mdb.c)
target_include_directories(mdb PUBLIC liblmdb)

add_library(lmdb-misc STATIC lmdb-misc.c)
target_link_libraries(lmdb-misc PUBLIC mdb)

add_executable(copy2 
    copy2.cpp
    xxhash.c
    xxh_x86dispatch.c
    allocator.cpp
    hlinkstate.cpp
    syscalls.cpp)

target_link_libraries(copy2 PUBLIC OpenMP::OpenMP_CXX aio spdlog::spdlog gtl mdb CLI11::CLI11)
target_compile_features(copy2 PUBLIC cxx_std_20)

option(COPY2_BUILD_TESTS "Build copy2 functional tests" ON)
if(COPY2_BUILD_TESTS)
  enable_testing()
  list(APPEND CMAKE_CTEST_ARGUMENTS "--output-on-failure")
  find_package(Python3 COMPONENTS Interpreter)
  if(Python3_Interpreter_FOUND)
    set(COPY2_TESTS_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    set(COPY2_TESTS_SEED 1234 CACHE STRING "Seed for copy2 test tree generation")
    set(COPY2_TESTS_PROFILE small CACHE STRING "copy2 test profile (small|medium)")

    add_test(NAME copy2.build
      COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target copy2)
    set_tests_properties(copy2.build PROPERTIES FIXTURES_SETUP copy2_built)

    # add_copy2_scenario(<name> <mode> <extras> <allow_extra> <check_sparse> "<copy2 flags>")
    function(add_copy2_scenario name mode extras allow_extra check_sparse flags)
      add_test(NAME copy2.${name}
        COMMAND ${CMAKE_COMMAND}
          -DPYTHON=${Python3_EXECUTABLE}
          -DGEN=${COPY2_TESTS_DIR}/gentrees.py
          -DVERIFY=${COPY2_TESTS_DIR}/verify.py
          -DCOPY2=$<TARGET_FILE:copy2>
          -DMODE=${mode}
          -DEXTRAS=${extras}
          -DALLOW_EXTRA=${allow_extra}
          -DCHECK_SPARSE=${check_sparse}
          "-DFLAGS=${flags}"
          -DWORKDIR=${CMAKE_CURRENT_BINARY_DIR}/copy2-tests/${name}
          -DSEED=${COPY2_TESTS_SEED}
          -DPROFILE=${COPY2_TESTS_PROFILE}
          -P ${COPY2_TESTS_DIR}/run_scenario.cmake)
      set_tests_properties(copy2.${name} PROPERTIES
        TIMEOUT 300
        FIXTURES_REQUIRED copy2_built)
    endfunction()

    #                      name                  mode      extras allow_extra check_sparse flags
    add_copy2_scenario(empty_basic            empty     OFF    OFF         OFF         "")
    add_copy2_scenario(empty_sparse           empty     OFF    OFF         ON          "--sparse --max-block-size 1MB")
    add_copy2_scenario(empty_sparse_readback  empty     OFF    OFF         ON          "--sparse --readback --max-block-size 1MB")
    add_copy2_scenario(identical_noop         identical OFF    OFF         OFF         "")
    add_copy2_scenario(identical_sync_noop    identical OFF    OFF         OFF         "--sync")
    add_copy2_scenario(modified_nosync        modified  OFF    OFF         OFF         "")
    add_copy2_scenario(modified_nosync_extras modified  ON     ON          OFF         "")
    add_copy2_scenario(modified_sync          modified  ON     OFF         OFF         "--sync")
    add_copy2_scenario(modified_sync_readback modified  ON     OFF         OFF         "--sync --readback")
  else()
    message(STATUS "Python3 interpreter not found; skipping copy2 functional tests")
  endif()
endif()

if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    set(CMAKE_INSTALL_PREFIX "/opt/pace-migration-utils" CACHE PATH "..." FORCE)
endif()

include(GNUInstallDirs)

install(TARGETS
    copy2
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

install(FILES ${CMAKE_SOURCE_DIR}/../README.md DESTINATION "${CMAKE_INSTALL_DOCDIR}" COMPONENT Documentation)

# --- Packaging Logic ---

# Set package metadata
set(CPACK_PACKAGE_NAME "pace-migration-utils")
set(CPACK_PACKAGE_VENDOR "PACE")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Tools to transfer data and verify transfers")
set(CPACK_PACKAGE_VERSION "4.0.0")
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_PACKAGE_CONTACT "alambert48@gatech.edu")
# set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE")

# RPM Specifics
set(CPACK_GENERATOR "RPM")
# set(CPACK_RPM_PACKAGE_LICENSE "MIT") # Or your specific license
set(CPACK_RPM_PACKAGE_GROUP "Development/Tools")
set(CPACK_RPM_PACKAGE_AUTOREQ "YES") # Automatically find dependencies

# This line must be last
include(CPack)
