
set(BENCHMARK_HISTOGRAM
${CMAKE_SOURCE_DIR}/benchmarks/histogram.cpp
)

# pmemkv
#add_executable(pmemkv_bench
#  ${CMAKE_CURRENT_LIST_DIR}/pmemkv_bench.cpp
#  ${BENCHMARK_HISTOGRAM}
#)
#
#add_executable(ycsb_pmemkv ${CMAKE_CURRENT_LIST_DIR}/ycsb_pmemkv.cpp)
#
#find_library(PMEMKV pmemkv)
#if (NOT PMEMKV_FOUND)
#  include(FetchContent)
#  FetchContent_Declare(
#    pmemkv
#    GIT_REPOSITORY  https://github.com/pmem/pmemkv.git
#    GIT_TAG         a92abed550ece9c5c70b6be17db8e9cb19e328e4
#  )
#  set(BUILD_TESTS OFF CACHE BOOL "Suppressing pmemkv' tests" FORCE)
#  set(BUILD_JSON_CONFIG OFF CACHE BOOL "Suppressing pmemkv' helper library" FORCE)
#  FetchContent_MakeAvailable(pmemkv)
#  target_include_directories(pmemkv_bench PRIVATE ${pmemkv_SOURCE_DIR}/src)
#  target_include_directories(ycsb_pmemkv PRIVATE ${pmemkv_SOURCE_DIR}/src)
#endif()
#
#target_link_libraries(pmemkv_bench benchmark::benchmark pmemkv "stdc++fs")
#
#target_link_libraries(ycsb_pmemkv pmemkv "stdc++fs")

# viper
# CONCURRENTQUEUE
add_library(viper INTERFACE)
target_include_directories(viper INTERFACE ${CMAKE_CURRENT_LIST_DIR}/viper/)
target_sources(viper INTERFACE ${CMAKE_CURRENT_LIST_DIR}/viper/hotkeyset.cpp)
target_compile_options(viper INTERFACE -march=native)

option(VIPER_CONCURRENT_QUEUE_PROVIDED "Set ON if the concurrentqueue dependency is provided and should not be downloaded by Viper." OFF)
if (NOT ${VIPER_CONCURRENT_QUEUE_PROVIDED})
    FetchContent_Declare(
            concurrentqueue
            GIT_REPOSITORY https://github.com/cameron314/concurrentqueue.git
            GIT_TAG v1.0.3
    )
    FetchContent_MakeAvailable(concurrentqueue)
    target_link_libraries(viper INTERFACE concurrentqueue ${PMEM} ${PMEMOBJ} "stdc++fs")
endif()

add_executable(viper_bench
  ${CMAKE_CURRENT_LIST_DIR}/viper_bench.cpp
  ${BENCHMARK_HISTOGRAM}
)
target_link_libraries(viper_bench viper benchmark::benchmark ${PMEM} ${PMEMOBJ} "stdc++fs")

add_executable(ycsb_viper ${CMAKE_CURRENT_LIST_DIR}/ycsb_viper.cpp)
target_link_libraries(ycsb_viper viper ${PMEM} ${PMEMOBJ} "stdc++fs")

# pmem_rocksdb
#add_executable(pmem_rocksdb_bench
#  ${CMAKE_CURRENT_LIST_DIR}/pmem_rocksdb_bench.cpp
#  ${BENCHMARK_HISTOGRAM}
#)
#add_executable(ycsb_pmem_rocksdb ${CMAKE_CURRENT_LIST_DIR}/ycsb_pmem_rocksdb.cpp)
#
#find_package(Git QUIET)
#set(PMEM_ROCKSDB_SRC_DIR ${CMAKE_CURRENT_LIST_DIR}/pmem-rocksdb)
#if(GIT_FOUND AND EXISTS "${PROJECT_SOURCE_DIR}/.git")
## Update submodules as needed
#    option(GIT_SUBMODULE "Check submodules during build" ON)
#    if(GIT_SUBMODULE)
#        message(STATUS "Submodule update")
#        execute_process(
#          COMMAND ${GIT_EXECUTABLE} submodule update --init --recursive
#          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
#          RESULT_VARIABLE PMEM_ROCKSDB_SUBMODULE_RESULT
#        )
#        if(NOT PMEM_ROCKSDB_SUBMODULE_RESULT EQUAL "0")
#            message(FATAL_ERROR "git submodule update --init --recursive failed with ${PMEM_ROCKSDB_SUBMODULE_RESULT}, please checkout submodules")
#        endif()
#    endif()
#endif()
## build pmem-rocksdb::rocksdb
#add_custom_command(TARGET pmem_rocksdb_bench PRE_BUILD
#  COMMAND ${CMAKE_COMMAND} -DCMAKE_BUILD_TYPE=Release -DWITH_DCPMM=ON -DWITH_SNAPPY=ON -DFAIL_ON_WARNINGS=OFF -S . -B "build"
#  COMMAND ${CMAKE_COMMAND} --build "build" --target rocksdb -j
#  WORKING_DIRECTORY ${PMEM_ROCKSDB_SRC_DIR}
#)
#add_custom_command(TARGET ycsb_pmem_rocksdb PRE_BUILD
#  COMMAND ${CMAKE_COMMAND} -DCMAKE_BUILD_TYPE=Release -DWITH_DCPMM=ON -DWITH_SNAPPY=ON -DFAIL_ON_WARNINGS=OFF -S . -B "build"
#  COMMAND ${CMAKE_COMMAND} --build "build" --target rocksdb -j
#  WORKING_DIRECTORY ${PMEM_ROCKSDB_SRC_DIR}
#)
#
#target_compile_definitions(pmem_rocksdb_bench PRIVATE ON_DCPMM)
#target_include_directories(pmem_rocksdb_bench PRIVATE ${PMEM_ROCKSDB_SRC_DIR}/include)
#target_link_directories(pmem_rocksdb_bench PRIVATE ${PMEM_ROCKSDB_SRC_DIR}/build)
#target_link_libraries(pmem_rocksdb_bench benchmark::benchmark rocksdb ${PMEM} ${PMEMOBJ} snappy "stdc++fs")
#
#target_compile_definitions(ycsb_pmem_rocksdb PRIVATE ON_DCPMM)
#target_include_directories(ycsb_pmem_rocksdb PRIVATE ${PMEM_ROCKSDB_SRC_DIR}/include)
#target_link_directories(ycsb_pmem_rocksdb PRIVATE ${PMEM_ROCKSDB_SRC_DIR}/build)
#target_link_libraries(ycsb_pmem_rocksdb rocksdb ${PMEM} ${PMEMOBJ} snappy "stdc++fs")


# ChameleonDB
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/ChameleonDB CHAMELEONDB_SOURCE_FILES)
add_executable(chameleondb_bench
  ${CMAKE_CURRENT_LIST_DIR}/chameleondb_bench.cpp
  ${CHAMELEONDB_SOURCE_FILES}
  ${BENCHMARK_HISTOGRAM}
  ${UTIL_FILES}
)
add_executable(ycsb_chameleondb
  ${CMAKE_CURRENT_LIST_DIR}/ycsb_chameleondb.cpp
  ${CHAMELEONDB_SOURCE_FILES}
  ${UTIL_FILES}
)
target_link_libraries(chameleondb_bench benchmark::benchmark ${PMEM} ${PMEMOBJ} "stdc++fs")
target_link_libraries(ycsb_chameleondb ${PMEM} ${PMEMOBJ} "stdc++fs")
