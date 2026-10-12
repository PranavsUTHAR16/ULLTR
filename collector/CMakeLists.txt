cmake_minimum_required(VERSION 3.15)
project(upstox_collector CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Extract Git Commit Hash at configuration/build time
execute_process(
    COMMAND git rev-parse --short HEAD
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE GIT_COMMIT_HASH
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
)
if(NOT GIT_COMMIT_HASH)
    set(GIT_COMMIT_HASH "dev-build")
endif()
add_definitions(-DGIT_COMMIT_HASH="${GIT_COMMIT_HASH}")
add_definitions(-DBUILD_TIMESTAMP="${CMAKE_SYSTEM_NAME} ${CMAKE_BUILD_TYPE}")

# Homebrew paths for Apple Silicon macs
include_directories(/opt/homebrew/include)
link_directories(/opt/homebrew/lib)

# Find packages
find_package(Boost REQUIRED)
find_package(Protobuf REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

# Protobuf generated source files
set(PROTO_SRC
    proto/MarketDataFeedV3.pb.cc
)

# Source files
set(SOURCES
    src/main.cpp
    src/candle_manager.cpp
    src/microstructure_engine.cpp
    ${PROTO_SRC}
)

add_executable(collector ${SOURCES})

# Include directories
target_include_directories(collector PRIVATE
    ${PROJECT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/proto
    ${Boost_INCLUDE_DIRS}
    ${Protobuf_INCLUDE_DIRS}
    ${OPENSSL_INCLUDE_DIR}
)

# Libraries to link
if(APPLE)
    target_link_libraries(collector PRIVATE
        protobuf::libprotobuf
        absl_log_internal_message
        absl_log_internal_nullguard
        absl_log_internal_check_op
        absl_raw_logging_internal
        absl_strings
        absl_strings_internal
        absl_hash
        absl_synchronization
        absl_time
        absl_time_zone
        absl_throw_delegate
        absl_spinlock_wait
        absl_stacktrace
        absl_malloc_internal
        OpenSSL::SSL
        OpenSSL::Crypto
        Threads::Threads
        hiredis
    )
else()
    target_link_libraries(collector PRIVATE
        protobuf::libprotobuf
        OpenSSL::SSL
        OpenSSL::Crypto
        Threads::Threads
        hiredis
    )
endif()
