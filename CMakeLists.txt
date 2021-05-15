cmake_minimum_required(VERSION 3.16.2)

project(webscraper)

set (CMAKE_CXX_STANDARD 17)

set(ENV{PKG_CONFIG_DIR} "")
set(ENV{PKG_CONFIG_LIBDIR} "/opt/curl/lib/pkgconfig:/usr/lib/pkgconfig:${CMAKE_SYSROOT}/usr/share/pkgconfig")
set(ENV{PKG_CONFIG_SYSROOT_DIR} ${CMAKE_SYSROOT})

#set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

find_package(CURL REQUIRED)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

find_package(ZLIB)
find_package(OpenSSL)

find_package(Boost 1.67 COMPONENTS system thread regex REQUIRED)

find_package(CapnProto)
set(CAPNP_INCLUDE_DIRECTORY /usr/include/) # Fix bug with void linux package.

add_subdirectory(x_nlohmann)

add_subdirectory(x_spdlog)

add_subdirectory(x_memory)

configure_file(x_seed/seed seed COPYONLY)
configure_file(x_seed/blacklist blacklist COPYONLY)

configure_file(config.json config.json COPYONLY)

add_executable(search_server search_server.cc
    ${capnp_sources}
    ${capnp_headers}

    search.cc

    index.cc
    index_part.cc
    tokenizer.cc
    posting.cc
    vbyte.cc
    str.c

    hash.cc
    site.cc
    util.cc
    config.cc)

target_include_directories(search_server PUBLIC ${Boost_INCLUDE_DIR})
target_include_directories(search_server PUBLIC ${OPENSSL_INCLUDE_DIR})
target_include_directories(search_server PUBLIC x_crow/include)

target_link_libraries(search_server nlohmann_json::nlohmann_json)
target_link_libraries(search_server ${Boost_LIBRARIES})
target_link_libraries(search_server z)
target_link_libraries(search_server spdlog::spdlog)
target_link_libraries(search_server foonathan_memory)

configure_file(templates/search.html templates/search.html COPYONLY)

capnp_generate_cpp(capnp_sources capnp_headers indexer.capnp)

add_executable(main_capnp main_capnp.cc
    ${capnp_sources}
    ${capnp_headers}

    crawler.cc

    index.cc
    index_part.cc
    tokenizer.cc
    posting.cc
    vbyte.cc
    str.c

    hash.cc
    site.cc
    util.cc
    config.cc)

target_include_directories(main_capnp PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(main_capnp Threads::Threads)
target_link_libraries(main_capnp nlohmann_json::nlohmann_json)
target_link_libraries(main_capnp spdlog::spdlog)
target_link_libraries(main_capnp foonathan_memory)
target_link_libraries(main_capnp CapnProto::capnp-rpc)

add_executable(crawler_capnp crawler_capnp.cc
    ${capnp_sources}
    ${capnp_headers}

    curl_kj.cc
    scrape_op.cc
    scrape.cc
    tokenizer.cc
    str.c

    hash.cc
    site.cc
    util.cc
    config.cc)

target_include_directories(crawler_capnp PUBLIC ${CURL_INCLUDE_DIRS})
target_include_directories(crawler_capnp PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(crawler_capnp ${CURL_LIBRARIES})
target_link_libraries(crawler_capnp Threads::Threads)
target_link_libraries(crawler_capnp nlohmann_json::nlohmann_json)
target_link_libraries(crawler_capnp spdlog::spdlog)
target_link_libraries(crawler_capnp foonathan_memory)
target_link_libraries(crawler_capnp CapnProto::capnp-rpc)

add_executable(indexer_capnp indexer_capnp.cc
    ${capnp_sources}
    ${capnp_headers}

    index_part.cc
    index.cc
    posting.cc
    vbyte.cc
    tokenizer.cc
    str.c

    hash.cc
    site.cc
    util.cc
    config.cc)

target_include_directories(indexer_capnp PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(indexer_capnp Threads::Threads)
target_link_libraries(indexer_capnp nlohmann_json::nlohmann_json)
target_link_libraries(indexer_capnp spdlog::spdlog)
target_link_libraries(indexer_capnp foonathan_memory)
target_link_libraries(indexer_capnp CapnProto::capnp-rpc)

add_executable(merger_capnp merger_capnp.cc
    ${capnp_sources}
    ${capnp_headers}

    index_part.cc
    index.cc
    posting.cc
    vbyte.cc
    tokenizer.cc
    str.c

    hash.cc
    site.cc
    util.cc
    config.cc)

target_include_directories(merger_capnp PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(merger_capnp Threads::Threads)
target_link_libraries(merger_capnp nlohmann_json::nlohmann_json)
target_link_libraries(merger_capnp spdlog::spdlog)
target_link_libraries(merger_capnp foonathan_memory)
target_link_libraries(merger_capnp CapnProto::capnp-rpc)


