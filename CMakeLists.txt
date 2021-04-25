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

add_executable(main main.cc
    scorer.cc
    merger.cc
    indexer.cc
    index_part.cc
    index.cc
    hash.cc
    posting.cc
    vbyte.cc
    crawler.cc
    crawl.cc
    scraper.cc
    scrape_op.cc
    scrape.cc
    util.cc
    tokenizer.cc
    str.c
    config.cc)

target_include_directories(main PUBLIC ${CURL_INCLUDE_DIRS})

target_link_libraries(main ${CURL_LIBRARIES})
target_link_libraries(main Threads::Threads)
target_link_libraries(main nlohmann_json::nlohmann_json)
target_link_libraries(main spdlog::spdlog)
target_link_libraries(main foonathan_memory)


add_executable(crawl crawl_main.cc
    crawler.cc
    crawl.cc
    scraper.cc
    scrape_op.cc
    scrape.cc
    util.cc
    tokenizer.cc
    str.c
    config.cc)

target_include_directories(crawl PUBLIC ${CURL_INCLUDE_DIRS})

target_link_libraries(crawl ${CURL_LIBRARIES})
target_link_libraries(crawl Threads::Threads)
target_link_libraries(crawl nlohmann_json::nlohmann_json)
target_link_libraries(crawl spdlog::spdlog)

add_executable(scorer score_main.cc
    scorer.cc
    crawler.cc
    crawl.cc
    scrape_op.cc
    scrape.cc
    tokenizer.cc
    str.c
    util.cc
    config.cc)

target_link_libraries(scorer nlohmann_json::nlohmann_json)
target_link_libraries(scorer spdlog::spdlog)

add_executable(indexer indexer_main.cc
    indexer.cc
    crawler.cc
    crawl.cc
    scrape_op.cc
    scrape.cc
    tokenizer.cc
    util.cc
    str.c
    hash.cc
    posting.cc
    vbyte.cc
    index_part.cc
    index.cc
    config.cc)

target_link_libraries(indexer nlohmann_json::nlohmann_json)
target_link_libraries(indexer Threads::Threads)
target_link_libraries(indexer spdlog::spdlog)
target_link_libraries(indexer foonathan_memory)

add_executable(merger merger_main.cc
    merger.cc
    crawler.cc
    crawl.cc
    scrape_op.cc
    scrape.cc
    tokenizer.cc
    util.cc
    str.c
    hash.cc
    posting.cc
    vbyte.cc
    index_part.cc
    index.cc
    config.cc)

target_link_libraries(merger nlohmann_json::nlohmann_json)
target_link_libraries(merger Threads::Threads)
target_link_libraries(merger spdlog::spdlog)
target_link_libraries(merger foonathan_memory)

add_executable(search_cli search_cli.cc
    search.cc
    crawler.cc
    crawl.cc
    scrape_op.cc
    scrape.cc
    scorer.cc
    tokenizer.cc
    util.cc
    str.c
    hash.cc
    posting.cc
    vbyte.cc
    index_part.cc
    index.cc
    config.cc)

target_link_libraries(search_cli nlohmann_json::nlohmann_json)
target_link_libraries(search_cli spdlog::spdlog)
target_link_libraries(search_cli foonathan_memory)

add_executable(search_server search_server.cc
    search.cc
    crawler.cc
    crawl.cc
    scrape_op.cc
    scrape.cc
    scorer.cc
    tokenizer.cc
    util.cc
    str.c
	hash.cc
	posting.cc
	vbyte.cc
    index_part.cc
    index.cc
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
    scorer.cc
    merger.cc
    indexer.cc
    index_part.cc
    index.cc
    hash.cc
    posting.cc
    vbyte.cc
    crawler.cc
    crawl.cc
    scrape_op.cc
    scrape.cc
    util.cc
    tokenizer.cc
    str.c
    config.cc)

target_include_directories(main_capnp PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(main_capnp Threads::Threads)
target_link_libraries(main_capnp nlohmann_json::nlohmann_json)
target_link_libraries(main_capnp spdlog::spdlog)
target_link_libraries(main_capnp foonathan_memory)
target_link_libraries(main_capnp CapnProto::capnp-rpc)

add_executable(crawler_capnp crawler_capnp.cc
    ${capnp_sources}
    scorer.cc
    indexer.cc
    index_part.cc
    index.cc
    hash.cc
    posting.cc
    vbyte.cc
    crawler.cc
    crawl.cc
    scraper.cc
    scrape_op.cc
    scrape.cc
    util.cc
    tokenizer.cc
    str.c
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
    scorer.cc
    indexer.cc
    index_part.cc
    index.cc
    hash.cc
    posting.cc
    vbyte.cc
    crawler.cc
    crawl.cc
    scrape_op.cc
    scrape.cc
    util.cc
    tokenizer.cc
    str.c
    config.cc)

target_include_directories(indexer_capnp PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(indexer_capnp Threads::Threads)
target_link_libraries(indexer_capnp nlohmann_json::nlohmann_json)
target_link_libraries(indexer_capnp spdlog::spdlog)
target_link_libraries(indexer_capnp foonathan_memory)
target_link_libraries(indexer_capnp CapnProto::capnp-rpc)

add_executable(merger_capnp merger_capnp.cc
    ${capnp_sources}
    scorer.cc
    indexer.cc
    index_part.cc
    index.cc
    hash.cc
    posting.cc
    vbyte.cc
    crawler.cc
    crawl.cc
    scrape_op.cc
    scrape.cc
    util.cc
    tokenizer.cc
    str.c
    config.cc)

target_include_directories(merger_capnp PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(merger_capnp Threads::Threads)
target_link_libraries(merger_capnp nlohmann_json::nlohmann_json)
target_link_libraries(merger_capnp spdlog::spdlog)
target_link_libraries(merger_capnp foonathan_memory)
target_link_libraries(merger_capnp CapnProto::capnp-rpc)

