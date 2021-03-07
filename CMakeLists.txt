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

configure_file(x_seed/seed seed COPYONLY)
configure_file(x_seed/blacklist blacklist COPYONLY)

add_executable(crawl crawl_main.cc
    crawler.cc
    crawl.cc
    scraper.cc
    scrape.cc
    util.cc
    tokenizer.cc
    str.c)

target_include_directories(crawl PUBLIC ${CURL_INCLUDE_DIRS})

target_link_libraries(crawl ${CURL_LIBRARIES})
target_link_libraries(crawl Threads::Threads)
target_link_libraries(crawl nlohmann_json::nlohmann_json)

add_executable(scorer score_main.cc scorer.cc crawl.cc util.cc)

target_link_libraries(scorer nlohmann_json::nlohmann_json)

add_executable(index index.cc
    crawl.cc
    tokenizer.cc
    util.cc
    str.c
	x_cocomel/bst.c
	x_cocomel/hash_table.c
	x_cocomel/memory.c
	x_cocomel/posting.c
	x_cocomel/vbyte)

target_link_libraries(index nlohmann_json::nlohmann_json)

add_executable(search search_cli.cc
    search.cc
    scorer.cc
    crawl.cc
    tokenizer.cc
    util.cc
    str.c
	x_cocomel/bst.c
	x_cocomel/hash_table.c
	x_cocomel/memory.c
	x_cocomel/posting.c
	x_cocomel/vbyte)

target_include_directories(search PUBLIC ${CURL_INCLUDE_DIRS})

target_link_libraries(search nlohmann_json::nlohmann_json)

add_subdirectory(x_nlohmann)

add_executable(search_server search_server.cc
    search.cc
    scorer.cc
    crawl.cc
    tokenizer.cc
    util.cc
    str.c
	x_cocomel/bst.c
	x_cocomel/hash_table.c
	x_cocomel/memory.c
	x_cocomel/posting.c
	x_cocomel/vbyte)

target_include_directories(search_server PUBLIC ${Boost_INCLUDE_DIR})
target_include_directories(search_server PUBLIC ${OPENSSL_INCLUDE_DIR})
target_include_directories(search_server PUBLIC x_crow/include)

target_link_libraries(search_server nlohmann_json::nlohmann_json)
target_link_libraries(search_server ${Boost_LIBRARIES})
target_link_libraries(search_server z)

configure_file(templates/search.html templates/search.html COPYONLY)

