cmake_minimum_required(VERSION 3.16.2)

project(webscraper)

set (CMAKE_CXX_STANDARD 17)

set(ENV{PKG_CONFIG_DIR} "")
set(ENV{PKG_CONFIG_LIBDIR} "/opt/curl/lib/pkgconfig:/usr/lib/pkgconfig:${CMAKE_SYSROOT}/usr/share/pkgconfig")
set(ENV{PKG_CONFIG_SYSROOT_DIR} ${CMAKE_SYSROOT})

#set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

find_package(CURL REQUIRED)

add_subdirectory(x_lexbor)

add_executable(crawl crawl_main.cc crawler.cc crawl.cc scraper.cc scrape.cc util.cc)

target_include_directories(crawl PUBLIC ${CURL_INCLUDE_DIRS})
target_include_directories(crawl PUBLIC ${LEXBOR_INCLUDES})

target_link_libraries(crawl ${CURL_LIBRARIES})
target_link_libraries(crawl lexbor)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

target_link_libraries(crawl Threads::Threads)

add_executable(scorer score_main.cc scorer.cc crawl.cc util.cc)

add_executable(index index.cc crawl.cc util.cc tokenizer.cc
	x_cocomel/bst.c
	x_cocomel/hash_table.c
	x_cocomel/memory.c
	x_cocomel/posting.c
	x_cocomel/vbyte)

target_link_libraries(index ${CURL_LIBRARIES})
target_link_libraries(index lexbor)
target_link_libraries(index Threads::Threads)

target_include_directories(index PUBLIC ${CURL_INCLUDE_DIRS})
target_include_directories(index PUBLIC ${LEXBOR_INCLUDES})


add_executable(search search.cc scorer.cc crawl.cc util.cc tokenizer.cc
	x_cocomel/bst.c
	x_cocomel/hash_table.c
	x_cocomel/memory.c
	x_cocomel/posting.c
	x_cocomel/vbyte)

target_link_libraries(search ${CURL_LIBRARIES})
target_link_libraries(search lexbor)
target_link_libraries(search Threads::Threads)

target_include_directories(search PUBLIC ${CURL_INCLUDE_DIRS})
target_include_directories(search PUBLIC ${LEXBOR_INCLUDES})


