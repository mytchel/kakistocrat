cmake_minimum_required(VERSION 3.16.2)

project(webscraper)

set (CMAKE_CXX_STANDARD 17)

#set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

find_package(CURL REQUIRED)
find_package(LibXml2 REQUIRED)

add_subdirectory(x_lexbor)

add_executable(crawl crawl_main.cc crawl.cc scrape.cc util.cc)

target_include_directories(crawl PUBLIC ${CURL_INCLUDE_DIRS})
target_include_directories(crawl PUBLIC ${LEXBOR_INCLUDES})

target_link_libraries(crawl ${CURL_LIBRARIES})
target_link_libraries(crawl lexbor)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

target_link_libraries(crawl Threads::Threads)

add_executable(test test.cc crawl.cc scrape.cc util.cc
	x_cocomel/bst.c
	x_cocomel/hash_table.c
	x_cocomel/memory.c
	x_cocomel/posting.c
	x_cocomel/search.c
	x_cocomel/vbyte)

target_link_libraries(test ${CURL_LIBRARIES})
target_link_libraries(test lexbor)
target_link_libraries(test Threads::Threads)

target_include_directories(test PUBLIC ${CURL_INCLUDE_DIRS})
target_include_directories(test PUBLIC ${LEXBOR_INCLUDES})


