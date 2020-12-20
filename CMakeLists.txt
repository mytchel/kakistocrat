cmake_minimum_required(VERSION 3.16.2)

project(webscraper)

#set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

find_package(CURL REQUIRED)
find_package(LibXml2 REQUIRED)

add_subdirectory(x_lexbor)

add_executable(scrape scrape_main.cc scrape.cc util.cc)

target_include_directories(scrape PUBLIC ${CURL_INCLUDE_DIRS})
target_include_directories(scrape PUBLIC ${LIBXML2_INCLUDE_DIR})
target_include_directories(scrape PUBLIC ${LEXBOR_INCLUDES})

target_link_libraries(scrape ${CURL_LIBRARIES})
target_link_libraries(scrape ${LIBXML2_LIBRARIES})
target_link_libraries(scrape lexbor)

add_executable(crawl crawl.cc scrape.cc util.cc)

target_include_directories(crawl PUBLIC ${CURL_INCLUDE_DIRS})
target_include_directories(crawl PUBLIC ${LIBXML2_INCLUDE_DIR})
target_include_directories(crawl PUBLIC ${LEXBOR_INCLUDES})

target_link_libraries(crawl ${CURL_LIBRARIES})
target_link_libraries(crawl ${LIBXML2_LIBRARIES})
target_link_libraries(crawl lexbor)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

target_link_libraries(crawl Threads::Threads)

