cmake_minimum_required(VERSION 3.16.3)

project(webscraper)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

find_package(CURL REQUIRED)
find_package(LibXml2 REQUIRED)
find_package(SQLITE3 REQUIRED)

add_executable(scrape scrape.cc util.cc)

target_include_directories(scrape PUBLIC ${CURL_INCLUDE_DIRS})
target_include_directories(scrape PUBLIC ${LIBXML2_INCLUDE_DIR})

target_link_libraries(scrape ${CURL_LIBRARIES})
target_link_libraries(scrape ${LIBXML2_LIBRARIES})

add_executable(scrape_all scrape_all.cc util.cc)

target_include_directories(scrape_all PUBLIC ${SQLITE3_INCLUDE_DIRS})

target_link_libraries(scrape_all ${SQLITE3_LIBRARIES})

configure_file(base.db scrape.db COPYONLY)

