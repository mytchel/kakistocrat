cmake_minimum_required(VERSION 3.16.3)

project(webscraper)

add_subdirectory(x_lexbor ${CMAKE_CURRENT_BINARY_DIR}/lexbor EXCLUDE_FROM_ALL)

find_package(CURL REQUIRED)
find_package(LibXml2 REQUIRED)

add_executable(scrape scrape.cc)

target_include_directories(scrape PUBLIC ${CURL_INCLUDE_DIRS})
target_include_directories(scrape PUBLIC ${LIBXML2_INCLUDE_DIR})

#target_link_libraries(scrape lexbor)
target_link_libraries(scrape ${CURL_LIBRARIES})
target_link_libraries(scrape ${LIBXML2_LIBRARIES})

