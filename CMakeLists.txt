cmake_minimum_required(VERSION 3.16.3)

project(webscraper)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

find_package(CURL REQUIRED)
find_package(LibXml2 REQUIRED)
find_package(sqlite3 REQUIRED)


#set (LIB_DIR_LOCATION ${NAME}/lib) 
#find_library(SQLite3 NAMES sqlite3 PATHS ${LIB_DIR_LOCATION}) 
#set(INCLUDE_DIRS ${NAME}/include)  
     
#target_link_libraries(YourProject SQLite3) 
#target_include_directories(YourProject PRIVATE ${INCLUDE_DIRS})

add_executable(scrape scrape.cc)

target_include_directories(scrape PUBLIC ${CURL_INCLUDE_DIRS})
target_include_directories(scrape PUBLIC ${LIBXML2_INCLUDE_DIR})

target_link_libraries(scrape ${CURL_LIBRARIES})
target_link_libraries(scrape ${LIBXML2_LIBRARIES})

