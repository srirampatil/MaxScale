add_library(bgdfilter SHARED bgdfilter.c bgdrw.c bgdutils.c)
set_target_properties(bgdfilter PROPERTIES INSTALL_RPATH ${CMAKE_INSTALL_RPATH}:${CMAKE_INSTALL_PREFIX}/lib)
target_link_libraries(bgdfilter query_classifier log_manager utils xml2 json-c)
install(TARGETS bgdfilter DESTINATION modules)
