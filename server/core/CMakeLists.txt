if(BUILD_TESTS OR BUILD_TOOLS)
  add_library(fullcore STATIC atomic.c buffer.c spinlock.c secrets.c utils.c
	gw_utils.c utils.c dcb.c load_utils.c session.c service.c server.c 
	poll.c config.c users.c hashtable.c dbusers.c thread.c gwbitmask.c 
	monitor.c adminusers.c secrets.c filter.c modutil.c hint.c array.c
	housekeeper.c memlog.c resultset.c sescmd/sescmd.c sescmd/sescmd_cursor.c
    sescmd/sescmd_list.c)
  if(WITH_JEMALLOC)
    target_link_libraries(fullcore ${JEMALLOC_LIBRARIES})
  elseif(WITH_TCMALLOC)
    target_link_libraries(fullcore ${TCMALLOC_LIBRARIES})
  endif()
  target_link_libraries(fullcore ${CURL_LIBRARIES} log_manager utils pthread ${EMBEDDED_LIB} ${PCRE_LINK_FLAGS} ssl aio rt crypt dl crypto inih z m stdc++)
endif()

# This could be moved to modules/routing/common directory
# and instead of linking it to MaxScale, link it to the module
set(SCMD_SRC "${CMAKE_CURRENT_SOURCE_DIR}/sescmd/sescmd.c ${CMAKE_CURRENT_SOURCE_DIR}/sescmd/sescmd_list.c ${CMAKE_CURRENT_SOURCE_DIR}/sescmd/sescmd_cursor.c"
  CACHE INTERNAL "Path to session command sources")

add_executable(maxscale atomic.c buffer.c spinlock.c gateway.c array.c
	gw_utils.c utils.c dcb.c load_utils.c session.c service.c server.c 
	poll.c config.c users.c hashtable.c dbusers.c thread.c gwbitmask.c 
	monitor.c adminusers.c secrets.c filter.c modutil.c hint.c
	housekeeper.c memlog.c resultset.c sescmd/sescmd.c sescmd/sescmd_list.c sescmd/sescmd_cursor.c)

if(WITH_JEMALLOC)
  target_link_libraries(maxscale ${JEMALLOC_LIBRARIES})
elseif(WITH_TCMALLOC)
  target_link_libraries(maxscale ${TCMALLOC_LIBRARIES})
endif()

target_link_libraries(maxscale ${EMBEDDED_LIB} ${PCRE_LINK_FLAGS} ${CURL_LIBRARIES} log_manager utils ssl aio pthread crypt dl crypto inih z rt m stdc++)

install(TARGETS maxscale DESTINATION bin)

add_executable(maxkeys maxkeys.c secrets.c utils.c)
target_link_libraries(maxkeys log_manager utils pthread  crypt crypto)
install(TARGETS maxkeys DESTINATION bin)

add_executable(maxpasswd maxpasswd.c secrets.c utils.c)
target_link_libraries(maxpasswd log_manager utils pthread crypt crypto)
install(TARGETS maxpasswd DESTINATION bin)

if(BUILD_TESTS)
  add_subdirectory(test)
endif()
