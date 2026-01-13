
add_library(mca_part_p2p SHARED
    part_p2p.c
    part_p2p.h
    part_p2p_component.c
    part_p2p_component.h
    part_p2p_request.c
    part_p2p_request.h
)
set_target_properties(mca_part_p2p PROPERTIES PREFIX "")
target_include_directories(mca_part_p2p PUBLIC ${OMPI_INCLUDE_PATHS})
target_include_directories(mca_part_p2p SYSTEM PUBLIC
    ${HWLOC_INCLUDE_PATH}
    ${PMIX_INCLUDE_PATH}
)
target_link_libraries(mca_part_p2p
    atomic
)

CHECK_CXX_COMPILER_FLAG("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
if(COMPILER_SUPPORTS_MARCH_NATIVE)
    # This fixes some obscure symbol lookup error
    target_compile_options(mca_part_p2p PRIVATE
        -march=native
    )
endif()

if(NOT "${OMPI_COMPONENTS_PATH}" STREQUAL "")
    # Put the library directly in OMPI_COMPONENTS_PATH after each build
    add_custom_command(
        TARGET mca_part_p2p
        POST_BUILD
        COMMAND cmake -E copy $<TARGET_FILE:mca_part_p2p> ${OMPI_COMPONENTS_PATH}
    )
endif()
