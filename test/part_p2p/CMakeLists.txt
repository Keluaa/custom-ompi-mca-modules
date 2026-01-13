
find_package(OpenMP REQUIRED)

add_executable(mca_part_p2p_test
    ../doctest_main.cpp
    ../wait_for_debugger.h
    mca_part_p2p_test.cpp
    ../open_mpi_doctest_utils.h
)
target_link_directories(mca_part_p2p_test PRIVATE
    ${OMPI_INSTALL_PATH}/lib
)
target_link_libraries(mca_part_p2p_test PRIVATE
    OpenMP::OpenMP_CXX
    mpi
    open-pal  # for 'opal_progress'
    ${OMPI_COMPONENTS_PATH}/$<TARGET_NAME:mca_part_p2p>.so  # to access the 'ompi_part_p2p_module'
)
target_include_directories(mca_part_p2p_test PRIVATE
    ${DOCTEST_PATH}
    ${DOCTEST_PATH}/doctest
    ${DOCTEST_PATH}/doctest/extensions
    ${OMPI_INSTALL_PATH}/include
    ${OMPI_INCLUDE_PATHS}
    ../../src/part_p2p
)

CHECK_CXX_COMPILER_FLAG("-Wno-deprecated-volatile" NO_DEPRECATED_VOLATILE)
if(NO_DEPRECATED_VOLATILE)
    # Removes an annoying warning message for Clang & Cie
    target_compile_options(mca_part_p2p_test PRIVATE
        -Wno-deprecated-volatile
    )
endif()

CHECK_CXX_COMPILER_FLAG("-Wno-volatile" NO_VOLATILE)
if(NO_VOLATILE)
    # Same with GCC
    target_compile_options(mca_part_p2p_test PRIVATE
        -Wno-volatile
    )
endif()
