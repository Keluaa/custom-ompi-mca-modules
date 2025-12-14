
find_package(OpenMP REQUIRED)

add_executable(mca_part_p2p_test
    ../doctest_main.cpp
    ../wait_for_debugger.h
    mca_part_p2p_test.cpp
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
