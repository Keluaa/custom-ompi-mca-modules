cmake_minimum_required(VERSION 3.22)
project(ompi_cart_plugin)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 20)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(OMPI_SRC_PATH $ENV{OMPI_SRC_PATH} CACHE PATH "Path to OpenMPI source directory")
set(PMIX_INCLUDE_PATH $ENV{PMIX_INCLUDE_PATH} CACHE PATH "Include path to PMIX headers. Can be omitted if OpenMPI isn't built with PMIX.")
set(HWLOC_INCLUDE_PATH $ENV{HWLOC_INCLUDE_PATH} CACHE PATH "Include path to HWLOC header. Can be omitted if OpenMPI uses its packaged HWLOC.")

set(OMPI_COMPONENTS_PATH $ENV{OMPI_COMPONENTS_PATH} CACHE PATH "A path present in the search path of OpenMPI's components.")

set(OMPI_INCLUDE_PATHS
    ${OMPI_SRC_PATH}
    ${OMPI_SRC_PATH}/ompi/include
    ${OMPI_SRC_PATH}/orte/include
    ${OMPI_SRC_PATH}/opal/include
)

include(CheckCXXCompilerFlag)

add_subdirectory(src/part_p2p)

option(ENABLE_TESTS "If tests should be compiled" OFF)
if(ENABLE_TESTS)
    set(OMPI_INSTALL_PATH $ENV{OMPI_INSTALL_PATH} CACHE PATH "Installation path of OpenMPI, for tests.")
    set(MPIEXEC_EXECUTABLE "${OMPI_INSTALL_PATH}/bin/mpiexec" CACHE PATH "Path to mpiexec, for tests.")
    set(MPIEXEC_NUMPROC_FLAG "-np" CACHE STRING "Flag to specify the number of processes to launch, for tests.")
    add_subdirectory(test)
endif()
