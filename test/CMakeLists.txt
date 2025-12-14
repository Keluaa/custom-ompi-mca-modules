
# Download doctest
include(FetchContent)
FetchContent_Declare(doctest
    GIT_REPOSITORY https://github.com/doctest/doctest.git
    GIT_TAG        v2.4.12
)
FetchContent_MakeAvailable(doctest)
FetchContent_GetProperties(doctest)
set(DOCTEST_PATH "${doctest_SOURCE_DIR}")

add_subdirectory(part_p2p)
