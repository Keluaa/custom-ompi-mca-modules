#ifndef OMPI_CART_PLUGIN_OPEN_MPI_DOCTEST_UTILS_H
#define OMPI_CART_PLUGIN_OPEN_MPI_DOCTEST_UTILS_H

#include <vector>
#include <doctest/extensions/doctest_mpi.h>

#include "part_p2p.h"
#include "opal/runtime/opal_progress.h"
#include "ompi/mca/part/base/base.h"

#if OMPI_VERSION_MAJOR < 6
END_C_DECLS  // needed due to a duplicate BEGIN_C_DECLS in "opal/util/event.h"
#endif

#include "wait_for_debugger.h"


#define MPI_TEST_ALL_RANKS (-1)


namespace doctest {
    template<typename F>
    void execute_mpi_rank_test_case(F func, size_t nb_procs)
    {
        auto it = sub_comms_by_size.find(nb_procs);
        if (it == sub_comms_by_size.end()) {
            bool was_emplaced = false;
            std::tie(it,was_emplaced) = sub_comms_by_size.emplace(std::make_pair(nb_procs, mpi_sub_comm(nb_procs)));
            assert(was_emplaced);
        }
        const mpi_sub_comm& sub = it->second;
        if (sub.comm != MPI_COMM_NULL) {
            func(sub.rank, nb_procs, sub.comm, nb_procs);
        }
    }

    template<typename F>
    void execute_mpi_rank_list_test_case(F func, std::initializer_list<int> rank_list)
    {
        static const int world_size = mpi_comm_world_size();
        std::vector<int> done;
        for (int nb_procs : rank_list) {
            nb_procs = nb_procs == MPI_TEST_ALL_RANKS ? world_size : nb_procs;
            if (std::find(done.begin(), done.end(), nb_procs) == done.end()) continue;
            execute_mpi_rank_test_case(func, nb_procs);
            done.push_back(nb_procs);
        }
    }

    inline bool insufficient_procs(std::initializer_list<int> rank_list)
    {
        static const int world_size = mpi_comm_world_size();
        int min_procs = world_size + 1;
        for (int nb_procs : rank_list) {
            nb_procs = nb_procs == MPI_TEST_ALL_RANKS ? world_size : nb_procs;
            min_procs = std::min(nb_procs, min_procs);
        }
        bool insufficient = min_procs > world_size;
        if (insufficient) {
            ++nb_test_cases_skipped_insufficient_procs;
        }
        return insufficient;
    }

} // doctest


#define DOCTEST_CREATE_MPI_RANK_LIST_TEST_CASE(name, func, ...) \
    static void func(DOCTEST_UNUSED int test_rank, DOCTEST_UNUSED int test_nb_procs, DOCTEST_UNUSED MPI_Comm test_comm, DOCTEST_UNUSED int nb_procs); \
    TEST_CASE(name * doctest::description("MPI_TEST_CASE") * doctest::skip(doctest::insufficient_procs({ __VA_ARGS__ }))) { \
        doctest::execute_mpi_rank_list_test_case(func, { __VA_ARGS__ }); \
    } \
    static void func(DOCTEST_UNUSED int test_rank, DOCTEST_UNUSED int test_nb_procs, DOCTEST_UNUSED MPI_Comm test_comm, DOCTEST_UNUSED int test_nb_procs_as_int_constant)

#define DOCTEST_MPI_RANK_LIST_TEST_CASE(name, ...) \
    DOCTEST_CREATE_MPI_RANK_LIST_TEST_CASE(name, DOCTEST_ANONYMOUS(DOCTEST_MPI_FUNC), __VA_ARGS__)

#if !defined(DOCTEST_CONFIG_NO_SHORT_MACRO_NAMES)
#define MPI_RANK_LIST_TEST_CASE DOCTEST_MPI_RANK_LIST_TEST_CASE
#endif


#define MPI_CHECK_RES(expr)              \
    do {                                 \
        int res_mpi = (expr);            \
        REQUIRE(res_mpi == MPI_SUCCESS); \
    } while (false)


#define MPI_CHECK_RES_MESSAGE(expr, ...)                         \
    do {                                                         \
        int res_mpi = (expr);                                    \
        REQUIRE_MESSAGE(res_mpi == MPI_SUCCESS, ## __VA_ARGS__); \
    } while (false)


/**
 * Calls `f(&err)` until it returns `true`, `err != MPI_SUCCESS` or `seconds` have elapsed.
 * Unless `f` returned `true`, `MPI_Abort` is called after print request states.
 */
template<typename Functor>
void test_until_condition_or_abort(int seconds, Functor&& f, std::string_view label, std::initializer_list<ompi_request_t*> requests)
{
    auto start_time = clock();
    auto current_time = start_time;
    int err = MPI_SUCCESS;
    bool ok = false;
    while (!ok && MPI_SUCCESS == err && current_time - start_time < seconds * CLOCKS_PER_SEC) {
        ok = f(&err);
        current_time = clock();
    }
    if (MPI_SUCCESS != err || !ok) {
        std::string err_label = "wait loop ";
        err_label += !ok ? "timeout" : "MPI fail";
        err_label += ": ";
        err_label += label;
        for (auto& req : requests) {
            mca_part_p2p_dump_request_state(req, err_label.c_str());
        }
        if (requests.size() == 0) {
            opal_output(ompi_part_base_framework.framework_output, "%s", err_label.c_str());
        }
        sleep(2);  // Wait for the other ranks to print their own output before aborting
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
}


/**
 * Calls `opal_progress()` until `f()` returns `true` or `seconds` have elapsed.
 * Returns `true` if it timedout, `false` otherwise.
 */
template<typename Functor>
bool progress_until_condition_or_timeout(int seconds, Functor&& f)
{
    if (f()) return false;
    auto start_time = clock();
    auto current_time = start_time;
    while (current_time - start_time < seconds * CLOCKS_PER_SEC) {
        opal_progress();
        if (f()) return false;
        current_time = clock();
    }
    return true;
}

#endif //OMPI_CART_PLUGIN_OPEN_MPI_DOCTEST_UTILS_H