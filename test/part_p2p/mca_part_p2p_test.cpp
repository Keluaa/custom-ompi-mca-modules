#include <vector>
#include <string_view>
#include <omp.h>

#include "../open_mpi_doctest_utils.h"


MPI_TEST_CASE("MCA Part p2p loaded", 1) {
    std::string_view selected_part_module_name(mca_part_base_selected_component.partm_version.mca_component_name);
    CHECK_EQ(selected_part_module_name, "p2p");
}


MPI_TEST_CASE("MCA Part p2p init", 2) {
    REQUIRE_EQ(ompi_part_p2p_module.module_in_use, 0);
    MPI_CHECK_RES(MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN));
    MPI_CHECK_RES(MPI_Comm_set_errhandler(test_comm, MPI_ERRORS_RETURN));

    int data = 0;
    MPI_Request request;
    if (test_rank == 0) {
        MPI_CHECK_RES(MPI_Psend_init(&data, 1, 1, MPI_INT, 1, 0, test_comm, MPI_INFO_NULL, &request));
    } else {
        MPI_CHECK_RES(MPI_Precv_init(&data, 1, 1, MPI_INT, 0, 0, test_comm, MPI_INFO_NULL, &request));
    }

    // Calling the MPI_Psend_init/MPI_Precv_init triggered the module's initialization.
    REQUIRE_EQ(ompi_part_p2p_module.module_in_use, 1);

    // Since we are not going to start the requests, we progress MPI's state manually with 'opal_progress',
    // until the module is initialized.
    progress_until_condition_or_timeout(2, [] {
        return ompi_part_p2p_module.module_in_use == 2;
    });
    REQUIRE_EQ(ompi_part_p2p_module.module_in_use, 2);
    CHECK_EQ(ompi_part_p2p_module.live_requests.opal_list_length, 1);
    CHECK_EQ(ompi_part_p2p_module.part_comm_init, MPI_REQUEST_NULL);
    CHECK_NE(ompi_part_p2p_module.part_comm, MPI_COMM_NULL);

    // Check if the request is correctly initialized
    auto* internal_req = reinterpret_cast<mca_part_p2p_request_t*>(request);
    progress_until_condition_or_timeout(2, [&] {
        return MCA_PART_P2P_INIT_HANDSHAKE_FLAG == internal_req->init_state;
    });
    REQUIRE_EQ(internal_req->init_state, MCA_PART_P2P_INIT_HANDSHAKE_FLAG);
    CHECK_EQ(internal_req->init_req, MPI_REQUEST_NULL);
    CHECK_EQ(internal_req->super.req_type, OMPI_REQUEST_PART);
    MPI_CHECK(0, internal_req->type == MCA_PART_P2P_REQUEST_SEND);
    MPI_CHECK(1, internal_req->type == MCA_PART_P2P_REQUEST_RECV);
    CHECK_EQ(internal_req->peer_rank, test_rank ^ 1);
    CHECK_EQ(internal_req->meta.first_part_tag, 0);
    CHECK_EQ(internal_req->meta.partition_count, 1);
    CHECK_EQ(internal_req->to_delete, 0);

    // Status is set immediately after initialization
    MPI_CHECK(0, internal_req->super.req_status.MPI_SOURCE == 0);
    MPI_CHECK(1, internal_req->super.req_status.MPI_SOURCE == 0);
    MPI_CHECK(0, internal_req->super.req_status.MPI_TAG == 0);
    MPI_CHECK(1, internal_req->super.req_status.MPI_TAG == 0);

    MPI_CHECK_RES(MPI_Request_free(&request));
    CHECK_EQ(request, MPI_REQUEST_NULL);
}


MPI_TEST_CASE("Basic send/recv", 2) {
    MPI_CHECK_RES(MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN));
    MPI_CHECK_RES(MPI_Comm_set_errhandler(test_comm, MPI_ERRORS_RETURN));

    constexpr size_t N = 1000;
    constexpr size_t P = 5;
    std::vector<int> buffer(N*P, 0);

    int flag = 0;
    MPI_Request request;
    if (test_rank == 0) {
        MPI_CHECK_RES(MPI_Psend_init(buffer.data(), P, N, MPI_INT, 1, 0, test_comm, MPI_INFO_NULL, &request));
        for (int i = 0; i < buffer.size(); i++) {
            buffer[i] = i;
        }
    } else {
        MPI_CHECK_RES(MPI_Precv_init(buffer.data(), P, N, MPI_INT, 0, 0, test_comm, MPI_INFO_NULL, &request));
        MPI_CHECK_RES(MPI_Parrived(request, 0, &flag));
        CHECK(flag);
    }

    MPI_CHECK_RES(MPI_Start(&request));
    CHECK_EQ(MPI_Start(&request), MPI_ERR_REQUEST);

    if (test_rank == 0) {
        CHECK_EQ(MPI_Parrived(request, 0, &flag), MPI_ERR_REQUEST);
        CHECK_EQ(MPI_Pready(P, request), MPI_ERR_ARG);
        for (int p = 0; p < P; ++p) {
            MPI_CHECK_RES(MPI_Pready(p, request));
        }
        MPI_CHECK_RES(MPI_Wait(&request, MPI_STATUS_IGNORE));
    } else {
        CHECK_EQ(MPI_Pready(0, request), MPI_ERR_REQUEST);
        CHECK_EQ(MPI_Parrived(request, P, &flag), MPI_ERR_ARG);
        MPI_CHECK_RES(MPI_Wait(&request, MPI_STATUS_IGNORE));
        for (int i = 0; i < N; i++) {
            if (buffer[i] != i) {
                CHECK_EQ(buffer[i], i);
            }
        }
    }

    MPI_CHECK_RES(MPI_Request_free(&request));
}


MPI_TEST_CASE("different comm", 2) {
    // Build a simple communicator where ranks don't exactly match with those in MPI_COMM_WORLD
    MPI_Comm zoink_comm;
    MPI_CHECK_RES(MPI_Comm_split(test_comm, 0, test_rank ^ 1, &zoink_comm));

    int zoink_rank, world_rank;
    MPI_CHECK_RES(MPI_Comm_rank(zoink_comm, &zoink_rank));
    MPI_CHECK_RES(MPI_Comm_rank(MPI_COMM_WORLD, &world_rank));
    MPI_CHECK(0, (zoink_rank == 1 && test_rank != zoink_rank));
    MPI_CHECK(1, (zoink_rank == 0 && test_rank != zoink_rank));

    constexpr size_t N = 1000;
    constexpr size_t P = 5;
    std::vector<int> buffer(N*P, 0);

    MPI_Request request;
    if (zoink_rank == 0) {
        MPI_CHECK_RES(MPI_Psend_init(buffer.data(), P, N, MPI_INT, 1, 0, zoink_comm, MPI_INFO_NULL, &request));
    } else {
        MPI_CHECK_RES(MPI_Precv_init(buffer.data(), P, N, MPI_INT, 0, 0, zoink_comm, MPI_INFO_NULL, &request));
    }

    // Immediately after MPI_P****_init, we know exactly to which rank we are talking to in MPI_COMM_WORLD
    auto* internal_req = reinterpret_cast<mca_part_p2p_request_t*>(request);
    CHECK_EQ(internal_req->peer_rank, world_rank ^ 1);

    MPI_CHECK_RES(MPI_Request_free(&request));
    MPI_CHECK_RES(MPI_Comm_free(&zoink_comm));
}


MPI_TEST_CASE("exchange", 2) {
    MPI_CHECK_RES(MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN));
    MPI_CHECK_RES(MPI_Comm_set_errhandler(test_comm, MPI_ERRORS_RETURN));

    constexpr size_t N = 1000;
    constexpr size_t P = 5;
    std::vector<int> send_buffer(N*P, 0), recv_buffer(N*P, 0);

    int peer     = test_rank ^ 1;
    int send_tag = test_rank;
    int recv_tag = test_rank ^ 1;

    MPI_Request send_request, recv_request;
    MPI_CHECK_RES(MPI_Psend_init(send_buffer.data(), P, N, MPI_INT, peer, send_tag, test_comm, MPI_INFO_NULL, &send_request));
    MPI_CHECK_RES(MPI_Precv_init(recv_buffer.data(), P, N, MPI_INT, peer, recv_tag, test_comm, MPI_INFO_NULL, &recv_request));

    int v = send_buffer.size() * test_rank;
    for (int i = 0; i < send_buffer.size(); i++) {
        send_buffer[i] = i + v;
    }

    MPI_CHECK_RES(MPI_Start(&send_request));
    MPI_CHECK_RES(MPI_Start(&recv_request));

    for (int p = 0; p < P; ++p) {
        MPI_CHECK_RES(MPI_Pready(p, send_request));
    }

    // Busy wait for 1 second max for all partitions to arrive
    test_until_condition_or_abort(1, [&](int* err) {
        int all_arrived = true;
        for (int p = 0; p < P && all_arrived && MPI_SUCCESS != *err; ++p) {
            *err = MPI_Parrived(recv_request, p, &all_arrived);
        }
        return all_arrived;
    }, "exchange all arrived", { send_request, recv_request });

    MPI_CHECK_RES(MPI_Wait(&send_request, MPI_STATUS_IGNORE));
    MPI_CHECK_RES(MPI_Wait(&recv_request, MPI_STATUS_IGNORE));

    MPI_CHECK_RES(MPI_Request_free(&send_request));
    MPI_CHECK_RES(MPI_Request_free(&recv_request));
}


MPI_TEST_CASE("self-exchange", 1) {
    MPI_CHECK_RES(MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN));
    MPI_CHECK_RES(MPI_Comm_set_errhandler(test_comm, MPI_ERRORS_RETURN));

    constexpr size_t N = 1000;
    constexpr size_t P = 5;
    std::vector<int> send_buffer(N*P, 0), recv_buffer(N*P, 0);

    // Same test as "exchange", but with the same rank
    int peer = test_rank;

    MPI_Request send_request, recv_request;
    MPI_CHECK_RES(MPI_Psend_init(send_buffer.data(), P, N, MPI_INT, peer, 0, test_comm, MPI_INFO_NULL, &send_request));
    MPI_CHECK_RES(MPI_Precv_init(recv_buffer.data(), P, N, MPI_INT, peer, 0, test_comm, MPI_INFO_NULL, &recv_request));

    int v = send_buffer.size() * test_rank;
    for (int i = 0; i < send_buffer.size(); i++) {
        send_buffer[i] = i + v;
    }

    MPI_CHECK_RES(MPI_Start(&send_request));
    MPI_CHECK_RES(MPI_Start(&recv_request));

    for (int p = 0; p < P; ++p) {
        MPI_CHECK_RES(MPI_Pready(p, send_request));
    }

    // Busy wait for 1 second max for all partitions to arrive
    test_until_condition_or_abort(1, [&](int* err) {
        int all_arrived = true;
        for (int p = 0; p < P && all_arrived && MPI_SUCCESS == *err; ++p) {
            *err = MPI_Parrived(recv_request, p, &all_arrived);
        }
        return all_arrived;
    }, "self-exchange all arrived", { send_request, recv_request });

    MPI_CHECK_RES(MPI_Wait(&send_request, MPI_STATUS_IGNORE));
    MPI_CHECK_RES(MPI_Wait(&recv_request, MPI_STATUS_IGNORE));

    MPI_CHECK_RES(MPI_Request_free(&send_request));
    MPI_CHECK_RES(MPI_Request_free(&recv_request));
}


MPI_TEST_CASE("parallel partitions", 2) {
    MPI_CHECK_RES(MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN));
    MPI_CHECK_RES(MPI_Comm_set_errhandler(test_comm, MPI_ERRORS_RETURN));

    const int num_threads = omp_get_max_threads();
    MESSAGE("using ", num_threads, " OpenMP threads");

    const size_t N = 1000;
    const size_t P = num_threads;
    std::vector<int> send_buffer(N*P, 0), recv_buffer(N*P, 0);

    int peer     = test_rank ^ 1;
    int send_tag = test_rank;
    int recv_tag = test_rank ^ 1;

    MPI_Request send_request, recv_request;
    MPI_CHECK_RES(MPI_Psend_init(send_buffer.data(), P, N, MPI_INT, peer, send_tag, test_comm, MPI_INFO_NULL, &send_request));
    MPI_CHECK_RES(MPI_Precv_init(recv_buffer.data(), P, N, MPI_INT, peer, recv_tag, test_comm, MPI_INFO_NULL, &recv_request));

    int v = send_buffer.size() * test_rank;
    for (int i = 0; i < send_buffer.size(); i++) {
        send_buffer[i] = i + v;
    }

    MPI_CHECK_RES(MPI_Start(&send_request));
    MPI_CHECK_RES(MPI_Start(&recv_request));

    // This might be unsupported by some compilers, but this is the easiest way of doing it
#pragma omp taskloop shared(send_request, recv_request, P) default(none)
    for (int p = 0; p < P; ++p) {
        MPI_CHECK_RES(MPI_Pready(p, send_request));

        int arrived = false;
        int err = MPI_SUCCESS;
        do {
            err = MPI_Parrived(recv_request, p, &arrived);
            if (!arrived) {
                #pragma omp taskyield
            }
        } while (!arrived && err == MPI_SUCCESS);
        MPI_CHECK_RES(err);
    }

    MPI_CHECK_RES(MPI_Wait(&send_request, MPI_STATUS_IGNORE));
    MPI_CHECK_RES(MPI_Wait(&recv_request, MPI_STATUS_IGNORE));

    MPI_CHECK_RES(MPI_Request_free(&send_request));
    MPI_CHECK_RES(MPI_Request_free(&recv_request));
}


MPI_TEST_CASE("parallel exchanges", 2) {
    MPI_CHECK_RES(MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN));
    MPI_CHECK_RES(MPI_Comm_set_errhandler(test_comm, MPI_ERRORS_RETURN));

    char label_arrived[200];
    char label_wait[200];
    int cycle = 0;

start_cycle:
    snprintf(label_arrived, 200, "mt-exchange all arrived (%d)", cycle);
    snprintf(label_wait, 200, "mt-exchange test (%d)", cycle);

#pragma omp parallel
    {
        int thread_id = omp_get_thread_num();

        const size_t N = 1000;
        const size_t P = 5;
        std::vector<int> send_buffer(N*P, 0), recv_buffer(N*P, 0);

        int comm_size = 0;
        int peer = test_rank ^ 1;
        MPI_CHECK_RES(MPI_Comm_size(MPI_COMM_WORLD, &comm_size));

        int send_tag = thread_id * comm_size + test_rank;
        int recv_tag = thread_id * comm_size + test_rank ^ 1;

        MPI_Request send_request, recv_request;
        MPI_CHECK_RES(MPI_Psend_init(send_buffer.data(), P, N, MPI_INT, peer, send_tag, test_comm, MPI_INFO_NULL, &send_request));
        MPI_CHECK_RES(MPI_Precv_init(recv_buffer.data(), P, N, MPI_INT, peer, recv_tag, test_comm, MPI_INFO_NULL, &recv_request));

        for (int rep = 1; rep <= 10; ++rep) {
            int v = send_buffer.size() * send_tag + cycle * rep;
            for (int i = 0; i < send_buffer.size(); i++) {
                send_buffer[i] = i + v;
            }

            MPI_CHECK_RES(MPI_Start(&send_request));
            MPI_CHECK_RES(MPI_Start(&recv_request));

            for (int p = 0; p < P; ++p) {
                MPI_CHECK_RES(MPI_Pready(p, send_request));
            }

            // Busy wait for 1 second max for all partitions to arrive
            test_until_condition_or_abort(1, [&](int* err) {
                int all_arrived = true;
                for (int p = 0; p < P && all_arrived && MPI_SUCCESS == *err; ++p) {
                    *err = MPI_Parrived(recv_request, p, &all_arrived);
                }
                return all_arrived;
            }, label_arrived, { send_request, recv_request });

            test_until_condition_or_abort(1, [&](int* err) {
                int done = false;
                *err = MPI_Test(&send_request, &done, MPI_STATUS_IGNORE);
                if (MPI_SUCCESS != *err || !done) return done;
                *err = MPI_Test(&recv_request, &done, MPI_STATUS_IGNORE);
                return done;
            }, label_wait, { send_request, recv_request });

            bool ok = true;
            v = recv_buffer.size() * recv_tag + cycle * rep;
            for (int i = 0; i < recv_buffer.size(); i++) {
                ok &= recv_buffer[i] == i + v;
            }
            CHECK(ok);
        }

        MPI_CHECK_RES(MPI_Request_free(&send_request));
        MPI_CHECK_RES(MPI_Request_free(&recv_request));
    }

    cycle++;
    if (cycle < 100) goto start_cycle;
}
