#include "ompi/communicator/communicator.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/part/base/base.h"

#include "part_p2p_request.h"
#include "part_p2p.h"


static int mca_part_p2p_init_module(void)
{
    if (0 != OPAL_THREAD_TRYLOCK(&ompi_part_p2p_module.lock)) {
        return OMPI_SUCCESS;
    }

    opal_output_verbose(50, ompi_part_base_framework.framework_output, "start of part p2p module initialization");

    // TODO: do we need to free this communicator in 'mca_part_p2p_component_close'? (and the request as well?)
    int err = ompi_comm_idup(
        &ompi_mpi_comm_world.comm,
        &ompi_part_p2p_module.part_comm,
        &ompi_part_p2p_module.part_comm_init
    );

    ompi_part_p2p_module.module_in_use = 1;
    OPAL_THREAD_UNLOCK(&ompi_part_p2p_module.lock);
    return err;
}


static int mca_part_p2p_complete_module_init(void)
{
    /* 'ompi_part_p2p_module.lock' must be acquired beforehand */
    int completed = 0;
    int err = ompi_request_test(&ompi_part_p2p_module.part_comm_init, &completed, MPI_STATUS_IGNORE);
    if (completed) {
        ompi_part_p2p_module.module_in_use = 2;
        opal_output_verbose(50, ompi_part_base_framework.framework_output, "completed part p2p module initialization");
    }
    return err;
}


static int mca_part_p2p_complete_request_init(mca_part_p2p_request_t* request)
{
    int completed = 0;
    int err = OMPI_SUCCESS;

    err = ompi_request_test(&request->init_send, &completed, MPI_STATUS_IGNORE);
    if (OMPI_SUCCESS != err || 0 == completed) {
        return err;
    }

    MPI_Status status;
    err = ompi_request_test(&request->init_recv, &completed, &status);
    if (OMPI_SUCCESS != err || 0 == completed) {
        return err;
    }

    if (MCA_PART_P2P_REQUEST_SEND == request->type) {
        request->meta.peer_rank = request->tmp_peer_rank;
    } else {
        /* We now know the exact source and tag, if MPI_ANY_SOURCE or MPI_ANY_TAG were passed */
        request->super.req_status.MPI_SOURCE = status.MPI_SOURCE;
        request->super.req_status.MPI_TAG = status.MPI_TAG;
    }

    /* Now 'request->meta' is synchronized between both processes */
    size_t parts = request->meta.partition_count;
    if (MCA_PART_P2P_REQUEST_RECV == request->type) {
        /* This array is already allocated and initialized on the send side */
        request->partition_states = malloc(parts * sizeof(mca_part_p2p_partition_state_t));
    }
    request->partition_requests = malloc(parts * sizeof(ompi_request_t*));
    if (NULL == request->partition_states || NULL == request->partition_requests) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    size_t partition_bytes;
    err = ompi_datatype_type_size(request->datatype, &partition_bytes);
    if (OMPI_SUCCESS != err) { return err; }
    partition_bytes *= request->partition_size;

    /* Initiate the persistent requests for each partition */
    for (size_t p = 0; p < parts; p++) {
        void* partition_data = ((char*) request->user_data) + partition_bytes * p;
        ompi_request_t* part_request;
        if (MCA_PART_P2P_REQUEST_SEND == request->type) {
            err = MCA_PML_CALL(isend_init(
                partition_data, request->partition_size, request->datatype,
                request->meta.peer_rank, request->meta.first_part_tag + p, MCA_PML_BASE_SEND_STANDARD,
                ompi_part_p2p_module.part_comm, &part_request
            ));
            if (OMPI_SUCCESS != err) { return err; }
        } else {
            err = MCA_PML_CALL(irecv_init(
                partition_data, request->partition_size, request->datatype,
                request->meta.peer_rank, request->meta.first_part_tag + p,
                ompi_part_p2p_module.part_comm, &part_request
            ));
            if (OMPI_SUCCESS != err) { return err; }
        }
        request->partition_requests[p] = part_request;
    }

    opal_output_verbose(50, ompi_part_base_framework.framework_output, "initialized request %p with rank %d", request, request->meta.peer_rank);

    request->is_initialized = 1;

    if (MCA_PART_P2P_REQUEST_RECV == request->type) {
        /* We handle the edge case where MPI_Start is called before initialization here, but only for receive requests.
         * As send requests mark partitions at any time, this must be handled in the progress loop. */
        bool can_start = OMPI_REQUEST_ACTIVE == request->super.req_state;
        if (!can_start) {
            int32_t expected = 1;
            can_start = OPAL_ATOMIC_COMPARE_EXCHANGE_STRONG_32(&request->is_initialized, &expected, 2);
        }

        if (can_start) {
            for (size_t p = 0; p < parts; p++) {
                ompi_request_t* part_request = request->partition_requests[p];
                err = part_request->req_start(1, &part_request);
                if (OMPI_SUCCESS != err) { return err; }
                request->partition_states[p] = MCA_PART_P2P_PARTITION_STARTED;
            }
            opal_output_verbose(50, ompi_part_base_framework.framework_output, "started partitions of request %p after init", request);
        }
    }

    return err;
}


static void mca_part_p2p_request_complete(mca_part_p2p_request_t* request)
{
    opal_output_verbose(50, ompi_part_base_framework.framework_output, "request %p completed", request);
    /* 'req_status._ucount' is set at request allocation,
     * while MPI_SOURCE and MPI_TAG are set after initialization. */
    request->super.req_status.MPI_ERROR = OMPI_SUCCESS;
    ompi_request_complete(&request->super, true);
}


static int mca_part_p2p_progress(void)
{
    int err = OMPI_SUCCESS;
    int progress = 0;

    if (OPAL_THREAD_TRYLOCK(&ompi_part_p2p_module.lock)) {
        return err;
    }

    if (ompi_part_p2p_module.module_in_use == 1) {
        err = mca_part_p2p_complete_module_init();
        if (OMPI_SUCCESS != err) { goto exit_progress; }
        if (ompi_part_p2p_module.module_in_use == 2) {
            progress++;
        }
    }
    if (ompi_part_p2p_module.module_in_use != 2) {
        /* We can only send/recv partitions when the communicator is initialized */
        goto exit_progress;
    }

    mca_part_p2p_request_list_item_t *current, *next;
    OPAL_LIST_FOREACH_SAFE(current, next, ompi_part_p2p_module.live_requests, mca_part_p2p_request_list_item_t) {
        mca_part_p2p_request_t* req = current->request;

        if (0 == req->is_initialized) {
            err = mca_part_p2p_complete_request_init(req);
            if (OMPI_SUCCESS != err) { goto exit_progress; }
            if (0 != req->is_initialized) {
                progress++;
            }
            continue;
        }

        if (true == req->to_delete) {
            mca_part_p2p_request_free(req);
            opal_list_remove_item(ompi_part_p2p_module.live_requests, (opal_list_item_t*) current);
            progress++;
            continue;
        }

        if (REQUEST_COMPLETED != req->super.req_complete && OMPI_REQUEST_ACTIVE == req->super.req_state) {
            size_t done_count = 0;
            bool is_send = MCA_PART_P2P_REQUEST_SEND == req->type;
            for (size_t p = 0; p < req->meta.partition_count; p++) {
                mca_part_p2p_partition_state_t part_state = req->partition_states[p];

                if (is_send && MCA_PART_P2P_PARTITION_INACTIVE == part_state && true == req->partition_ready_flags[p]) {
                    /* MPI_Pready called before request initialization */
                    ompi_request_t* part_req = req->partition_requests[p];
                    err = part_req->req_start(1, &part_req);
                    if (OMPI_SUCCESS != err) { goto exit_progress; }
                    part_state = MCA_PART_P2P_PARTITION_STARTED;
                    req->partition_states[p] = part_state;
                    progress++;
                }

                if (MCA_PART_P2P_PARTITION_STARTED == part_state) {
                    int done = false;
                    err = ompi_request_test((ompi_request_t**) &req->partition_requests[p], &done, MPI_STATUS_IGNORE);
                    if (OMPI_SUCCESS != err) { goto exit_progress; }
                    if (done) {
                        part_state = MCA_PART_P2P_PARTITION_COMPLETE;
                        req->partition_states[p] = part_state;
                        progress++;
                    }
                }

                done_count += MCA_PART_P2P_PARTITION_COMPLETE == part_state;
            }

            if (req->meta.partition_count == done_count) {
                mca_part_p2p_request_complete(req);
                progress++;
            }
        }
    }

exit_progress:
    if (OMPI_SUCCESS != err) {
        ompi_rte_abort(err, "part p2p internal failure");
    }
    OPAL_THREAD_UNLOCK(&ompi_part_p2p_module.lock);
    return progress;
}


static int mca_part_p2p_psend_init(
    const void* buf, size_t parts, size_t count,
    ompi_datatype_t* datatype, int dst, int tag,
    ompi_communicator_t* comm, ompi_info_t* info,
    ompi_request_t** request)
{
    int err = OMPI_SUCCESS;

    /* initialize the module if needed */
    if (0 == ompi_part_p2p_module.module_in_use) {
        err = mca_part_p2p_init_module();
        if (OMPI_SUCCESS != err) {
            return err;
        }
    }

    mca_part_p2p_request_t* req = mca_part_p2p_request_alloc_send();
    if (OPAL_UNLIKELY(NULL == req)) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }
    mca_part_p2p_request_init(req, buf, parts, count, datatype, dst, tag, comm);

    size_t first_part_tag = opal_atomic_fetch_add_size_t(&ompi_part_p2p_module.next_tag, parts);
    size_t last_part_tag = first_part_tag + count;
    if (first_part_tag > INT_MAX || last_part_tag > INT_MAX) {
        /* int overflow: too many partitions */
        return OMPI_ERR_BAD_PARAM;
    }

    // TODO: the MPI spec requires that a MPI_Psend_init can only be matched with a MPI_Precv_init
    /* It is the send side which dictates the number of partitions requests and their tags. */
    req->meta.peer_rank = ompi_mpi_comm_world.comm.c_my_rank;
    req->meta.first_part_tag = (int) first_part_tag;
    req->meta.partition_count = parts;
    err = MCA_PML_CALL(isend(
        &req->meta, sizeof(mca_part_p2p_request_meta_t), MPI_BYTE,
        dst, tag, MCA_PML_BASE_SEND_STANDARD, comm,
        &req->init_send
    ));
    if (OMPI_SUCCESS != err) { return err; }

    /* We are only interested in the receiving process' rank */
    err = MCA_PML_CALL(irecv(
        &req->tmp_peer_rank, 1, MPI_INT,
        dst, tag, comm,
        &req->init_recv
    ));
    if (OMPI_SUCCESS != err) { return err; }

    /* Those arrays need to be available early, to allow 'MPI_Start' to use them */
    req->partition_states = malloc(parts * sizeof(mca_part_p2p_partition_state_t));
    req->partition_ready_flags = calloc(parts, sizeof(opal_atomic_int32_t));
    if (NULL == req->partition_states || NULL == req->partition_ready_flags) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    for (size_t i = 0; i < parts; i++) {
        req->partition_states[i] = MCA_PART_P2P_PARTITION_INACTIVE;
    }

    /* Add the request to the list of requests to progress */
    mca_part_p2p_request_list_item_t* item = OBJ_NEW(mca_part_p2p_request_list_item_t);
    item->request = req;
    OPAL_THREAD_LOCK(&ompi_part_p2p_module.lock);
    opal_list_append(ompi_part_p2p_module.live_requests, (opal_list_item_t*) item);
    OPAL_THREAD_UNLOCK(&ompi_part_p2p_module.lock);

    opal_output_verbose(50, ompi_part_base_framework.framework_output, "created new psend request %p", req);

    *request = (ompi_request_t*) req;
    return err;
}


static int mca_part_p2p_precv_init(
    void* buf, size_t parts, size_t count, ompi_datatype_t* datatype,
    int src, int tag, ompi_communicator_t* comm, ompi_info_t* info,
    ompi_request_t** request)
{
    int err = OMPI_SUCCESS;

    /* initialize the module if needed */
    if (0 == ompi_part_p2p_module.module_in_use) {
        err = mca_part_p2p_init_module();
        if (OMPI_SUCCESS != err) {
            return err;
        }
    }

    mca_part_p2p_request_t* req = mca_part_p2p_request_alloc_recv();
    if (OPAL_UNLIKELY(NULL == req)) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }
    mca_part_p2p_request_init(req, buf, parts, count, datatype, src, tag, comm);

    /* Send our global rank to the send side */
    req->tmp_peer_rank = ompi_mpi_comm_world.comm.c_my_rank;
    err = MCA_PML_CALL(isend(
        &req->tmp_peer_rank, 1, MPI_INT,
        src, tag, MCA_PML_BASE_SEND_STANDARD, comm,
        &req->init_send
    ));
    if (OMPI_SUCCESS != err) { return err; }

    /* Receive the partitions configuration */
    err = MCA_PML_CALL(irecv(
        &req->meta, sizeof(mca_part_p2p_request_meta_t), MPI_BYTE,
        src, tag, comm,
        &req->init_recv
    ));
    if (OMPI_SUCCESS != err) { return err; }

    /* Add the request to the list of requests to progress */
    mca_part_p2p_request_list_item_t* item = OBJ_NEW(mca_part_p2p_request_list_item_t);
    item->request = req;
    OPAL_THREAD_LOCK(&ompi_part_p2p_module.lock);
    opal_list_append(ompi_part_p2p_module.live_requests, (opal_list_item_t*) item);
    OPAL_THREAD_UNLOCK(&ompi_part_p2p_module.lock);

    opal_output_verbose(50, ompi_part_base_framework.framework_output, "created new precv request %p", req);

    *request = (ompi_request_t*) req;
    return err;
}


static int mca_part_p2p_start(size_t count, ompi_request_t** requests)
{
    for (size_t i = 0; i < count; i++) {
        mca_part_p2p_request_t* req = (mca_part_p2p_request_t*) requests[i];
        if (NULL == req || OMPI_REQUEST_PART != requests[i]->req_type) {
            continue;
        }

        if (REQUEST_COMPLETED != req->super.req_complete) {
            return OMPI_ERR_REQUEST;
        }

        switch (req->type) {
        case MCA_PART_P2P_REQUEST_SEND: {
            req->super.req_state = OMPI_REQUEST_ACTIVE;
            req->super.req_complete = REQUEST_PENDING;
            break;
        }
        case MCA_PART_P2P_REQUEST_RECV: {
            bool can_start = req->is_initialized == 2;
            if (!can_start) {
                int32_t expected = 1;
                can_start = OPAL_ATOMIC_COMPARE_EXCHANGE_STRONG_32(&req->is_initialized, &expected, 2);
            }

            if (can_start) {
                for (size_t p = 0; p < req->meta.partition_count; p++) {
                    int err = req->partition_requests[i]->req_start(1, &req->partition_requests[i]);
                    if (OMPI_SUCCESS != err) { return err; }
                    req->partition_states[i] = MCA_PART_P2P_PARTITION_STARTED;
                }
                opal_output_verbose(50, ompi_part_base_framework.framework_output, "started partitions of request %p", req);
            } else {
                /* We don't know the number of partition requests yet, so 'req->partition_ready_flags' cannot be used.
                 * Instead, the requests will be started immediately after initialization. */
            }

            req->super.req_complete = REQUEST_PENDING;
            req->super.req_state = OMPI_REQUEST_ACTIVE;
            break;
        }
        default:
            return OMPI_ERR_REQUEST;
        }
        opal_output_verbose(50, ompi_part_base_framework.framework_output, "started request %p", req);
    }
    return OMPI_SUCCESS;
}


static int mca_part_p2p_pready(size_t min_part, size_t max_part, ompi_request_t* request)
{
    int err = OMPI_SUCCESS;
    mca_part_p2p_request_t* req = (mca_part_p2p_request_t*) request;
    if (MCA_PART_P2P_REQUEST_SEND != req->type) {
        err = OMPI_ERR_REQUEST;
    } else if (min_part > max_part || max_part >= req->user_partition_count) {
        err = OMPI_ERR_BAD_PARAM;
    } else {
        /* On the send side, the number of partition requests matches the user's partition count,
         * so we can use 'min_part' and 'max_part' directly. */
        if (0 != req->is_initialized) {
            ompi_request_t* first_part_req = req->partition_requests[min_part];
            err = first_part_req->req_start(max_part - min_part + 1, &first_part_req);
            for (size_t p = min_part; p <= max_part; p++) {
                req->partition_states[p] = MCA_PART_P2P_PARTITION_STARTED;
            }
        }
        for (size_t p = min_part; p <= max_part; p++) {
            req->partition_ready_flags[p] = true;
        }
    }
    return err;
}


static int mca_part_p2p_parrived(size_t min_part, size_t max_part, int* flag, ompi_request_t* request)
{
    int err = OMPI_SUCCESS;
    bool has_arrived = false;
    mca_part_p2p_request_t* req = (mca_part_p2p_request_t*) request;
    if (MCA_PART_P2P_REQUEST_RECV != req->type) {
        err = OMPI_ERR_REQUEST;
    } else if (min_part > max_part || max_part >= req->user_partition_count) {
        err = OMPI_ERR_BAD_PARAM;
    } else if (OMPI_REQUEST_INACTIVE == req->super.req_state) {
        has_arrived = true;
    } else if (NULL == req->partition_states) {  /* cheaper than '!req->is_initialized' */
        has_arrived = false;
    } else {
        size_t min_send_part = min_part;
        size_t max_send_part = max_part;
        if (req->user_partition_count != req->meta.partition_count) {
            /* Match the receive partitions with the source */
            size_t first_elem = min_part * req->partition_size;
            size_t last_elem  = (max_part + 1) * req->partition_size - 1;
            size_t source_partition_size = req->user_partition_count * req->partition_size / req->meta.partition_count;
            min_send_part = first_elem / source_partition_size;
            max_send_part = last_elem / source_partition_size + (last_elem % source_partition_size > 0);
        }

        has_arrived = true;
        for (size_t p = min_send_part; p <= max_send_part; p++) {
            has_arrived &= MCA_PART_P2P_PARTITION_COMPLETE == req->partition_states[p];
        }
    }

    if (!has_arrived && OMPI_SUCCESS == err) {
        /* MPI_Parrived is required to progress the MPI state */
        opal_progress();
    }

    *flag = has_arrived;
    return err;
}


int mca_part_p2p_free(ompi_request_t** request)
{
    mca_part_p2p_request_t* req = (mca_part_p2p_request_t*) *request;

    if (true == req->to_delete) {
        return OMPI_ERROR;
    }
    req->to_delete = true;
    opal_output_verbose(50, ompi_part_base_framework.framework_output, "freed request %p", req);

    *request = MPI_REQUEST_NULL;
    return OMPI_SUCCESS;
}


ompi_part_p2p_module_t ompi_part_p2p_module = {
    .super = {
        .part_progress = mca_part_p2p_progress,
        .part_psend_init = mca_part_p2p_psend_init,
        .part_precv_init = mca_part_p2p_precv_init,
        .part_start = mca_part_p2p_start,
        .part_pready = mca_part_p2p_pready,
        .part_parrived = mca_part_p2p_parrived
    }
};


OBJ_CLASS_INSTANCE(mca_part_p2p_request_list_item_t, opal_list_item_t, NULL, NULL);
