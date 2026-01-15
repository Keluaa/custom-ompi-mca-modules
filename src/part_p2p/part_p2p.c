#include "ompi/communicator/communicator.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/part/base/base.h"

#include "part_p2p_request.h"
#include "part_p2p.h"

#include "part_p2p_component.h"


void mca_part_p2p_dump_request_state(ompi_request_t* request, const char* label)
{
    int output_id = ompi_part_base_framework.framework_output;
    if (NULL == request || OMPI_REQUEST_PART != request->req_type) {
        opal_output(output_id, "%s --- request %p is not a partitioned request\n", label, request);
        return;
    }

    mca_part_p2p_request_t* req = (mca_part_p2p_request_t*) request;
    mca_part_p2p_init_state_t init_state = req->init_state;
    int req_type = req->type;
    if (0 == init_state) {
        int init_send_done = MPI_REQUEST_NULL == req->init_req;
        opal_output(output_id, "%s --- %s request %p is not yet initialized (state=%d), send_done=%d\n",
                label, req_type == MCA_PART_P2P_REQUEST_SEND ? "send" : "recv", request, init_state, init_send_done);
        return;
    }

    int peer_rank = req->peer_rank;
    int peer_tag  = req->super.req_status.MPI_TAG;
    int first_tag = req->meta.first_part_tag;
    size_t part_count = req->meta.partition_count;
    size_t user_parts = req->user_partition_count;
    size_t last_tag   = first_tag + part_count;
    int aggregation_factor = req->aggregation_factor;

    ompi_request_t** part_reqs = req->partition_requests;
    volatile int32_t* part_ready = req->partition_states;
    int to_delete = req->to_delete;
    if (to_delete || part_reqs == NULL || part_ready == NULL) {
        opal_output(output_id, "%s --- %s request %p is scheduled for deletion\n",
                label, req_type == MCA_PART_P2P_REQUEST_SEND ? "send" : "recv", request);
        return;
    }

    const size_t msg_len = 4096;
    char msg[msg_len];
    size_t remaining_len = msg_len;
    size_t written = 0;
    char* msg_pos = msg;

    int completed = REQUEST_COMPLETED == req->super.req_complete;
    int active = OMPI_REQUEST_ACTIVE == req->super.req_state;

    written = snprintf(msg_pos, remaining_len,
        "%s --- request %p %s rank %d (tag %d) with %ld parts (%ld user, aggr_factor=%d, tags %d to %ld),"
        "active=%d, completed=%d, to_delete=%d, init=%d, parts:",
        label, request, req_type == MCA_PART_P2P_REQUEST_SEND ? "sends to" : "receives from",
        peer_rank, peer_tag, part_count, user_parts, aggregation_factor, first_tag, last_tag,
        active, completed, to_delete, init_state);
    msg_pos += written;
    remaining_len = remaining_len >= written ? remaining_len - written : 0;

    for (size_t p = 0; p < part_count && remaining_len > 0; p++) {
        ompi_request_t* part_req = part_reqs[p];
        int32_t ready_value = part_ready[p];
        mca_part_p2p_partition_state_t ready = ready_value;
        int32_t partial_completion = 0;
        if (0 > ready_value) {
            /* Number of user partitions marked as ready for this partition */
            partial_completion = -ready_value;
            ready = MCA_PART_P2P_PARTITION_INACTIVE;
        }

        completed = REQUEST_COMPLETED == req->super.req_complete;

        int state = part_req->req_state;
        const char* state_str =
            state == OMPI_REQUEST_INACTIVE  ? "INACTIVE" :
            state == OMPI_REQUEST_ACTIVE    ? "ACTIVE"   :
            state == OMPI_REQUEST_CANCELLED ? "CANCEL"   :
            state == OMPI_REQUEST_INVALID   ? "INVALID"  : "???";

        const char* part_state_str =
            partial_completion > 0                    ? "PARTIAL"   :
            ready == MCA_PART_P2P_PARTITION_INACTIVE  ? "INACTIVE"  :
            ready == MCA_PART_P2P_PARTITION_WAITING   ? "STARTED"   :
            ready == MCA_PART_P2P_PARTITION_READY     ? "READY"     :
            ready == MCA_PART_P2P_PARTITION_COMPLETED ? (req_type == MCA_PART_P2P_REQUEST_SEND ? "COMPLETED" : "ARRIVED") : "???";

        written = snprintf(msg_pos, remaining_len, "\n - %3ld (%9s), ready=%d, completed=%d, state=%8s (partial: %d)",
            p, part_state_str, ready, completed, state_str, partial_completion);
        msg_pos += written;
        remaining_len = remaining_len >= written ? remaining_len - written : 0;
    }

    if (remaining_len == 0) {
        // add '...' at the end indicating that the output was truncated
        msg[msg_len - 4] = '.';
        msg[msg_len - 3] = '.';
        msg[msg_len - 2] = '.';
    } else {
        // Normal message end
        snprintf(msg_pos, remaining_len, "\n");
    }
    msg[msg_len - 1] = '\0';  // just to be sure
    opal_output(output_id, "%s", msg);
}


static int mca_part_p2p_init_module(void)
{
    if (0 != OPAL_THREAD_TRYLOCK(&ompi_part_p2p_module.lock)) {
        return OMPI_SUCCESS;
    }

    opal_output_verbose(50, ompi_part_base_framework.framework_output, "start of part p2p module initialization");

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

    err = ompi_request_test(&request->init_req, &completed, MPI_STATUS_IGNORE);
    if (OMPI_SUCCESS != err || 0 == completed) {
        return err;
    }

    /* Now 'request->meta' is synchronized between both processes */
    size_t parts = request->meta.partition_count;
    if (MCA_PART_P2P_REQUEST_RECV == request->type) {
        /* This array is already allocated and initialized on the send side */
        request->partition_states = calloc(parts, sizeof(int32_t));
    }
    request->partition_requests = malloc(parts * sizeof(ompi_request_t*));
    if (NULL == request->partition_states || NULL == request->partition_requests) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    // TODO:
    //   total_elements     is sync (otherwise it is wrong)
    //   elements_per_part  is missing 'aggregation factor' on the receive side

    size_t total_elements    = request->partition_size * request->user_partition_count;
    size_t elements_per_part = request->partition_size * request->aggregation_factor;

    size_t bytes_per_part;
    err = ompi_datatype_type_size(request->datatype, &bytes_per_part);
    if (OMPI_SUCCESS != err) { return err; }
    bytes_per_part *= elements_per_part;

    /* Initiate the persistent requests for each partition */
    for (size_t p = 0; p < parts; p++) {
        void* partition_data = ((char*) request->user_data) + bytes_per_part * p;
        size_t partition_size = MIN(elements_per_part, total_elements - elements_per_part * p);
        ompi_request_t* part_request;
        if (MCA_PART_P2P_REQUEST_SEND == request->type) {
            err = MCA_PML_CALL(isend_init(
                partition_data, partition_size, request->datatype,
                request->peer_rank, request->meta.first_part_tag + p, MCA_PML_BASE_SEND_STANDARD,
                ompi_part_p2p_module.part_comm, &part_request
            ));
            if (OMPI_SUCCESS != err) { return err; }
        } else {
            err = MCA_PML_CALL(irecv_init(
                partition_data, partition_size, request->datatype,
                request->peer_rank, request->meta.first_part_tag + p,
                ompi_part_p2p_module.part_comm, &part_request
            ));
            if (OMPI_SUCCESS != err) { return err; }
        }
        request->partition_requests[p] = part_request;
    }

    opal_output_verbose(50, ompi_part_base_framework.framework_output, "initialized part request %p with rank %d",
        request, request->peer_rank);

    mca_part_p2p_init_state_t init_state = opal_atomic_or_fetch_32(&request->init_state, MCA_PART_P2P_INIT_HANDSHAKE_FLAG);

    if (MCA_PART_P2P_REQUEST_RECV == request->type && MCA_PART_P2P_INIT_DONE == init_state) {
        /* We handle the edge case where MPI_Start is called before initialization here, but only for receive requests.
         * As send requests mark partitions at any time, this must be handled in the progress loop. */
        err = request->partition_requests[0]->req_start(parts, &request->partition_requests[0]);
        for (size_t p = 0; p < parts && OMPI_SUCCESS == err; p++) {
            request->partition_states[p] = MCA_PART_P2P_PARTITION_WAITING;
        }
    }

    return err;
}


static void mca_part_p2p_request_complete(mca_part_p2p_request_t* request)
{
    opal_output_verbose(50, ompi_part_base_framework.framework_output, "part request %p completed", request);
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
    OPAL_LIST_FOREACH_SAFE(current, next, &ompi_part_p2p_module.live_requests, mca_part_p2p_request_list_item_t) {
        mca_part_p2p_request_t* req = current->request;

        if ((req->init_state & MCA_PART_P2P_INIT_HANDSHAKE_FLAG) == 0) {
            err = mca_part_p2p_complete_request_init(req);
            if (OMPI_SUCCESS != err) { goto exit_progress; }
            if ((req->init_state & MCA_PART_P2P_INIT_HANDSHAKE_FLAG) != 0) {
                progress++;
            }
            continue;
        }

        if (true == req->to_delete) {
            mca_part_p2p_request_free(req);
            opal_list_remove_item(&ompi_part_p2p_module.live_requests, (opal_list_item_t*) current);
            progress++;
            continue;
        }

        if (REQUEST_COMPLETED != req->super.req_complete && OMPI_REQUEST_ACTIVE == req->super.req_state) {
            size_t completed_partitions = 0;
            for (size_t p = 0; p < req->meta.partition_count; p++) {
                mca_part_p2p_partition_state_t part_state = req->partition_states[p];

                if (MCA_PART_P2P_PARTITION_READY == part_state) {
                    /* MPI_Pready called before request initialization */
                    err = req->partition_requests[p]->req_start(1, &req->partition_requests[p]);
                    if (OMPI_SUCCESS != err) { goto exit_progress; }
                    part_state = MCA_PART_P2P_PARTITION_WAITING;
                    req->partition_states[p] = part_state;
                    progress++;
                }

                if (MCA_PART_P2P_PARTITION_WAITING == part_state) {
                    int done = false;
                    err = ompi_request_test(&req->partition_requests[p], &done, MPI_STATUS_IGNORE);
                    if (OMPI_SUCCESS != err) { goto exit_progress; }
                    if (done) {
                        part_state = MCA_PART_P2P_PARTITION_COMPLETED;
                        req->partition_states[p] = part_state;
                        progress++;
                    }
                }

                completed_partitions += MCA_PART_P2P_PARTITION_COMPLETED == part_state;
            }

            if (completed_partitions == req->meta.partition_count) {
                mca_part_p2p_request_complete(req);
                progress++;
            }
        }
    }

exit_progress:
    if (OMPI_SUCCESS != err) {
        ompi_rte_abort(err, "part p2p internal failure (%d)", err);
    }
    OPAL_THREAD_UNLOCK(&ompi_part_p2p_module.lock);
    return progress;
}


static int mca_part_p2p_compute_aggregation_factor(ompi_info_t* info, size_t part_size, ompi_datatype_t* datatype, int* aggregation_factor)
{
    int is_set = false;
    opal_cstring_t* param_str = NULL;
    int err = ompi_info_get(info, "ompi_part_aggregation_factor", &param_str, &is_set);
    if (OMPI_SUCCESS != err) { return err; }
    if (is_set) {
        err = opal_cstring_to_int(param_str, aggregation_factor);
        if (OPAL_SUCCESS != err) { return err; }
        OBJ_RELEASE(param_str);
    } else {
        int min_partition_size_kb = 0;
        err = ompi_info_get(info, "ompi_min_partition_size", &param_str, &is_set);
        if (OMPI_SUCCESS != err) { return err; }
        if (is_set) {
            err = opal_cstring_to_int(param_str, &min_partition_size_kb);
            if (OPAL_SUCCESS != err) { return err; }
            OBJ_RELEASE(param_str);
        } else {
            min_partition_size_kb = mca_part_p2p_component.default_min_partition_size;
        }

        size_t min_partition_size = min_partition_size_kb * 1000;  /* kilobytes to bytes */
        size_t part_bytes = 0;
        ompi_datatype_type_size(datatype, &part_bytes);
        part_bytes *= part_size;

        if (min_partition_size <= part_bytes || min_partition_size == 0 || part_bytes == 0) {
            *aggregation_factor = 1;
        } else {
            *aggregation_factor = (int) (min_partition_size / part_bytes);
        }
    }

    if (*aggregation_factor <= 0) {
        return OMPI_ERR_BAD_PARAM;
    }

    return OMPI_SUCCESS;
}


static int mca_part_p2p_lookup_peer_rank_in_world(ompi_communicator_t* comm, int peer_rank, int* world_rank)
{
    if (MPI_COMM_WORLD == comm) {
        *world_rank = peer_rank;
        return OMPI_SUCCESS;
    }

    // TODO: is this the most efficient way of obtaining the MPI_COMM_WORLD from a rank ?
    int err = ompi_group_translate_ranks(comm->c_local_group, 1, &peer_rank, ompi_mpi_comm_world.comm.c_local_group, world_rank);
    if (OMPI_SUCCESS != err) { return err; }
    if (MPI_UNDEFINED == *world_rank || MPI_PROC_NULL == *world_rank) { return MPI_ERR_RANK; }
    return OMPI_SUCCESS;
}


static int mca_part_p2p_psend_init(
    const void* buf, size_t parts, size_t count,
    ompi_datatype_t* datatype, int dst, int tag,
    ompi_communicator_t* comm, ompi_info_t* info,
    ompi_request_t** request)
{
    int err = OMPI_SUCCESS;

    /* initialize the module if needed */
    if (OPAL_UNLIKELY(0 == ompi_part_p2p_module.module_in_use)) {
        err = mca_part_p2p_init_module();
        if (OMPI_SUCCESS != err) {
            return err;
        }
    }

    mca_part_p2p_request_t* req = (mca_part_p2p_request_t*) opal_free_list_get(&ompi_part_p2p_module.requests);
    if (OPAL_UNLIKELY(NULL == req)) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }
    mca_part_p2p_request_init(req, MCA_PART_P2P_REQUEST_SEND, buf, parts, count, datatype, dst, tag, comm);

    /* Partitions are transmitted in a duplicate of MPI_COMM_WORLD, not in the 'comm'
     * given by the user, allowing to use different tags for each request of a partition.
     * Before moving to this duplicate comm, we must know which rank 'dst' corresponds to. */
    err = mca_part_p2p_lookup_peer_rank_in_world(comm, dst, &req->peer_rank);
    if (OMPI_SUCCESS != err) { return err; }

    err = mca_part_p2p_compute_aggregation_factor(info, count, datatype, &req->aggregation_factor);
    if (OMPI_SUCCESS != err) { return err; }

    // TODO: we need to manage tags to avoid encountering this error at runtime
    size_t real_parts     = parts / req->aggregation_factor + (parts % req->aggregation_factor > 0);
    size_t first_part_tag = opal_atomic_fetch_add_size_t(&ompi_part_p2p_module.next_tag, real_parts);
    size_t last_part_tag  = first_part_tag + real_parts;
    int max_tag = mca_pml.pml_max_tag;
    if (last_part_tag >= max_tag || first_part_tag > INT_MAX || last_part_tag > INT_MAX) {
        opal_output_verbose(ompi_part_base_framework.framework_output, 10,
                            "global partition tag (%ld) exceeded the maximum PML tag (%d) while allocating %ld partitions",
                            last_part_tag, max_tag, real_parts);
        return MPI_ERR_TAG;
    }

    // TODO: the MPI spec requires that a MPI_Psend_init can only be matched with a MPI_Precv_init,
    //  and this is a clear violation of this requirement.
    /* It is the send side which dictates the number of partitions requests and their tags. */
    req->meta.first_part_tag = (int) first_part_tag;
    req->meta.partition_count = real_parts;
    err = MCA_PML_CALL(isend(
        &req->meta, sizeof(mca_part_p2p_request_meta_t), MPI_BYTE,
        dst, tag, MCA_PML_BASE_SEND_STANDARD, comm,
        &req->init_req
    ));
    if (OMPI_SUCCESS != err) { return err; }

    /* This array needs to be available early, to allow 'MPI_Start' to use them before initialization is complete */
    req->partition_states = calloc(real_parts, sizeof(int32_t));
    if (NULL == req->partition_states) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    /* Add the request to the list of requests to progress */
    mca_part_p2p_request_list_item_t* item = OBJ_NEW(mca_part_p2p_request_list_item_t);
    item->request = req;
    OPAL_THREAD_LOCK(&ompi_part_p2p_module.lock);
    opal_list_append(&ompi_part_p2p_module.live_requests, (opal_list_item_t*) item);
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

    if (MPI_ANY_TAG == tag || MPI_ANY_SOURCE == src) {
        /* Disallowed by the MPI spec (implicitly) */
        return OMPI_ERR_BAD_PARAM;
    }

    /* initialize the module if needed */
    if (0 == ompi_part_p2p_module.module_in_use) {
        err = mca_part_p2p_init_module();
        if (OMPI_SUCCESS != err) {
            return err;
        }
    }

    mca_part_p2p_request_t* req = (mca_part_p2p_request_t*) opal_free_list_get(&ompi_part_p2p_module.requests);
    if (OPAL_UNLIKELY(NULL == req)) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }
    mca_part_p2p_request_init(req, MCA_PART_P2P_REQUEST_RECV, buf, parts, count, datatype, src, tag, comm);

    err = mca_part_p2p_lookup_peer_rank_in_world(comm, src, &req->peer_rank);
    if (OMPI_SUCCESS != err) { return err; }

    /* Receive the partitions configuration */
    err = MCA_PML_CALL(irecv(
        &req->meta, sizeof(mca_part_p2p_request_meta_t), MPI_BYTE,
        src, tag, comm,
        &req->init_req
    ));
    if (OMPI_SUCCESS != err) { return err; }

    /* Add the request to the list of requests to progress */
    mca_part_p2p_request_list_item_t* item = OBJ_NEW(mca_part_p2p_request_list_item_t);
    item->request = req;
    OPAL_THREAD_LOCK(&ompi_part_p2p_module.lock);
    opal_list_append(&ompi_part_p2p_module.live_requests, (opal_list_item_t*) item);
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

        mca_part_p2p_init_state_t init_state = req->init_state;
        if ((MCA_PART_P2P_INIT_START_FLAG & init_state) == 0) {
            init_state = opal_atomic_or_fetch_32(&req->init_state, MCA_PART_P2P_INIT_START_FLAG);
        }

        switch (req->type) {
        case MCA_PART_P2P_REQUEST_SEND: {
            for (size_t p = 0; p < req->meta.partition_count; p++) {
                req->partition_states[p] = MCA_PART_P2P_PARTITION_INACTIVE;
            }
            break;
        }
        case MCA_PART_P2P_REQUEST_RECV: {
            /* If we can't start the requests now, it will be done after initialization */
            if (MCA_PART_P2P_INIT_DONE == init_state) {
                int err = req->partition_requests[0]->req_start(req->meta.partition_count, &req->partition_requests[0]);
                if (OMPI_SUCCESS != err) { return err; }
                for (size_t p = 0; p < req->meta.partition_count; p++) {
                    req->partition_states[p] = MCA_PART_P2P_PARTITION_WAITING;
                }
            }
            break;
        }
        default:
            return OMPI_ERR_REQUEST;
        }

        req->super.req_state = OMPI_REQUEST_ACTIVE;
        req->super.req_complete = REQUEST_PENDING;
    }
    return OMPI_SUCCESS;
}


static int mca_part_p2p_last_aggregated_partition_size(mca_part_p2p_request_t* req)
{
    int extra_user_partitions = (int) (req->meta.partition_count * req->aggregation_factor - req->user_partition_count);
    return req->aggregation_factor - extra_user_partitions;
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
        size_t min_full = min_part;
        size_t max_full = max_part + 1;  /* exclusive */
        int32_t min_partial = 0;  /* Number of partially completed user partitions at 'min_full - 1' */
        int32_t max_partial = 0;  /* Number of partially completed user partitions at 'max_full' */
        if (1 < req->aggregation_factor) {
            /* Convert user partition indices to aggregated partitions indices */
            size_t  min_part_div = min_part / req->aggregation_factor;
            int32_t min_part_rem = (int32_t) (min_part % req->aggregation_factor);
            if (max_part == min_part) {
                /* Optimization for 'MPI_Pready(part_idx, req)' */
                min_partial = 1;
                max_full = min_part_div;
            } else {
                size_t  max_part_div = max_part / req->aggregation_factor;
                int32_t max_part_rem = (int32_t) (max_part % req->aggregation_factor);
                if (min_part_div == max_part_div) {
                    min_partial = max_part_rem - min_part_rem + 1;
                    max_full = min_part_div;
                } else {
                    min_partial = min_part_rem > 0 ? req->aggregation_factor - min_part_rem : 0;
                    max_partial = max_part_rem + 1;
                    max_full = max_part_div;
                }
            }
            min_full = min_part_div + (min_partial > 0);
        }

        /* 'full' partitions are those where all user partitions are marked as ready, and therefore
         * we can start their respective request.
         * Only fully completed partitions are marked as such, therefore 'min_full' and 'max_full' exclude
         * partially completed partitions. */
        mca_part_p2p_partition_state_t part_states;
        if (MCA_PART_P2P_INIT_DONE == req->init_state) {
            part_states = MCA_PART_P2P_PARTITION_WAITING;
            if (max_full > min_full) {
                ompi_request_t* first_part_req = req->partition_requests[min_full];
                err = first_part_req->req_start(max_full - min_full, &first_part_req);
                if (OMPI_SUCCESS != err) { return err; }
            }
        } else {
            /* Schedule the requests to be started in the progress loop after initialization is done */
            part_states = MCA_PART_P2P_PARTITION_READY;
        }

        for (size_t p = min_full; p < max_full; p++) {
            req->partition_states[p] = part_states;
        }

        /* Partial completions occur only when using aggregation.
         * Partition state needs to be updated atomically as other threads might contribute as well.
         * The number of user partitions ready is negative, to differentiate them from the values
         * of the partition state enum.
         * MPI forbids 'MPI_Pready' to be called multiple times on the same partition, therefore it
         * is safe to track only their count and not each user partition's state. */
        if (0 < min_partial) {
            int32_t total_ready = opal_atomic_add_fetch_32(&req->partition_states[min_full - 1], -min_partial);
            int partition_size = min_full - 1 == req->meta.partition_count - 1 ?
                mca_part_p2p_last_aggregated_partition_size(req) : req->aggregation_factor;
            if (-total_ready == partition_size) {
                if (MCA_PART_P2P_PARTITION_WAITING == part_states) {
                    ompi_request_t* part_req = req->partition_requests[min_full - 1];
                    err = part_req->req_start(1, &part_req);
                    if (OMPI_SUCCESS != err) { return err; }
                }
                req->partition_states[min_full - 1] = part_states;
            }
        }
        if (0 < max_partial) {
            int32_t total_ready = opal_atomic_add_fetch_32(&req->partition_states[max_full], -max_partial);
            int partition_size = max_full == req->meta.partition_count - 1 ?
                mca_part_p2p_last_aggregated_partition_size(req) : req->aggregation_factor;
            if (-total_ready == partition_size) {
                if (MCA_PART_P2P_PARTITION_WAITING == part_states) {
                    ompi_request_t* part_req = req->partition_requests[max_full];
                    err = part_req->req_start(1, &part_req);
                    if (OMPI_SUCCESS != err) { return err; }
                }
                req->partition_states[max_full] = part_states;
            }
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
    } else if (MCA_PART_P2P_INIT_DONE != req->init_state) {
        has_arrived = false;
    } else {
        /* Partition aggregation isn't done on the receiver's side, only real vs user partition counts matter */
        if (req->user_partition_count != req->meta.partition_count) {
            /* Match the user partitions with the internal ones */
            size_t first_elem = min_part * req->partition_size;
            size_t last_elem  = (max_part + 1) * req->partition_size - 1;
            size_t source_partition_size = req->user_partition_count * req->partition_size / req->meta.partition_count;
            min_part = first_elem / source_partition_size;
            max_part = last_elem / source_partition_size + (last_elem % source_partition_size > 0);
        }

        has_arrived = true;
        for (size_t p = min_part; p <= max_part; p++) {
            has_arrived &= MCA_PART_P2P_PARTITION_COMPLETED == req->partition_states[p];
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
