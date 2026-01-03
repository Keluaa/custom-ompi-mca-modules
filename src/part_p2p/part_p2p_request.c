
#include "ompi/mca/part/part.h"
#include "ompi/mca/part/base/part_base_psendreq.h"
#include "ompi/mca/part/base/part_base_precvreq.h"

#include "part_p2p_request.h"
#include "part_p2p.h"


void mca_part_p2p_request_init(
    mca_part_p2p_request_t* request,
    mca_part_p2p_request_type_t type,
    const void* buf, size_t parts, size_t count,
    ompi_datatype_t* datatype, int target, int tag,
    ompi_communicator_t* comm)
{
    OMPI_REQUEST_INIT(&request->super, true);
    OBJ_RETAIN(comm);
    OMPI_DATATYPE_RETAIN(datatype);

    ompi_request_t* req_ompi = &request->super;

    req_ompi->req_mpi_object.comm = comm;
    req_ompi->req_start = ompi_part_p2p_module.super.part_start;
    req_ompi->req_free = mca_part_p2p_free;
    req_ompi->req_cancel = NULL;
    req_ompi->req_persistent = true;

    req_ompi->req_status.MPI_SOURCE = MCA_PART_P2P_REQUEST_SEND == type ? comm->c_my_rank : target;
    req_ompi->req_status.MPI_TAG = tag;
    req_ompi->req_status._ucount = count * parts;

    request->type = type;
    request->to_delete = false;
    request->peer_rank = MPI_PROC_NULL;
    request->init_state = MCA_PART_P2P_INIT_NONE;
    request->user_partition_count = parts;
    request->partition_size = count;
    request->datatype = datatype;
    request->user_data = buf;

    request->init_req = MPI_REQUEST_NULL;

    request->partition_requests = NULL;
    request->partition_states = NULL;
}


void mca_part_p2p_request_free(mca_part_p2p_request_t* request)
{
    OMPI_DATATYPE_RELEASE(request->datatype);
    OBJ_RELEASE(request->super.req_mpi_object.comm);
    OMPI_REQUEST_FINI(&request->super);

    if (NULL != request->partition_requests) {
        for (size_t p = 0; p < request->meta.partition_count; p++) {
            ompi_request_free(&request->partition_requests[p]);
        }
        free(request->partition_requests);
    }

    if (NULL != request->partition_states) {
        free((void*) request->partition_states);
    }

    if (request->init_req != MPI_REQUEST_NULL) {
        ompi_request_free(&request->init_req);
    }

    opal_free_list_return(&ompi_part_p2p_module.requests, (opal_free_list_item_t*) request);
}


static void mca_part_p2p_request_constructor(mca_part_p2p_request_t* request)
{
    request->super.req_type = OMPI_REQUEST_PART;
}


OBJ_CLASS_INSTANCE(mca_part_p2p_request_t, ompi_request_t, mca_part_p2p_request_constructor, NULL);
