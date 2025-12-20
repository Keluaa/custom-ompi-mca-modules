#ifndef OMPI_CART_PLUGIN_PART_P2P_REQUEST_H
#define OMPI_CART_PLUGIN_PART_P2P_REQUEST_H

#include "ompi/datatype/ompi_datatype.h"
#include "ompi/request/request.h"

BEGIN_C_DECLS

typedef enum {
    MCA_PART_P2P_REQUEST_SEND,
    MCA_PART_P2P_REQUEST_RECV,
    MCA_PART_P2P_REQUEST_NULL,  // TODO: useful??
} mca_part_p2p_request_enum_t;  // TODO: rename


typedef enum {
    MCA_PART_P2P_PARTITION_INACTIVE = 0, /**< partition request is not yet started */
    MCA_PART_P2P_PARTITION_STARTED = 1,  /**< partition request started. The default after initialization, or after MPI_Start for receive requests */
    MCA_PART_P2P_PARTITION_COMPLETE = 2, /**< partition request complete */
    // TODO: this might be useless?
    MCA_PART_P2P_PARTITION_QUEUED = 3,   /**< next progress loop will start the partition request (send only) */
} mca_part_p2p_partition_state_t;


struct mca_part_p2p_request_meta_t {
    /** Rank of the other process in 'MPI_COMM_WORLD'. */
    int peer_rank;
    /** Tag for the request of the first partition.
     *  The others use 'first_part_tag + part_idx' */
    int first_part_tag;
    /** Number of partitions to send/recv */
    size_t partition_count;
};
typedef struct mca_part_p2p_request_meta_t mca_part_p2p_request_meta_t;


struct mca_part_p2p_request_t {
    ompi_request_t super;
    mca_part_p2p_request_enum_t type;
    opal_atomic_int32_t to_delete;

    /** Information shared by both processes after initialization */
    mca_part_p2p_request_meta_t meta;

    /* Each of those arrays have 'meta.partition_count' elements */
    ompi_request_t** partition_requests;               /**< Persistent request for each partition */
    mca_part_p2p_partition_state_t* partition_states;  /**< State of each partition */

    // TODO: do we have a false sharing issue? this is an array of atomic values, smaller than the cache size...
    opal_atomic_int32_t* partition_ready_flags;        /**< Readiness flag for partitions. NULL on the receiving side. */

    size_t user_partition_count;  /**< Exact number of partitions given to MPI_Psend_init or MPI_Precv_init */
    size_t partition_size;        /**< Number of elements per partition */
    ompi_datatype_t* datatype;    /**< Datatype of buffer elements */
    const void* user_data;        /**< Contiguous user buffer for the partitions */

    /* Data required for initialization */
    opal_atomic_int32_t is_initialized;
    opal_atomic_int32_t has_started;   /**< If 'MPI_Start' was called. Useful only for the receiving side, which doesn't know yet the number of requests. */
    int tmp_peer_rank;
    ompi_request_t* init_send;
    ompi_request_t* init_recv;
};
typedef struct mca_part_p2p_request_t mca_part_p2p_request_t;
OBJ_CLASS_DECLARATION(mca_part_p2p_request_t);


void mca_part_p2p_request_init(
    mca_part_p2p_request_t* request,
    mca_part_p2p_request_enum_t type,
    const void* buf, size_t parts, size_t count,
    ompi_datatype_t* datatype, int target, int tag,
    struct ompi_communicator_t* comm);

void mca_part_p2p_request_free(mca_part_p2p_request_t* request);

END_C_DECLS

#endif //OMPI_CART_PLUGIN_PART_P2P_REQUEST_H
