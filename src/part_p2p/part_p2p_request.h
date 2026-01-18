#ifndef OMPI_CART_PLUGIN_PART_P2P_REQUEST_H
#define OMPI_CART_PLUGIN_PART_P2P_REQUEST_H

#include "ompi/datatype/ompi_datatype.h"
#include "ompi/request/request.h"

BEGIN_C_DECLS

typedef enum {
    MCA_PART_P2P_REQUEST_SEND,
    MCA_PART_P2P_REQUEST_RECV,
} mca_part_p2p_request_type_t;


typedef enum {
    MCA_PART_P2P_PARTITION_INACTIVE  = 0,  /**< MPI_Start (or MPI_Pready for send requests) was not called */
    MCA_PART_P2P_PARTITION_READY     = 1,  /**< send partition marked as ready, but not yet started */
    MCA_PART_P2P_PARTITION_WAITING   = 2,  /**< recv/send partition started, but not yet completed */
    MCA_PART_P2P_PARTITION_COMPLETED = 3,  /**< send partition completed, or recv partition has arrived */
} mca_part_p2p_partition_state_t;


typedef enum {
    MCA_PART_P2P_INIT_NONE           = 0b00,
    MCA_PART_P2P_INIT_HANDSHAKE_FLAG = 0b01,  /**< Partitions and arrays are initialized on both processes */
    MCA_PART_P2P_INIT_START_FLAG     = 0b10,  /**< MPI_Start was called for the first time */
    MCA_PART_P2P_INIT_DONE           = MCA_PART_P2P_INIT_HANDSHAKE_FLAG | MCA_PART_P2P_INIT_START_FLAG,
} mca_part_p2p_init_state_t;


struct mca_part_p2p_request_meta_t {
    /** Tag for the request of the first partition.
     *  The others use 'first_part_tag + part_idx' */
    int first_part_tag;
    /** Number of partitions to send/recv */
    size_t partition_count;
    /** Real size of each partition, accounting for aggregation.
     *  With aggregation, the last partition can have fewer elements. */
    size_t elements_per_partition;
};
typedef struct mca_part_p2p_request_meta_t mca_part_p2p_request_meta_t;


struct mca_part_p2p_request_t {
    ompi_request_t super;
    mca_part_p2p_request_type_t type;
    opal_atomic_int32_t to_delete;

    /** Rank of peer in MPI_COMM_WORLD.
     *  Partitions live in a communicator duplicating MPI_COMM_WORLD, instead of the communicator used to initiate
     *  the partitioned request, since we need to generate unique tags for each partition. */
    int peer_rank;

    /** Information shared by both processes after initialization */
    mca_part_p2p_request_meta_t meta;

    /* Each of those arrays have 'meta.partition_count' elements */
    ompi_request_t** partition_requests;  /**< Persistent request for each partition */
    /** State of each partition: a 'mca_part_p2p_partition_state_t' value, or a negative value indicating
     *  partial completion, in case multiple user partitions are aggregated. */
    volatile int32_t* partition_states;

    size_t user_partition_count;  /**< Exact number of partitions given to MPI_Psend_init or MPI_Precv_init */
    size_t partition_size;        /**< Number of elements per partition, not accounting for aggregation */
    ompi_datatype_t* datatype;    /**< Datatype of buffer elements */
    const void* user_data;        /**< Contiguous user buffer for the partitions */
    int aggregation_factor;       /**< Number of user partitions per real partitions (sender side only) */

    /* Data required for initialization */
    opal_atomic_int32_t init_state;  /**< A 'mca_part_p2p_init_state_t' value */
    ompi_request_t*     init_req;    /**< used to send 'meta' from MPI_Psend_init to MPI_Precv_init */
};
typedef struct mca_part_p2p_request_t mca_part_p2p_request_t;
OBJ_CLASS_DECLARATION(mca_part_p2p_request_t);


void mca_part_p2p_request_init(
    mca_part_p2p_request_t* request,
    mca_part_p2p_request_type_t type,
    const void* buf, size_t parts, size_t count,
    ompi_datatype_t* datatype, int target, int tag,
    struct ompi_communicator_t* comm);

void mca_part_p2p_request_free(mca_part_p2p_request_t* request);

END_C_DECLS

#endif //OMPI_CART_PLUGIN_PART_P2P_REQUEST_H
