#ifndef PART_P2P_H
#define PART_P2P_H

#include "ompi_config.h"
#include "part_p2p_request.h"
#include "ompi/request/request.h"
#include "ompi/mca/part/part.h"
#include "ompi/datatype/ompi_datatype.h"

BEGIN_C_DECLS

struct mca_part_p2p_request_list_item_t {
    opal_list_item_t super;
    mca_part_p2p_request_t* request;
};

typedef struct mca_part_p2p_request_list_item_t mca_part_p2p_request_list_item_t;
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(mca_part_p2p_request_list_item_t);

// TODO: GPU awareness???

struct ompi_part_p2p_module_t {
    mca_part_base_module_t super;
    opal_atomic_int32_t module_in_use;
    opal_mutex_t lock;

    opal_list_t live_requests;  /**< List of active request objects */
    opal_free_list_t requests;  /**< Manages request objects */

    /** Duplicate of MPI_COMM_WORLD where all partition transit */
    struct ompi_communicator_t* part_comm;
    /** Next tag for communications within 'part_comm' */
    opal_atomic_size_t next_tag;

    ompi_request_t* part_comm_init;
};
typedef struct ompi_part_p2p_module_t ompi_part_p2p_module_t;
extern ompi_part_p2p_module_t ompi_part_p2p_module;


int mca_part_p2p_free(ompi_request_t** request);
void mca_part_p2p_dump_request_state(ompi_request_t* request, const char* label);

END_C_DECLS

#endif //PART_P2P_H